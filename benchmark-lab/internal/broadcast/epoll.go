//go:build linux

// Package broadcast provides multiple fanout strategies for benchmarking message delivery.
package broadcast

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// epollConn tracks a single connection registered with the epoll instance.
// pending holds outbound payloads that have not yet been flushed to the socket.
type epollConn struct {
	mu      sync.Mutex
	conn    net.Conn
	fd      int
	pending [][]byte
	closed  atomic.Bool
}

// EpollBroadcaster fans out messages using Linux edge-triggered epoll (EPOLLOUT | EPOLLET).
// It registers each connection's file descriptor with an epoll instance and runs
// NumCPU worker goroutines that drain pending write queues on EPOLLOUT events.
//
// Broadcast appends the payload to every registered connection's pending queue and
// then modifies the epoll registration to include EPOLLOUT so that a worker will
// wake up and flush the queue as soon as the socket is writable.
//
// Requires Linux kernel 2.5.44+. For io_uring support see EpollBroadcaster's
// companion IOUringBroadcaster in iou.go (requires Linux 5.10+).
type EpollBroadcaster struct {
	epfd    int
	fds     sync.Map // int -> *epollConn
	workers int
	stop    chan struct{}
	wg      sync.WaitGroup
}

// NewEpollBroadcaster creates an EpollBroadcaster and starts NumCPU worker goroutines.
// Call Close() to shut down cleanly.
func NewEpollBroadcaster() (*EpollBroadcaster, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("epoll broadcaster: EpollCreate1: %w", err)
	}

	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}

	b := &EpollBroadcaster{
		epfd:    epfd,
		workers: workers,
		stop:    make(chan struct{}),
	}

	for i := 0; i < workers; i++ {
		b.wg.Add(1)
		go b.runWorker()
	}

	return b, nil
}

// Add registers a net.Conn with the epoll broadcaster.
// The connection's file descriptor is extracted via SyscallConn and registered for
// EPOLLIN | EPOLLOUT | EPOLLET (edge-triggered).
func (b *EpollBroadcaster) Add(conn net.Conn) error {
	fd, err := connFd(conn)
	if err != nil {
		return fmt.Errorf("epoll broadcaster: extract fd: %w", err)
	}

	ec := &epollConn{conn: conn, fd: fd}

	// Register for EPOLLIN | EPOLLOUT | EPOLLET
	event := unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLET,
		Fd:     int32(fd),
	}
	if err := unix.EpollCtl(b.epfd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		return fmt.Errorf("epoll broadcaster: EpollCtl ADD fd %d: %w", fd, err)
	}

	b.fds.Store(fd, ec)
	return nil
}

// Remove deregisters the connection with the given file descriptor.
func (b *EpollBroadcaster) Remove(fd int) error {
	val, ok := b.fds.LoadAndDelete(fd)
	if !ok {
		return nil
	}
	ec := val.(*epollConn)
	ec.closed.Store(true)

	// EPOLL_CTL_DEL ignores the event argument but it must not be nil on older kernels.
	dummy := &unix.EpollEvent{}
	if err := unix.EpollCtl(b.epfd, unix.EPOLL_CTL_DEL, fd, dummy); err != nil {
		return fmt.Errorf("epoll broadcaster: EpollCtl DEL fd %d: %w", fd, err)
	}
	return nil
}

// Broadcast appends data to the pending queue of every registered connection and
// re-arms the EPOLLOUT edge-trigger so a worker will flush the queue promptly.
func (b *EpollBroadcaster) Broadcast(data []byte) error {
	// Copy once; all connections share the same immutable slice.
	payload := make([]byte, len(data))
	copy(payload, data)

	var firstErr error
	b.fds.Range(func(key, val any) bool {
		ec := val.(*epollConn)
		if ec.closed.Load() {
			return true
		}

		ec.mu.Lock()
		ec.pending = append(ec.pending, payload)
		ec.mu.Unlock()

		// Re-arm EPOLLOUT | EPOLLIN | EPOLLET to trigger a worker wake-up.
		event := unix.EpollEvent{
			Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLET,
			Fd:     int32(ec.fd),
		}
		if err := unix.EpollCtl(b.epfd, unix.EPOLL_CTL_MOD, ec.fd, &event); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("epoll broadcaster: EpollCtl MOD fd %d: %w", ec.fd, err)
		}
		return true
	})
	return firstErr
}

// Len returns the number of registered connections.
func (b *EpollBroadcaster) Len() int {
	count := 0
	b.fds.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// Close stops all worker goroutines and closes the epoll file descriptor.
func (b *EpollBroadcaster) Close() error {
	close(b.stop)
	b.wg.Wait()
	return unix.Close(b.epfd)
}

// runWorker is the event loop for one epoll worker goroutine.
// It calls epoll_wait with a 1 ms timeout so it can also respond to the stop signal.
func (b *EpollBroadcaster) runWorker() {
	defer b.wg.Done()

	events := make([]unix.EpollEvent, 64)
	for {
		select {
		case <-b.stop:
			return
		default:
		}

		n, err := unix.EpollWait(b.epfd, events, 1 /* ms */)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			// epfd closed by Close(); exit cleanly.
			return
		}

		for i := 0; i < n; i++ {
			ev := events[i]
			if ev.Events&unix.EPOLLOUT != 0 {
				fd := int(ev.Fd)
				val, ok := b.fds.Load(fd)
				if !ok {
					continue
				}
				ec := val.(*epollConn)
				b.flushConn(ec)
			}
		}
	}
}

// flushConn drains the pending write queue for ec, writing each payload to the
// underlying connection. If a write fails the connection is closed and removed.
func (b *EpollBroadcaster) flushConn(ec *epollConn) {
	if ec.closed.Load() {
		return
	}

	ec.mu.Lock()
	if len(ec.pending) == 0 {
		ec.mu.Unlock()
		return
	}
	// Take the current pending slice; replace with a fresh one.
	queue := ec.pending
	ec.pending = nil
	ec.mu.Unlock()

	for _, payload := range queue {
		if _, err := ec.conn.Write(payload); err != nil {
			ec.closed.Store(true)
			_ = b.Remove(ec.fd)
			return
		}
	}
}

// rawConn is the interface exposed by concrete net.Conn implementations
// (net.TCPConn, net.UnixConn, etc.) to access the underlying file descriptor.
type rawConn interface {
	SyscallConn() (syscallRawConn, error)
}

// syscallRawConn mirrors syscall.RawConn from the standard library.
type syscallRawConn interface {
	Control(func(uintptr)) error
}

// connFd extracts the raw file descriptor from a net.Conn using SyscallConn.
func connFd(conn net.Conn) (int, error) {
	rc, ok := conn.(rawConn)
	if !ok {
		return 0, fmt.Errorf("epoll broadcaster: conn type %T does not implement SyscallConn", conn)
	}
	sc, err := rc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("epoll broadcaster: SyscallConn: %w", err)
	}
	var fd uintptr
	var ctrlErr error
	if err := sc.Control(func(f uintptr) { fd = f }); err != nil {
		ctrlErr = fmt.Errorf("epoll broadcaster: Control: %w", err)
	}
	if ctrlErr != nil {
		return 0, ctrlErr
	}
	return int(fd), nil
}
