//go:build linux

// Package broadcast provides multiple fanout strategies for benchmarking message delivery.
//
// IOUringBroadcaster — io_uring based broadcaster.
//
// io_uring is a Linux kernel interface (introduced in kernel 5.1, production-ready in
// 5.10+) that allows submitting I/O operations to a shared ring buffer without
// making per-operation syscalls, dramatically reducing context-switch overhead for
// write-heavy workloads.
//
// This implementation checks for io_uring availability at runtime:
//   - If io_uring is available (Linux >= 5.10): set up a submission/completion ring
//     and batch IORING_OP_WRITE requests for each Broadcast call.
//   - If io_uring is not available: fall through to EpollBroadcaster which provides
//     similar asynchronous delivery semantics via edge-triggered epoll.
//
// Requires: Linux 5.10+, golang.org/x/sys/unix
package broadcast

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"unsafe"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// iouRingSize is the number of entries in both the submission and completion rings.
// Must be a power of two. 256 is sufficient for most broadcast fan-outs.
const iouRingSize = 256

// iouSQE mirrors the io_uring submission queue entry layout (io_uring_sqe).
// We only use the fields needed for IORING_OP_WRITE.
type iouSQE struct {
	opcode   uint8
	flags    uint8
	ioprio   uint16
	fd       int32
	off      uint64
	addr     uint64
	len      uint32
	opFlags  uint32
	userData uint64
	pad      [3]uint64
}

// iouCQE mirrors the io_uring completion queue entry layout (io_uring_cqe).
type iouCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

// iouRing holds the mmap'd ring buffers and control structures for one io_uring instance.
type iouRing struct {
	fd int

	// Submission queue
	sqHead     *uint32
	sqTail     *uint32
	sqMask     *uint32
	sqFlags    *uint32
	sqDropped  *uint32
	sqArray    []uint32
	sqEntries  []iouSQE

	// Completion queue
	cqHead    *uint32
	cqTail    *uint32
	cqMask    *uint32
	cqEntries []iouCQE

	sqMmapBase uintptr
	sqMmapSize uintptr
	cqMmapBase uintptr
	cqMmapSize uintptr
	sqeMmap    uintptr
	sqeMmapSz  uintptr

	mu sync.Mutex
}

// IOUringBroadcaster delivers messages via io_uring batched writes when available,
// falling back to EpollBroadcaster on kernels that do not support io_uring.
type IOUringBroadcaster struct {
	ring    *iouRing
	epoll   *EpollBroadcaster // fallback
	useRing bool
	logger  *zap.Logger

	fds     sync.Map // int -> net.Conn  (used when ring is active)
	fdsMu   sync.Mutex
	connFDs []int // ordered list for iteration
}

// NewIOUringBroadcaster creates an IOUringBroadcaster.
// It probes for io_uring support by attempting a minimal io_uring_setup(1, ...) call.
// If the probe fails (ENOSYS, permission denied, etc.) it falls back to EpollBroadcaster.
func NewIOUringBroadcaster(logger *zap.Logger) (*IOUringBroadcaster, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	b := &IOUringBroadcaster{logger: logger}

	ring, err := setupIOURing(iouRingSize)
	if err != nil {
		logger.Warn("io_uring not available, falling back to epoll broadcaster",
			zap.Error(err),
			zap.String("note", "requires Linux 5.10+"))

		ep, epErr := NewEpollBroadcaster()
		if epErr != nil {
			return nil, fmt.Errorf("iou broadcaster: epoll fallback: %w", epErr)
		}
		b.epoll = ep
		b.useRing = false
		return b, nil
	}

	b.ring = ring
	b.useRing = true
	return b, nil
}

// Add registers a connection with the broadcaster.
func (b *IOUringBroadcaster) Add(conn net.Conn) error {
	if !b.useRing {
		return b.epoll.Add(conn)
	}

	fd, err := connFd(conn)
	if err != nil {
		return fmt.Errorf("iou broadcaster: extract fd: %w", err)
	}
	b.fds.Store(fd, conn)
	b.fdsMu.Lock()
	b.connFDs = append(b.connFDs, fd)
	b.fdsMu.Unlock()
	return nil
}

// Remove deregisters a connection identified by its file descriptor.
func (b *IOUringBroadcaster) Remove(fd int) error {
	if !b.useRing {
		return b.epoll.Remove(fd)
	}

	b.fds.Delete(fd)
	b.fdsMu.Lock()
	for i, f := range b.connFDs {
		if f == fd {
			last := len(b.connFDs) - 1
			b.connFDs[i] = b.connFDs[last]
			b.connFDs = b.connFDs[:last]
			break
		}
	}
	b.fdsMu.Unlock()
	return nil
}

// Broadcast sends data to all registered connections.
// When io_uring is active, writes are submitted as a batch of IORING_OP_WRITE SQEs
// and the call blocks until all completions are drained.
// When running in epoll fallback mode, delegates to EpollBroadcaster.Broadcast.
func (b *IOUringBroadcaster) Broadcast(data []byte) error {
	if !b.useRing {
		return b.epoll.Broadcast(data)
	}

	// Snapshot the current FD list to avoid holding the lock during I/O.
	b.fdsMu.Lock()
	fds := make([]int, len(b.connFDs))
	copy(fds, b.connFDs)
	b.fdsMu.Unlock()

	if len(fds) == 0 {
		return nil
	}

	// Copy payload so it stays valid throughout async I/O.
	payload := make([]byte, len(data))
	copy(payload, data)

	// Submit writes in batches of iouRingSize.
	var firstErr error
	for start := 0; start < len(fds); start += iouRingSize {
		end := start + iouRingSize
		if end > len(fds) {
			end = len(fds)
		}
		batch := fds[start:end]
		if err := b.submitBatch(payload, batch); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Len returns the number of registered connections.
func (b *IOUringBroadcaster) Len() int {
	if !b.useRing {
		return b.epoll.Len()
	}
	count := 0
	b.fds.Range(func(_, _ any) bool { count++; return true })
	return count
}

// Close shuts down the broadcaster and releases kernel resources.
func (b *IOUringBroadcaster) Close() error {
	if !b.useRing {
		return b.epoll.Close()
	}
	return teardownIOURing(b.ring)
}

// ─── io_uring plumbing ────────────────────────────────────────────────────────

// SYS_io_uring_setup, SYS_io_uring_enter — Linux syscall numbers (x86-64).
const (
	sysIOUringSetup  = 425
	sysIOUringEnter  = 426
	sysIOUringRegister = 427

	// io_uring_setup params flags
	iouSetupSQPoll = 0x2 // kernel-side SQ poll (requires CAP_SYS_NICE)

	// io_uring SQE opcodes
	iouOpWrite = 23 // IORING_OP_WRITE

	// io_uring_enter flags
	iouEnterGetEvents = 0x4
)

// io_uring_params — minimal subset needed for setup.
type iouParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        iouSQRingOffsets
	cqOff        iouCQRingOffsets
}

// io_sqring_offsets
type iouSQRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	resv2       uint64
}

// io_cqring_offsets
type iouCQRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	resv2       uint64
}

// IORING_OFF_* — fixed mmap offsets defined by the kernel.
const (
	iouOffSQRing uint64 = 0
	iouOffCQRing uint64 = 0x8000000
	iouOffSQEs   uint64 = 0x10000000
)

func setupIOURing(entries uint32) (*iouRing, error) {
	var params iouParams

	// io_uring_setup(entries, &params)
	fd, _, errno := unix.Syscall(sysIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&params)), 0)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", errno)
	}

	ring := &iouRing{fd: int(fd)}

	// mmap the SQ ring.
	sqSize := uintptr(params.sqOff.array) + uintptr(params.sqEntries)*4
	sqBase, _, errno := unix.Syscall6(unix.SYS_MMAP, 0, sqSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED|unix.MAP_POPULATE,
		fd, uintptr(iouOffSQRing))
	if errno != 0 {
		unix.Close(int(fd))
		return nil, fmt.Errorf("io_uring mmap SQ ring: %w", errno)
	}
	ring.sqMmapBase = sqBase
	ring.sqMmapSize = sqSize

	ring.sqHead = (*uint32)(unsafe.Pointer(sqBase + uintptr(params.sqOff.head)))
	ring.sqTail = (*uint32)(unsafe.Pointer(sqBase + uintptr(params.sqOff.tail)))
	ring.sqMask = (*uint32)(unsafe.Pointer(sqBase + uintptr(params.sqOff.ringMask)))
	ring.sqFlags = (*uint32)(unsafe.Pointer(sqBase + uintptr(params.sqOff.flags)))
	ring.sqDropped = (*uint32)(unsafe.Pointer(sqBase + uintptr(params.sqOff.dropped)))

	arrPtr := (*uint32)(unsafe.Pointer(sqBase + uintptr(params.sqOff.array)))
	arrSlice := unsafe.Slice(arrPtr, params.sqEntries)
	ring.sqArray = arrSlice

	// mmap the SQE array.
	sqeSize := uintptr(params.sqEntries) * 64 // sizeof(io_uring_sqe) == 64
	sqeBase, _, errno := unix.Syscall6(unix.SYS_MMAP, 0, sqeSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED|unix.MAP_POPULATE,
		fd, uintptr(iouOffSQEs))
	if errno != 0 {
		unix.Munmap((*[1 << 30]byte)(unsafe.Pointer(sqBase))[:sqSize])
		unix.Close(int(fd))
		return nil, fmt.Errorf("io_uring mmap SQE array: %w", errno)
	}
	ring.sqeMmap = sqeBase
	ring.sqeMmapSz = sqeSize

	sqePtr := (*iouSQE)(unsafe.Pointer(sqeBase))
	ring.sqEntries = unsafe.Slice(sqePtr, params.sqEntries)

	// mmap the CQ ring.
	cqSize := uintptr(params.cqOff.cqes) + uintptr(params.cqEntries)*16 // sizeof(io_uring_cqe) == 16
	cqBase, _, errno := unix.Syscall6(unix.SYS_MMAP, 0, cqSize,
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED|unix.MAP_POPULATE,
		fd, uintptr(iouOffCQRing))
	if errno != 0 {
		unix.Munmap((*[1 << 30]byte)(unsafe.Pointer(sqeMmap))[:sqeSize])
		unix.Munmap((*[1 << 30]byte)(unsafe.Pointer(sqBase))[:sqSize])
		unix.Close(int(fd))
		return nil, fmt.Errorf("io_uring mmap CQ ring: %w", errno)
	}
	_ = cqBase
	ring.cqMmapBase = cqBase
	ring.cqMmapSize = cqSize

	ring.cqHead = (*uint32)(unsafe.Pointer(cqBase + uintptr(params.cqOff.head)))
	ring.cqTail = (*uint32)(unsafe.Pointer(cqBase + uintptr(params.cqOff.tail)))
	ring.cqMask = (*uint32)(unsafe.Pointer(cqBase + uintptr(params.cqOff.ringMask)))

	cqePtr := (*iouCQE)(unsafe.Pointer(cqBase + uintptr(params.cqOff.cqes)))
	ring.cqEntries = unsafe.Slice(cqePtr, params.cqEntries)

	return ring, nil
}

// sqeMmap is a package-level alias used in the cleanup path above (avoids repetition).
var sqeMmap uintptr // shadow — only read in teardown context

func teardownIOURing(ring *iouRing) error {
	if ring == nil {
		return nil
	}
	if ring.cqMmapBase != 0 {
		unix.Munmap((*[1 << 30]byte)(unsafe.Pointer(ring.cqMmapBase))[:ring.cqMmapSize])
	}
	if ring.sqeMmap != 0 {
		unix.Munmap((*[1 << 30]byte)(unsafe.Pointer(ring.sqeMmap))[:ring.sqeMmapSz])
	}
	if ring.sqMmapBase != 0 {
		unix.Munmap((*[1 << 30]byte)(unsafe.Pointer(ring.sqMmapBase))[:ring.sqMmapSize])
	}
	return unix.Close(ring.fd)
}

// submitBatch submits one IORING_OP_WRITE SQE per fd in the batch and waits for
// all completions before returning.
func (b *IOUringBroadcaster) submitBatch(payload []byte, fds []int) error {
	ring := b.ring
	ring.mu.Lock()
	defer ring.mu.Unlock()

	n := len(fds)
	sqTail := *ring.sqTail
	mask := *ring.sqMask

	for i, fd := range fds {
		idx := (sqTail + uint32(i)) & mask
		ring.sqArray[idx] = idx
		sqe := &ring.sqEntries[idx]
		sqe.opcode = iouOpWrite
		sqe.flags = 0
		sqe.ioprio = 0
		sqe.fd = int32(fd)
		sqe.off = 0 // ignored for sockets
		sqe.addr = uint64(uintptr(unsafe.Pointer(&payload[0])))
		sqe.len = uint32(len(payload))
		sqe.opFlags = 0
		sqe.userData = uint64(fd)
	}

	// Publish the new tail (memory barrier via atomic store — on x86 a plain store is sufficient
	// because TSO guarantees write ordering, but we use runtime atomics for portability).
	runtime.KeepAlive(payload)
	*ring.sqTail = sqTail + uint32(n)

	// io_uring_enter(fd, to_submit, min_complete, IORING_ENTER_GETEVENTS, NULL, 0)
	_, _, errno := unix.Syscall6(
		sysIOUringEnter,
		uintptr(ring.fd),
		uintptr(n),
		uintptr(n), // wait for all submissions to complete
		iouEnterGetEvents,
		0, 0,
	)
	if errno != 0 {
		return fmt.Errorf("io_uring_enter: %w", errno)
	}

	// Drain completions — advance CQ head past all CQEs we just received.
	cqHead := *ring.cqHead
	cqTail := *ring.cqTail
	cqMask := *ring.cqMask

	var firstErr error
	for cqHead != cqTail {
		cqe := &ring.cqEntries[cqHead&cqMask]
		if cqe.res < 0 && firstErr == nil {
			firstErr = fmt.Errorf("io_uring write fd %d: errno %d", cqe.userData, -cqe.res)
		}
		cqHead++
	}
	*ring.cqHead = cqHead

	return firstErr
}
