//go:build linux

package udp

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmsghdr mirrors the kernel struct mmsghdr (Linux-specific).
// The kernel layout is: struct msghdr msg_hdr; unsigned int msg_len.
// On amd64, Msghdr is 56 bytes + 4-byte Msglen + 4-byte padding = 64 bytes total.
type mmsghdr struct {
	Msghdr unix.Msghdr
	Msglen uint32
	_      [4]byte // padding to match kernel layout
}

// setSocketBuf sets SO_RCVBUF and SO_SNDBUF on the UDP connection's file descriptor.
func setSocketBuf(conn *net.UDPConn, size int) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("SyscallConn: %w", err)
	}
	var setErr error
	err = raw.Control(func(fd uintptr) {
		if e := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, size); e != nil {
			setErr = fmt.Errorf("SO_RCVBUF: %w", e)
			return
		}
		if e := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, size); e != nil {
			setErr = fmt.Errorf("SO_SNDBUF: %w", e)
		}
	})
	if err != nil {
		return err
	}
	return setErr
}

// batchRecv reads up to len(bufs) datagrams via recvmmsg in a single syscall.
// bufs and addrs must have the same length. Returns the number of messages read.
func batchRecv(conn *net.UDPConn, bufs [][]byte, addrs []*net.UDPAddr) (int, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("SyscallConn: %w", err)
	}

	n := len(bufs)
	msgs := make([]mmsghdr, n)
	names := make([]unix.RawSockaddrAny, n)
	iovecs := make([]unix.Iovec, n)

	for i := 0; i < n; i++ {
		iovecs[i].Base = &bufs[i][0]
		iovecs[i].SetLen(len(bufs[i]))
		msgs[i].Msghdr.Name = (*byte)(unsafe.Pointer(&names[i]))
		msgs[i].Msghdr.Namelen = uint32(unix.SizeofSockaddrAny)
		msgs[i].Msghdr.Iov = &iovecs[i]
		msgs[i].Msghdr.Iovlen = 1
		msgs[i].Msghdr.SetControllen(0)
	}

	var count int
	var recvErr error
	err = raw.Read(func(fd uintptr) bool {
		r, _, e := syscall.Syscall6(
			unix.SYS_RECVMMSG,
			fd,
			uintptr(unsafe.Pointer(&msgs[0])),
			uintptr(n),
			0, // flags
			0, // timeout (nil)
			0,
		)
		if e != 0 {
			if e == unix.EAGAIN || e == unix.EWOULDBLOCK {
				// Not ready — tell runtime rawconn to wait.
				return false
			}
			recvErr = e
			return true
		}
		count = int(r)
		// Truncate each buffer to the actual received length.
		for i := 0; i < count; i++ {
			bufs[i] = bufs[i][:msgs[i].Msglen]
		}
		// Parse source addresses.
		for i := 0; i < count; i++ {
			addrs[i] = toUDPAddr(&names[i])
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	if recvErr != nil {
		return 0, recvErr
	}

	// Restore original buffer lengths for reuse on the next call.
	for i := 0; i < n; i++ {
		bufs[i] = bufs[i][:cap(bufs[i])]
	}

	return count, nil
}

// batchSend sends the same payload to every address using sendmmsg.
func batchSend(conn *net.UDPConn, addrs []*net.UDPAddr, data []byte) error {
	if len(addrs) == 0 {
		return nil
	}

	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("SyscallConn: %w", err)
	}

	msgs := make([]mmsghdr, len(addrs))
	names := make([]unix.RawSockaddrAny, len(addrs))
	iovecs := make([]unix.Iovec, len(addrs))

	iov := unix.Iovec{Base: &data[0]}
	iov.SetLen(len(data))

	for i, addr := range addrs {
		iovecs[i] = iov
		fillSockaddr(&names[i], addr)
		if addr.IP.To4() != nil {
			msgs[i].Msghdr.Namelen = unix.SizeofSockaddrInet4
		} else {
			msgs[i].Msghdr.Namelen = unix.SizeofSockaddrInet6
		}
		msgs[i].Msghdr.Name = (*byte)(unsafe.Pointer(&names[i]))
		msgs[i].Msghdr.Iov = &iovecs[i]
		msgs[i].Msghdr.Iovlen = 1
		msgs[i].Msghdr.SetControllen(0)
	}

	var sendErr error
	err = raw.Write(func(fd uintptr) bool {
		sent := 0
		for sent < len(msgs) {
			r, _, e := syscall.Syscall6(
				unix.SYS_SENDMMSG,
				fd,
				uintptr(unsafe.Pointer(&msgs[sent])),
				uintptr(len(msgs)-sent),
				0, // flags
				0,
				0,
			)
			if e != 0 {
				if e == unix.EAGAIN || e == unix.EWOULDBLOCK {
					return false
				}
				sendErr = e
				return true
			}
			sent += int(r)
		}
		return true
	})
	if err != nil {
		return err
	}
	return sendErr
}

// fillSockaddr fills a RawSockaddrAny with the address family, port, and IP
// for the given UDP address.
func fillSockaddr(sa *unix.RawSockaddrAny, addr *net.UDPAddr) {
	ip4 := addr.IP.To4()
	if ip4 != nil {
		s := (*unix.RawSockaddrInet4)(unsafe.Pointer(sa))
		s.Family = unix.AF_INET
		s.Port = htons(uint16(addr.Port))
		copy(s.Addr[:], ip4)
	} else {
		ip6 := addr.IP.To16()
		s := (*unix.RawSockaddrInet6)(unsafe.Pointer(sa))
		s.Family = unix.AF_INET6
		s.Port = htons(uint16(addr.Port))
		copy(s.Addr[:], ip6)
	}
}

// toUDPAddr converts a RawSockaddrAny to a *net.UDPAddr.
func toUDPAddr(rsa *unix.RawSockaddrAny) *net.UDPAddr {
	switch rsa.Addr.Family {
	case unix.AF_INET:
		sa := (*unix.RawSockaddrInet4)(unsafe.Pointer(rsa))
		return &net.UDPAddr{
			IP:   net.IP(append([]byte(nil), sa.Addr[:]...)),
			Port: int(ntohs(sa.Port)),
		}
	case unix.AF_INET6:
		sa := (*unix.RawSockaddrInet6)(unsafe.Pointer(rsa))
		return &net.UDPAddr{
			IP:   net.IP(append([]byte(nil), sa.Addr[:]...)),
			Port: int(ntohs(sa.Port)),
		}
	}
	return nil
}

// htons converts a uint16 from host byte order to network byte order (big-endian).
func htons(v uint16) uint16 { return (v>>8)&0xff | (v&0xff)<<8 }

// ntohs converts a uint16 from network byte order to host byte order.
func ntohs(v uint16) uint16 { return (v>>8)&0xff | (v&0xff)<<8 }
