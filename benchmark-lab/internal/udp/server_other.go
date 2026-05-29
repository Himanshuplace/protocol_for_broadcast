//go:build !linux

package udp

import (
	"fmt"
	"net"
	"syscall"
)

// setSocketBuf sets SO_RCVBUF and SO_SNDBUF on non-Linux platforms.
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

// batchRecv is a no-op stub on non-Linux platforms.
// The server falls back to receiveLoopGeneric on these platforms.
func batchRecv(conn *net.UDPConn, bufs [][]byte, addrs []*net.UDPAddr) (int, error) {
	return 0, fmt.Errorf("batchRecv: not supported on this platform")
}

// batchSend is a no-op stub on non-Linux platforms.
// The server falls back to WriteToUDP on these platforms.
func batchSend(conn *net.UDPConn, addrs []*net.UDPAddr, data []byte) error {
	return fmt.Errorf("batchSend: not supported on this platform")
}
