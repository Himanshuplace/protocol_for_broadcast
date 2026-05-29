//go:build linux

package tcp

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// setReusePort sets SO_REUSEPORT on the socket via SyscallConn.Control.
// On Linux this allows multiple listeners to bind to the same port; the kernel
// load-balances incoming connections across them.
func setReusePort(raw syscall.RawConn) error {
	var setErr error
	err := raw.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	})
	if err != nil {
		return err
	}
	return setErr
}
