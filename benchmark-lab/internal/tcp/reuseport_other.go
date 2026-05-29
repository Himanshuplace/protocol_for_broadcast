//go:build !linux

package tcp

import (
	"syscall"
)

// setReusePort is a no-op on non-Linux platforms.
// SO_REUSEPORT semantics differ between BSDs and macOS; for portability we
// skip the setsockopt call. A single listener is used on these platforms.
func setReusePort(raw syscall.RawConn) error {
	return nil
}
