//go:build !linux && !windows

package metrics

import (
	"syscall"
)

// readCPUTime returns process CPU time using syscall.Getrusage on non-Linux platforms.
// Works on macOS, FreeBSD, and other POSIX systems.
func readCPUTime() cpuTime {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return cpuTime{}
	}
	toSec := func(tv syscall.Timeval) float64 {
		return float64(tv.Sec) + float64(tv.Usec)*1e-6
	}
	return cpuTime{
		user:   toSec(ru.Utime),
		system: toSec(ru.Stime),
	}
}

// countFDs returns -1 on non-Linux platforms (no /proc filesystem).
func countFDs() int { return -1 }

// SetSocketBuf is a no-op on non-Linux platforms.
func SetSocketBuf(fd int, rcvBuf, sndBuf int) error { return nil }
