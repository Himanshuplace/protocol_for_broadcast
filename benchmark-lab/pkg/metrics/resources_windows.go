//go:build windows

package metrics

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// readCPUTime reads user+system CPU time for the current process via the Win32
// GetProcessTimes API. kernelTime and userTime are FILETIME values expressed in
// 100-nanosecond units; Filetime.Nanoseconds() converts them to nanoseconds,
// which we scale to seconds to match the cross-platform cpuTime contract.
//
// This is the same accounting Task Manager and `Get-Process` use, so CPU%
// numbers are directly comparable to those tools.
func readCPUTime() cpuTime {
	var creation, exit, kernel, user windows.Filetime
	h := windows.CurrentProcess()
	if err := windows.GetProcessTimes(h, &creation, &exit, &kernel, &user); err != nil {
		return cpuTime{}
	}
	return cpuTime{
		user:   float64(user.Nanoseconds()) / 1e9,
		system: float64(kernel.Nanoseconds()) / 1e9,
	}
}

var (
	modkernel32               = windows.NewLazySystemDLL("kernel32.dll")
	procGetProcessHandleCount = modkernel32.NewProc("GetProcessHandleCount")
)

// countFDs returns the number of open kernel handles for the current process —
// the closest Windows equivalent to POSIX open file descriptors. Sockets, files,
// events, and threads all consume handles, so this tracks resource growth the
// same way /proc/self/fd does on Linux. Returns -1 if the call fails.
func countFDs() int {
	var count uint32
	h := windows.CurrentProcess()
	r, _, _ := procGetProcessHandleCount.Call(uintptr(h), uintptr(unsafe.Pointer(&count)))
	if r == 0 {
		return -1
	}
	return int(count)
}

// SetSocketBuf is a no-op on Windows. The Winsock stack auto-tunes
// SO_RCVBUF/SO_SNDBUF and manual tuning requires a raw socket handle that the
// transport layer does not expose here. Linux uses explicit setsockopt instead.
func SetSocketBuf(fd int, rcvBuf, sndBuf int) error { return nil }
