//go:build linux

package metrics

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// readCPUTime reads user+system CPU time for the current process from /proc/self/stat.
// Linux-specific: /proc/self/stat fields 14 (utime) and 15 (stime) in clock ticks.
// Divides by sysconf(_SC_CLK_TCK) = 100 to get seconds.
//
// This is the same method used by `ps`, `top`, and `docker stats`.
func readCPUTime() cpuTime {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return cpuTime{}
	}
	// Find the closing ')' of the process name field
	// /proc/self/stat format: pid (name) state ppid ... utime stime ...
	// utime is field 14 (0-indexed: 13), stime is field 15 (0-indexed: 14)
	// after the closing ')'
	s := string(data)
	end := strings.LastIndex(s, ")")
	if end < 0 {
		return cpuTime{}
	}
	fields := strings.Fields(s[end+2:])
	if len(fields) < 13 {
		return cpuTime{}
	}
	// fields[11] = utime (field 14 in stat, 0-indexed from after ')')
	// fields[12] = stime (field 15)
	utime, _ := strconv.ParseFloat(fields[11], 64)
	stime, _ := strconv.ParseFloat(fields[12], 64)
	// CLK_TCK is typically 100 on Linux (0.01s per tick)
	const clkTck = 100.0
	return cpuTime{
		user:   utime / clkTck,
		system: stime / clkTck,
	}
}

// countFDs counts the number of open file descriptors by listing /proc/self/fd.
// This is O(FD count) due to the directory read, but only called every 100ms.
// Returns -1 on error.
func countFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

// getRSS returns the resident set size (RSS) in bytes from /proc/self/status.
// More accurate than runtime.ReadMemStats.Sys for cross-language comparison.
// Falls back to 0 on error.
func getRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.ParseUint(fields[1], 10, 64)
				return kb * 1024
			}
		}
	}
	return 0
}

// setSocketBuf sets SO_RCVBUF and SO_SNDBUF on the given file descriptor.
// On Linux, the kernel doubles the requested size (kernel overhead), so we set
// to 2× the desired size and the effective buffer will be the requested amount.
// Requires CAP_NET_ADMIN to exceed /proc/sys/net/core/{r,w}mem_max.
func SetSocketBuf(fd int, rcvBuf, sndBuf int) error {
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF, rcvBuf); err != nil {
		return err
	}
	return syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, sndBuf)
}
