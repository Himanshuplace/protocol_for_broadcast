//go:build windows

package metrics

func readCPUTime() cpuTime { return cpuTime{} }

func countFDs() int { return -1 }

func SetSocketBuf(fd int, rcvBuf, sndBuf int) error { return nil }
