//go:build windows

package udp

import (
	"fmt"
	"net"
)

func setSocketBuf(_ *net.UDPConn, _ int) error { return nil }

func batchRecv(_ *net.UDPConn, _ [][]byte, _ []*net.UDPAddr) (int, error) {
	return 0, fmt.Errorf("batchRecv: not supported on windows")
}

func batchSend(_ *net.UDPConn, _ []*net.UDPAddr, _ []byte) error {
	return fmt.Errorf("batchSend: not supported on windows")
}
