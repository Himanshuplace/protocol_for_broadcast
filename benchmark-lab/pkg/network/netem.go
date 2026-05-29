package network

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"

	"go.uber.org/zap"
)

// NetemController applies tc/netem network impairment profiles on a Linux interface.
// Requires CAP_NET_ADMIN or root.
type NetemController struct {
	iface   string
	applied bool
	logger  *zap.Logger
}

// NewNetemController creates a controller for the given interface.
// If iface is empty, DetectInterface() is called automatically.
func NewNetemController(iface string, logger *zap.Logger) *NetemController {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &NetemController{iface: iface, logger: logger}
}

// Apply activates the given impairment profile on the interface.
// Any previously applied qdisc is cleared first.
func (n *NetemController) Apply(profile Profile) error {
	if n.iface == "" {
		iface, err := n.DetectInterface()
		if err != nil {
			return fmt.Errorf("netem: detect interface: %w", err)
		}
		n.iface = iface
	}

	if n.applied {
		_ = n.Clear()
	}

	if profile.Name == "clean" {
		n.applied = false
		return nil
	}

	args := n.buildArgs(profile)
	cmd := exec.CommandContext(context.Background(), "tc", args...)
	n.logger.Info("netem: applying profile",
		zap.String("profile", profile.Name),
		zap.String("cmd", "tc "+strings.Join(args, " ")),
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("netem: tc failed: %s: %w", string(out), err)
	}
	n.applied = true
	return nil
}

// Clear removes any tc qdisc added by this controller.
func (n *NetemController) Clear() error {
	if n.iface == "" {
		return nil
	}
	cmd := exec.Command("tc", "qdisc", "del", "dev", n.iface, "root")
	if out, err := cmd.CombinedOutput(); err != nil {
		n.logger.Debug("netem: clear qdisc (may not exist)", zap.String("out", string(out)))
	}
	n.applied = false
	return nil
}

// DetectInterface returns the primary non-loopback network interface name.
func (n *NetemController) DetectInterface() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		if len(addrs) > 0 {
			return iface.Name, nil
		}
	}
	return "", fmt.Errorf("netem: no suitable interface found")
}

// Interface returns the interface name in use.
func (n *NetemController) Interface() string { return n.iface }

func (n *NetemController) buildArgs(p Profile) []string {
	args := []string{"qdisc", "add", "dev", n.iface, "root", "netem"}

	if p.DelayMs > 0 {
		if p.JitterMs > 0 {
			args = append(args, "delay", fmt.Sprintf("%dms", p.DelayMs), fmt.Sprintf("%dms", p.JitterMs), "distribution", "normal")
		} else {
			args = append(args, "delay", fmt.Sprintf("%dms", p.DelayMs))
		}
	}

	if p.LossPct > 0 {
		args = append(args, "loss", fmt.Sprintf("%.2f%%", p.LossPct))
	}

	if p.DuplicatePct > 0 {
		args = append(args, "duplicate", fmt.Sprintf("%.2f%%", p.DuplicatePct))
	}

	if p.ReorderPct > 0 {
		// reorder requires a delay to be set
		if p.DelayMs == 0 {
			args = append(args, "delay", "10ms")
		}
		args = append(args, "reorder", fmt.Sprintf("%.2f%%", p.ReorderPct))
	}

	if p.CorruptPct > 0 {
		args = append(args, "corrupt", fmt.Sprintf("%.2f%%", p.CorruptPct))
	}

	if p.Rate != "" {
		args = append(args, "rate", p.Rate)
	}

	return args
}
