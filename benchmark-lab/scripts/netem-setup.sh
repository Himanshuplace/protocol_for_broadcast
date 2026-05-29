#!/usr/bin/env bash
# netem-setup.sh — apply tc/netem impairment profiles to the primary interface.
# Usage: ./scripts/netem-setup.sh <profile> [interface]
# Profiles: clean loss1 loss5 loss10 latency20 latency50 latency100 reorder duplicate jitter wan
# Requires: root or CAP_NET_ADMIN
set -euo pipefail

PROFILE="${1:-clean}"
IFACE="${2:-}"

# Auto-detect interface if not specified
if [ -z "$IFACE" ]; then
  IFACE=$(ip route show default | awk '/default/ { print $5 }' | head -1)
  [ -z "$IFACE" ] && { echo "Cannot detect interface"; exit 1; }
fi

echo "Applying profile '$PROFILE' on interface '$IFACE'"

# Clear existing qdisc
tc qdisc del dev "$IFACE" root 2>/dev/null || true

case "$PROFILE" in
  clean)
    echo "Network cleared (no impairment)"
    ;;
  loss1)
    tc qdisc add dev "$IFACE" root netem loss 1.00%
    ;;
  loss5)
    tc qdisc add dev "$IFACE" root netem loss 5.00%
    ;;
  loss10)
    tc qdisc add dev "$IFACE" root netem loss 10.00%
    ;;
  loss20)
    tc qdisc add dev "$IFACE" root netem loss 20.00%
    ;;
  latency20)
    tc qdisc add dev "$IFACE" root netem delay 20ms 2ms distribution normal
    ;;
  latency50)
    tc qdisc add dev "$IFACE" root netem delay 50ms 5ms distribution normal
    ;;
  latency100)
    tc qdisc add dev "$IFACE" root netem delay 100ms 10ms distribution normal
    ;;
  reorder)
    tc qdisc add dev "$IFACE" root netem delay 10ms reorder 25.00%
    ;;
  duplicate)
    tc qdisc add dev "$IFACE" root netem duplicate 1.00%
    ;;
  jitter)
    tc qdisc add dev "$IFACE" root netem delay 20ms 15ms distribution normal
    ;;
  wan)
    tc qdisc add dev "$IFACE" root netem delay 50ms 10ms distribution normal loss 0.50%
    ;;
  mobile4g)
    tc qdisc add dev "$IFACE" root netem delay 40ms 20ms distribution normal loss 1.00%
    ;;
  *)
    echo "Unknown profile: $PROFILE"
    exit 1
    ;;
esac

echo "Done."
