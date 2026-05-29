#!/usr/bin/env bash
# netem-teardown.sh — remove all tc/netem qdisc from the primary interface.
set -euo pipefail
IFACE="${1:-$(ip route show default | awk '/default/ { print $5 }' | head -1)}"
echo "Clearing netem on $IFACE"
tc qdisc del dev "$IFACE" root 2>/dev/null && echo "Cleared." || echo "Nothing to clear."
