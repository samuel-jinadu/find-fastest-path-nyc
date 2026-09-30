#!/usr/bin/env bash
set -euo pipefail

echo "Fixing legacy iptables FORWARD policy (Codespaces)..."
sudo iptables-legacy -P FORWARD ACCEPT
sudo iptables-legacy -I FORWARD 1 -i br-+ -j ACCEPT
sudo iptables-legacy -I FORWARD 1 -o br-+ -j ACCEPT
sudo iptables-legacy -L FORWARD -n | head -1

echo "Starting docker compose..."
docker compose up --no-color --timestamps 2>&1 | tee compose.log