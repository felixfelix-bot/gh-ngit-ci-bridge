#!/usr/bin/env bash
# Idempotent installer for the gh-ngit-ci-bridge systemd user timer.
set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/repos/gh-ngit-ci-bridge}"
UNIT_DIR="$HOME/.config/systemd/user"

echo "== installing units from $REPO_DIR"
mkdir -p "$UNIT_DIR"
cp "$REPO_DIR/systemd/gh-ngit-ci-bridge.service" "$UNIT_DIR/gh-ngit-ci-bridge.service"
cp "$REPO_DIR/systemd/gh-ngit-ci-bridge.timer"   "$UNIT_DIR/gh-ngit-ci-bridge.timer"

echo "== state/log dirs"
mkdir -p "$HOME/.local/state/gh-ngit-ci-bridge" "$HOME/.local/share/gh-ngit-ci-bridge/cache"
chmod 700 "$HOME/.local/state/gh-ngit-ci-bridge" "$HOME/.local/share/gh-ngit-ci-bridge"

echo "== reload + enable"
systemctl --user daemon-reload
systemctl --user enable --now gh-ngit-ci-bridge.timer
systemctl --user list-timers gh-ngit-ci-bridge.timer --all

echo
echo "next run:   systemctl --user list-timers gh-ngit-ci-bridge.timer"
echo "run now:    systemctl --user start gh-ngit-ci-bridge.service"
echo "logs:       journalctl --user -u gh-ngit-ci-bridge.service -n 50 --no-pager"
echo "bridge log: $HOME/.local/state/gh-ngit-ci-bridge/bridge.log"
