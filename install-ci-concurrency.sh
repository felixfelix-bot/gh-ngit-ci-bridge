#!/usr/bin/env bash
# Idempotent installer for the kalman-ci-concurrency systemd user timer.
#
# Installs the unit pair and enables the timer. The units' ExecStart points at
# $REPO_DIR, so the repo must be checked out where the service expects it
# (default: ~/repos/gh-ngit-ci-bridge).
set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/repos/gh-ngit-ci-bridge}"
UNIT_DIR="$HOME/.config/systemd/user"
STATE_DIR="$HOME/.local/state/gh-ngit-ci-bridge"

echo "== installing units from $REPO_DIR"
[ -f "$REPO_DIR/ci_concurrency_controller.py" ] || {
  echo "ERROR: $REPO_DIR/ci_concurrency_controller.py not found" >&2
  exit 1
}
mkdir -p "$UNIT_DIR"
cp "$REPO_DIR/systemd/kalman-ci-concurrency.service" "$UNIT_DIR/kalman-ci-concurrency.service"
cp "$REPO_DIR/systemd/kalman-ci-concurrency.timer"   "$UNIT_DIR/kalman-ci-concurrency.timer"

echo "== state/log dir"
mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"

echo "== reload + enable"
systemctl --user daemon-reload
systemctl --user enable --now kalman-ci-concurrency.timer
systemctl --user list-timers kalman-ci-concurrency.timer --all

echo
echo "next run:  systemctl --user list-timers kalman-ci-concurrency.timer"
echo "run now:   systemctl --user start kalman-ci-concurrency.service"
echo "journal:   journalctl --user -u kalman-ci-concurrency.service -n 50 --no-pager"
echo "decisions: $STATE_DIR/ci-concurrency.log"
echo "override:  echo 2 > $STATE_DIR/ci-concurrency.override   # pin; rm to unpin"
