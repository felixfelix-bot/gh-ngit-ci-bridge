# gh-ngit-ci-bridge
PY ?= python3
SIGNER ?= /opt/miniconda/bin/python3
# the pytest entry point, not `python3 -m pytest`: on this host the pytest
# module lives only in the miniconda interpreter
PYTEST ?= pytest
CONFIG ?= config.json

.PHONY: help test dry once install logs lint signer-check audit \
        concurrency-test concurrency-dry concurrency-once concurrency-logs concurrency-install

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

test:  ## run the unit tests (decision matrix + public-only gate)
	$(PYTEST) -q tests/

lint:  ## byte-compile every module
	$(PY) -m py_compile bridge.py decision.py public_only.py nostr_event.py audit.py \
	  ci_concurrency.py ci_concurrency_controller.py \
	  tests/test_decision.py tests/test_public_only.py tests/test_ci_concurrency.py
	@echo "compile ok"

dry:  ## classify only: no publishes, no state changes
	$(PY) bridge.py --config $(CONFIG) --dry-run --verbose

once:  ## one real tick
	$(PY) bridge.py --config $(CONFIG)

audit:  ## per-repo visibility + triggerability report
	$(PY) audit.py --config $(CONFIG)

install:  ## install + enable the systemd user timer
	./install.sh

logs:  ## tail the bridge log
	tail -n 40 -f ~/.local/state/gh-ngit-ci-bridge/bridge.log

signer-check:  ## verify the signer can derive the maintainer pubkey (key never printed)
	@$(SIGNER) nostr_event.py --print-pubkey

concurrency-test:  ## unit tests for the Kalman-driven concurrency controller
	$(PYTEST) -q tests/test_ci_concurrency.py

concurrency-dry:  ## read the live Kalman signal and print the decision, apply nothing
	$(PY) ci_concurrency_controller.py --dry-run

concurrency-once:  ## one real tick: applies only when the coordinator is idle and the value changed
	$(PY) ci_concurrency_controller.py

concurrency-logs:  ## tail the decision log (signal, computed/previous limit, action, reason)
	tail -n 30 -f ~/.local/state/gh-ngit-ci-bridge/ci-concurrency.log

concurrency-install:  ## install + enable the concurrency systemd user timer
	./install-ci-concurrency.sh
