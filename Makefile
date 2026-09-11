# gh-ngit-ci-bridge
PY ?= python3
SIGNER ?= /opt/miniconda/bin/python3
CONFIG ?= config.json

.PHONY: help test dry once install logs lint

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

test:  ## run the decision-logic unit tests
	$(PY) -m pytest -q tests/test_decision.py

lint:  ## byte-compile every module
	$(PY) -m py_compile bridge.py decision.py nostr_event.py tests/test_decision.py
	@echo "compile ok"

dry:  ## classify only: no publishes, no state changes
	$(PY) bridge.py --config $(CONFIG) --dry-run --verbose

once:  ## one real tick
	$(PY) bridge.py --config $(CONFIG)

install:  ## install + enable the systemd user timer
	./install.sh

logs:  ## tail the bridge log
	tail -n 40 -f ~/.local/state/gh-ngit-ci-bridge/bridge.log

signer-check:  ## verify the signer can derive the maintainer pubkey (key never printed)
	@$(SIGNER) nostr_event.py --print-pubkey
