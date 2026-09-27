.PHONY: install test lint format check bench nats release-check ci-local ci-linux ci-windows

install:
	uv sync --all-extras

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

check:
	uv run pyright

# The CI matrix on this machine: Linux in docker (3.13, 3.14) and Windows 11 ARM in a
# Parallels VM (x64 and ARM64 CPython 3.13). GitHub CI is manual-only; this is the gate.
# `make -j2 ci-local` runs the two sides side by side.
ci-local: ci-linux ci-windows

ci-linux:
	scripts/ci-linux.sh

ci-windows:
	scripts/ci-windows.sh

# Everything that must line up before a v<version> tag is pushed
release-check:
	python3 scripts/release_check.py

# Fan-out benchmark (see bench/fanout.py). Override: make bench ARGS="--members 5000 --processes 8"
bench:
	uv run python -m bench.fanout $(ARGS)

# Run a local nats-server (PATH, or ~/go/bin from `go install`)
nats:
	$${NATS_SERVER:-nats-server} -p 4222
