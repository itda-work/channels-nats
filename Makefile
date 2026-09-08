.PHONY: install test lint format check bench nats

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

# Fan-out benchmark (see bench/fanout.py). Override: make bench ARGS="--members 5000 --processes 8"
bench:
	uv run python -m bench.fanout $(ARGS)

# Run a local nats-server (PATH, or ~/go/bin from `go install`)
nats:
	$${NATS_SERVER:-nats-server} -p 4222
