#!/usr/bin/env bash
# The Linux job of .github/workflows/ci.yml, run in a container on this machine.
#
#   scripts/ci-linux.sh [python-version ...]     (default: 3.13 3.14)
#
# Tests the working tree as git sees it -- tracked files plus new ones not ignored,
# uncommitted edits included -- so it can gate a commit. Needs a docker daemon.
# The container runs the host's architecture (arm64 on Apple silicon); GitHub's
# runner is amd64.
set -euo pipefail

NATS_VERSION="${NATS_VERSION:-v2.14.6}"
repo="$(cd "$(dirname "$0")/.." && pwd)"
versions=("$@")
[ ${#versions[@]} -gt 0 ] || versions=(3.13 3.14)

# macOS tar would add AppleDouble ._* files for extended attributes.
export COPYFILE_DISABLE=1

status=0
for py in "${versions[@]}"; do
  echo "=== linux python $py"
  # A named volume keeps uv's cache between runs.
  if ! git -C "$repo" ls-files -z --cached --others --exclude-standard \
    | tar -C "$repo" --no-xattrs --null -T - -cf - \
    | docker run --rm -i -e PY="$py" -e NATS_VERSION="$NATS_VERSION" \
        -v channels-nats-uv-cache:/root/.cache/uv "python:$py" bash -c '
set -euo pipefail
mkdir /w && cd /w && tar xf -
pip install -q --root-user-action=ignore uv
arch=$(uname -m | sed "s/aarch64/arm64/; s/x86_64/amd64/")
name="nats-server-$NATS_VERSION-linux-$arch"
curl -fsSL --retry 3 -o /tmp/nats.tgz "https://github.com/nats-io/nats-server/releases/download/$NATS_VERSION/$name.tar.gz"
tar xzf /tmp/nats.tgz -C /tmp
export PATH="/tmp/$name:$PATH"
uv sync -q --all-extras
uv run ruff check .
uv run ruff format --check .
if [ "$PY" = 3.13 ]; then uv run pyright; fi
uv run pytest -o addopts= -q
'; then
    status=1
    echo "=== linux python $py: FAILED"
  fi
done
exit $status
