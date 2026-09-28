#!/usr/bin/env bash
# The Windows job of .github/workflows/ci.yml, run in a Parallels VM on this machine.
#
#   scripts/ci-windows.sh
#
# Tests the working tree as git sees it (like scripts/ci-linux.sh) on Windows 11 ARM,
# twice: x64 CPython 3.13 under emulation, the closest to GitHub's windows-latest,
# and native ARM64 CPython 3.13, which no hosted runner covers.
#
# The VM is win11-parlab-cnats, a clone kept for this repository and driven with
# pmlab.sh from the windows-parallels-lab skill. Each run reverts it to the
# snapshot cnats-tools, so nothing a run leaves behind reaches the next, and stops
# it afterwards. Set up once (2026-09-27):
#   - cloned from the stopped lab clone win11-parlab (not from a master VM)
#   - host-defined sharing off, one share: parlab -> ~/parlab-cnats
#   - hostname WIN11-CNATS; snapshot cnats-base
#   - C:\cnats: uv (aarch64), CPython 3.13 x64 and ARM64 via `uv python install`,
#     nats-server v2.14.6 windows-arm64; snapshot cnats-tools
set -euo pipefail

PMLAB_SH="${PMLAB_SH:-$HOME/Apps/itda-skills/hyve/skills/itda-dev/skills/windows-parallels-lab/scripts/pmlab.sh}"
export PMLAB_VM="${PMLAB_VM:-win11-parlab-cnats}"
export PMLAB_SHARE_DIR="${PMLAB_SHARE_DIR:-$HOME/parlab-cnats}"
export PMLAB_SNAP="${PMLAB_SNAP:-cnats-tools}"
export PMLAB_EXEC_TIMEOUT="${PMLAB_EXEC_TIMEOUT:-900}"
repo="$(cd "$(dirname "$0")/.." && pwd)"

[ -f "$PMLAB_SH" ] || { echo "ci-windows: pmlab.sh not found at $PMLAB_SH (set PMLAB_SH)" >&2; exit 2; }
set +u  # pmlab.sh is not written for nounset
# shellcheck source=/dev/null
source "$PMLAB_SH"

zip_path="$(mktemp -d)/src.zip"
(cd "$repo" && git ls-files -z --cached --others --exclude-standard | xargs -0 zip -q "$zip_path")
cp "$zip_path" "$PMLAB_SHARE_DIR/src.zip"
cp "$repo/scripts/ci-windows.ps1" "$PMLAB_SHARE_DIR/ci-windows.ps1"

pmlab_switch >/dev/null
pmlab_wait_ready >/dev/null
trap 'pmlab_stop >/dev/null 2>&1 || true' EXIT
# pmlab_wait_ready only sees exec succeed; right after a revert the share can
# still be coming up, and a run started then leaves no log (seen once).
for _ in $(seq 1 30); do
  pmlab_exec cmd /c dir '\\Mac\parlab\src.zip' >/dev/null 2>&1 && break
  sleep 2
done

status=0
for cell in "cpython-3.13-windows-x86_64-none x64" "cpython-3.13-windows-aarch64-none arm64"; do
  read -r python tag <<<"$cell"
  echo "=== windows $tag"
  rm -f "$PMLAB_SHARE_DIR/test-$tag.log"
  # Positional: pmlab's wrapper passes arguments on as plain strings, not -Name value.
  log="$PMLAB_SHARE_DIR/test-$tag.log"
  # Right after a revert, prlctl exec itself has failed now and then
  # ("PrlJob_GetRetCode: Invalid argument") without the guest running anything.
  # That is the VM, not the tests: one more try.
  for _ in 1 2; do
    said="$(pmlab_runps ci-windows.ps1 "$python" "$tag" 2>&1 || true)"
    [ -f "$log" ] && break
    echo "(no log yet; prlctl said: $(printf '%s' "$said" | LC_ALL=C tr -d '\r' | tail -1); trying once more)"
  done
  if [ ! -f "$log" ]; then
    echo "no log from the guest; it said:"
    printf '%s\n' "$said" | LC_ALL=C tr -d '\r' | tail -20
    status=1; continue
  fi
  # Everything but pytest's progress rows, so warnings printed after them show.
  LC_ALL=C tr -d '\r' <"$log" | grep -vE '^\.+ +\[ *[0-9]+%\]$' || true
  LC_ALL=C tr -d '\r' <"$log" | grep -q '^exit=0$' || status=1
done
exit $status
