#!/usr/bin/env bash
# The Windows job of .github/workflows/ci.yml, run over SSH on a Windows machine.
#
#   scripts/ci-windows.sh
#
# Tests the working tree as git sees it (like scripts/ci-linux.sh) on native x64
# Windows, twice: x64 CPython 3.13 (the floor) and 3.14, the same versions as Linux.
# ARM64 Windows is not covered.
#
# The host is CI_WINDOWS_HOST, an ssh alias (default allieus-macbook-2017-win10:
# Windows 10 22H2 AMD64, PowerShell 7 as the ssh shell, so remote commands are
# PowerShell). The tools sit in %USERPROFILE%\cnats and nowhere else; PATH is left
# alone. scripts/ci-windows-setup.ps1 puts them there (checked 2026-10-26):
#   - uv\uv.exe 0.12.10 (x86_64-pc-windows-msvc), uv-cache\
#   - nats-server-v2.15.0-windows-amd64\nats-server.exe
#   - python\ (UV_PYTHON_INSTALL_DIR): CPython 3.13 and 3.14 x86_64
#
# Each run works in its own directory, %USERPROFILE%\cnats\run-<id>, and removes only
# that directory at the end. Unlike a VM snapshot, nothing undoes what a run leaves
# behind, so the run reports nats-server processes still alive afterwards. It does
# not kill them: on a shared machine they may not be ours.
set -euo pipefail

host="${CI_WINDOWS_HOST:-allieus-macbook-2017-win10}"
repo="$(cd "$(dirname "$0")/.." && pwd)"
ssh_opts=(-o BatchMode=yes -o ConnectTimeout=15)
run_id="run-$(date +%Y%m%d-%H%M%S)-$$"

rsh() { ssh "${ssh_opts[@]}" "$host" "$@"; }

if ! said="$(rsh 'Write-Output ok' 2>&1)"; then
  echo "ci-windows: cannot reach $host over ssh (set CI_WINDOWS_HOST):" >&2
  printf '%s\n' "$said" | LC_ALL=C tr -d '\r' | tail -5 >&2
  exit 2
fi

local_dir="$(mktemp -d)"
zip_path="$local_dir/src.zip"
(cd "$repo" && git ls-files -z --cached --others --exclude-standard | xargs -0 zip -q "$zip_path")

rsh "New-Item -ItemType Directory -Path \"\$env:USERPROFILE\\cnats\\$run_id\" | Out-Null"
cleanup() {
  # Only this run's directory, by name; the guard stops an empty path from
  # turning into a removal of cnats itself.
  rsh "\$d = Join-Path \$env:USERPROFILE 'cnats\\$run_id'
if (-not '$run_id' -or -not (Test-Path -LiteralPath \$d)) { throw \"no run directory: \$d\" }
Remove-Item -Recurse -Force -LiteralPath \$d" >/dev/null 2>&1 \
    || echo "ci-windows: could not remove %USERPROFILE%\\cnats\\$run_id on $host" >&2
}
trap cleanup EXIT
# scp paths are relative to the remote home directory.
scp -q "${ssh_opts[@]}" "$zip_path" "$repo/scripts/ci-windows.ps1" "$host:cnats/$run_id/"

status=0
for python in 3.13 3.14; do
  tag="x64-$python"
  echo "=== windows $tag"
  log="$local_dir/test-$tag.log"
  rsh "pwsh -NoProfile -NonInteractive -File \"\$env:USERPROFILE\\cnats\\$run_id\\ci-windows.ps1\" -Run $run_id -Python cpython-$python-windows-x86_64-none -Tag $tag" \
    >/dev/null 2>&1 || true
  if ! scp -q "${ssh_opts[@]}" "$host:cnats/$run_id/test-$tag.log" "$log" 2>/dev/null; then
    echo "no log from $host"
    status=1; continue
  fi
  # Everything but pytest's progress rows, so warnings printed after them show.
  LC_ALL=C tr -d '\r' <"$log" | grep -vE '^\.+ +\[ *[0-9]+%\]$' || true
  LC_ALL=C tr -d '\r' <"$log" | grep -q '^exit=0$' || status=1
done

# Report, don't kill: a leftover here means a test did not stop its server.
left="$(rsh "Get-Process nats-server -ErrorAction SilentlyContinue | Where-Object Path -like \"\$env:USERPROFILE\\cnats\\*\" | ForEach-Object { \"\$(\$_.Id) \$(\$_.Path)\" }" 2>/dev/null | LC_ALL=C tr -d '\r' || true)"
if [ -n "$left" ]; then
  echo "=== windows: nats-server still running after the run (not stopped):"
  printf '%s\n' "$left"
fi
exit $status
