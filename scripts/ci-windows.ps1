# Runs the test suite inside the Windows lab VM. Driven by scripts/ci-windows.sh,
# which puts this file and src.zip on the share (\\Mac\parlab) first.
#
# Runs as SYSTEM through `prlctl exec`. The tools live in C:\cnats, installed once
# when the VM was set up (see scripts/ci-windows.sh).
param(
    [string]$Python = "cpython-3.13-windows-x86_64-none",
    [string]$Tag = "x64"
)
$ErrorActionPreference = "Continue"
$root = "C:\cnats"
$out = "\\Mac\parlab\test-$Tag.log"
$env:UV_PYTHON_INSTALL_DIR = "$root\python"
$env:UV_PYTHON = $Python
$env:NATS_SERVER = "$root\nats-server-v2.14.6-windows-arm64\nats-server.exe"

$src = "$root\src-$Tag"
if (Test-Path $src) { Remove-Item -Recurse -Force $src }
Expand-Archive -Force "\\Mac\parlab\src.zip" $src
Set-Location $src

# Through cmd: PowerShell 5.1 turns a native command's stderr into errors.
cmd /c "$root\uv\uv.exe sync -q --all-extras > $root\sync-$Tag.log 2>&1"
cmd /c "$root\uv\uv.exe run python -c ""import sys; print(sys.version)"" > $out 2>&1"
cmd /c "$root\uv\uv.exe run pytest -o addopts= -q -p no:cacheprovider >> $out 2>&1"
"exit=$LASTEXITCODE" | Out-File $out -Append -Encoding ascii
Get-Content "$root\sync-$Tag.log" | Out-File $out -Append -Encoding utf8
