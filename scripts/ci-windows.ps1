# Runs the test suite on the Windows CI machine. Driven by scripts/ci-windows.sh,
# which copies this file and src.zip into %USERPROFILE%\cnats\<Run> first and
# removes that directory afterwards.
#
# Runs as the ssh user under PowerShell 7. The tools live in %USERPROFILE%\cnats
# (see scripts/ci-windows-setup.ps1); PATH is not used for them.
param(
    [Parameter(Mandatory)][string]$Run,
    [string]$Python = "cpython-3.13-windows-x86_64-none",
    [string]$Tag = "x64-3.13"
)
$ErrorActionPreference = "Continue"
$root = "$env:USERPROFILE\cnats"
$work = "$root\$Run"
$out = "$work\test-$Tag.log"
$env:UV_PYTHON_INSTALL_DIR = "$root\python"
$env:UV_CACHE_DIR = "$root\uv-cache"
$env:UV_PYTHON = $Python
$env:NATS_SERVER = "$root\nats-server-v2.15.0-windows-amd64\nats-server.exe"
$uv = "$root\uv\uv.exe"

$src = "$work\src-$Tag"
Expand-Archive -Force "$work\src.zip" $src
Set-Location $src

# PowerShell 7 writes a native command's redirected stderr as plain text, so no cmd.
& $uv sync -q --all-extras *> "$work\sync-$Tag.log"
& $uv run python -c "import platform, sys; print(sys.version, platform.machine())" *> $out
& $uv run pytest -o addopts= -q -p no:cacheprovider *>> $out
"exit=$LASTEXITCODE" | Out-File $out -Append -Encoding ascii
Get-Content "$work\sync-$Tag.log" | Out-File $out -Append -Encoding utf8
