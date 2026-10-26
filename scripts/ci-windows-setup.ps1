# Puts the tools scripts/ci-windows.sh needs into %USERPROFILE%\cnats on the Windows
# CI machine. Idempotent: whatever is already in place is left as it is.
#
#   scp scripts/ci-windows-setup.ps1 allieus-macbook-2017-win10:cnats/
#   ssh allieus-macbook-2017-win10 'pwsh -NoProfile -File $env:USERPROFILE\cnats\ci-windows-setup.ps1'
#
# Needs PowerShell 7 (the ssh shell there). Writes only under -Root: PATH, the
# registry and services are not touched. Downloads are checked against the
# checksums each release publishes.
param(
    [string]$Root = "$env:USERPROFILE\cnats",
    [string]$UvVersion = "0.12.10",
    [string]$NatsVersion = "v2.15.0",
    [string[]]$Pythons = @("cpython-3.13-windows-x86_64-none", "cpython-3.14-windows-x86_64-none")
)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
New-Item -ItemType Directory -Force -Path $Root | Out-Null

function Get-Checked([string]$Url, [string]$Sums, [string]$Name) {
    $tmp = Join-Path $Root ".download-$Name"
    $sumsFile = "$tmp.sums"
    Invoke-WebRequest -Uri $Url -OutFile $tmp
    # Through a file: GitHub serves checksum files as octet-stream, so .Content is bytes.
    Invoke-WebRequest -Uri $Sums -OutFile $sumsFile
    $want = (Get-Content -LiteralPath $sumsFile |
        Where-Object { $_ -match [regex]::Escape($Name) } |
        Select-Object -First 1) -split '\s+' | Select-Object -First 1
    Remove-Item -LiteralPath $sumsFile
    $got = (Get-FileHash -Algorithm SHA256 -LiteralPath $tmp).Hash
    if (-not $want -or $got -ne $want.ToUpper()) {
        Remove-Item -LiteralPath $tmp
        throw "checksum mismatch for $Name (want '$want', got '$got')"
    }
    $tmp
}

$uv = "$Root\uv\uv.exe"
if (Test-Path $uv) {
    Write-Output "uv: present ($(& $uv --version))"
} else {
    $name = "uv-x86_64-pc-windows-msvc.zip"
    $base = "https://github.com/astral-sh/uv/releases/download/$UvVersion"
    $zip = Get-Checked "$base/$name" "$base/$name.sha256" $name
    Expand-Archive -Force -LiteralPath $zip -DestinationPath "$Root\uv"
    Remove-Item -LiteralPath $zip
    Write-Output "uv: installed ($(& $uv --version))"
}

# The zip holds one directory of the same name.
$natsDir = "nats-server-$NatsVersion-windows-amd64"
$nats = "$Root\$natsDir\nats-server.exe"
if (Test-Path $nats) {
    Write-Output "nats-server: present ($(& $nats --version))"
} else {
    $name = "$natsDir.zip"
    $base = "https://github.com/nats-io/nats-server/releases/download/$NatsVersion"
    $zip = Get-Checked "$base/$name" "$base/SHA256SUMS" $name
    Expand-Archive -Force -LiteralPath $zip -DestinationPath $Root
    Remove-Item -LiteralPath $zip
    Write-Output "nats-server: installed ($(& $nats --version))"
}

# uv skips versions already installed.
$env:UV_PYTHON_INSTALL_DIR = "$Root\python"
$env:UV_CACHE_DIR = "$Root\uv-cache"
& $uv python install --no-bin --no-registry @Pythons
if ($LASTEXITCODE) { throw "uv python install failed ($LASTEXITCODE)" }
& $uv python list --only-installed
