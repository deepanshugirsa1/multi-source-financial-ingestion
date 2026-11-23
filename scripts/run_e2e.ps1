$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$python = Join-Path $root ".venv\Scripts\python.exe"
if ($root.Length -gt 120) {
    $short = Join-Path $env:USERPROFILE "venvs\finingest"
    if (-not (Test-Path $short)) {
        python -m venv $short
    }
    $python = Join-Path $short "Scripts\python.exe"
} elseif (-not (Test-Path $python)) {
    python -m venv (Join-Path $root ".venv")
}
& $python -m pip install -r requirements.txt
& $python scripts\run_e2e.py
