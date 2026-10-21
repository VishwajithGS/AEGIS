<#
.SYNOPSIS  One-time setup: checks prerequisites, builds the Docker images, installs the dashboard.
.EXAMPLE   powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
.EXAMPLE   powershell -ExecutionPolicy Bypass -File scripts\setup.ps1 -HostVenv   # also create .venv for host tools/tests
#>
param([switch]$HostVenv, [switch]$SkipFrontend)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot

Write-Step "Checking prerequisites"
Assert-Docker
$c = Get-PSDrive C -ErrorAction SilentlyContinue
if ($c -and $c.Free -lt 6GB) { Write-Warn2 ("Drive C: has only {0:N1} GB free. Docker images need ~5 GB; move Docker Desktop's disk image (Settings > Resources > Advanced) if C: is low." -f ($c.Free / 1GB)) }
if (-not $SkipFrontend) {
    $null = Get-Command node -ErrorAction SilentlyContinue
    if (-not $?) { throw "Node.js not found (needed for the dashboard). Install Node 18+ or use -SkipFrontend." }
    Write-Ok ("Node " + (node --version))
}

Write-Step "Creating data directories"
foreach ($d in "data\generated", "data\reference", "data\lake", "data\checkpoints", "data\state", "data\reports", "data\models", "data\dead_letter") {
    New-Item -ItemType Directory -Force (Join-Path $AegisRoot $d) | Out-Null
}
Write-Ok "data\ ready"

Write-Step "Building Docker images (aegis-spark: Python 3.11 + Java 17 + PySpark 3.5.9; aegis-hbase: HBase 2.5.16)"
Invoke-Compose build hbase tools
Write-Ok "images built"

if ($HostVenv) {
    Write-Step "Creating host virtual environment .venv (producers, API without Spark, unit tests)"
    if (-not (Test-Path "$AegisRoot\.venv")) { python -m venv "$AegisRoot\.venv" }
    & "$AegisRoot\.venv\Scripts\python.exe" -m pip install --upgrade pip
    & "$AegisRoot\.venv\Scripts\python.exe" -m pip install -r "$AegisRoot\requirements\host.txt"
    Write-Ok ".venv ready (activate: .\.venv\Scripts\Activate.ps1)"
}

if (-not $SkipFrontend) {
    Write-Step "Installing dashboard dependencies"
    Push-Location "$AegisRoot\frontend"
    try { npm install --no-audit --no-fund; if ($LASTEXITCODE -ne 0) { throw "npm install failed" } } finally { Pop-Location }
    if (-not (Test-Path "$AegisRoot\frontend\.env.local")) { Copy-Item "$AegisRoot\frontend\.env.example" "$AegisRoot\frontend\.env.local" }
    Write-Ok "frontend ready"
}
Write-Host "`nSetup complete. Next: scripts\start_infrastructure.ps1" -ForegroundColor Green
