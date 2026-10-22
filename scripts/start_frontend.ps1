<#
.SYNOPSIS  Start the React/Vite dashboard dev server.
.EXAMPLE   scripts\start_frontend.ps1             # synthetic demo: http://127.0.0.1:5173 -> API :8000
.EXAMPLE   scripts\start_frontend.ps1 -RealData   # real historical data: http://127.0.0.1:5174 -> API :8001
.EXAMPLE   scripts\start_frontend.ps1 -Hybrid     # real rainfall + synthetic fill: http://127.0.0.1:5175 -> API :8002
#>
param([switch]$RealData, [switch]$Hybrid)
. "$PSScriptRoot\common.ps1"
if ($RealData -and $Hybrid) { throw "use either -RealData or -Hybrid" }
Set-Location "$AegisRoot\frontend"
if (-not (Test-Path "node_modules")) { npm install --no-audit --no-fund }
if (-not (Test-Path ".env.local")) { Copy-Item ".env.example" ".env.local" }
if ($RealData) {
    $env:VITE_API_BASE_URL = "http://localhost:8001"          # process env overrides .env.local
    Write-Step "Dashboard (REAL historical data) on http://127.0.0.1:5174 -> API http://localhost:8001 (Ctrl+C to stop)"
    npm run dev -- --port 5174 --strictPort
} elseif ($Hybrid) {
    $env:VITE_API_BASE_URL = "http://localhost:8002"
    Write-Step "Dashboard (HYBRID: real rainfall + synthetic fill) on http://127.0.0.1:5175 -> API http://localhost:8002 (Ctrl+C to stop)"
    npm run dev -- --port 5175 --strictPort
} else {
    Write-Step "Dashboard (synthetic demo) on http://127.0.0.1:5173 (Ctrl+C to stop)"
    npm run dev -- --strictPort
}
