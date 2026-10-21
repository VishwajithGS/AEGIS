<#
.SYNOPSIS  Start the React/Vite dashboard dev server on http://127.0.0.1:5173
#>
. "$PSScriptRoot\common.ps1"
Set-Location "$AegisRoot\frontend"
if (-not (Test-Path "node_modules")) { npm install --no-audit --no-fund }
if (-not (Test-Path ".env.local")) { Copy-Item ".env.example" ".env.local" }
Write-Step "Dashboard on http://127.0.0.1:5173 (Ctrl+C to stop)"
npm run dev
