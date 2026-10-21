<#
.SYNOPSIS  Start the FastAPI backend (container, with Spark MLlib inference) or on the host (-OnHost, no model hosting).
#>
param([switch]$OnHost)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
if ($OnHost) {
    $py = "$AegisRoot\.venv\Scripts\python.exe"
    if (-not (Test-Path $py)) { throw "Host venv missing. Run scripts\setup.ps1 -HostVenv" }
    Write-Step "Starting the API on the host (model predictions need the container: Spark/Java 17)"
    & $py -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
    return
}
Assert-Docker
Invoke-Compose up -d api
Write-Ok "API: http://localhost:8000  |  Swagger UI: http://localhost:8000/docs"
