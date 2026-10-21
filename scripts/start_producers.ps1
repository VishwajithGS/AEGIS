<#
.SYNOPSIS  Publish SYNTHETIC live events to Kafka.
.EXAMPLE   scripts\start_producers.ps1                         # all sources, detached container, 4 events/s
.EXAMPLE   scripts\start_producers.ps1 -Rate 10 -Duration 120  # foreground, stops after 120 s
.EXAMPLE   scripts\start_producers.ps1 -Source weather -OnHost   # one source from the host .venv (localhost:9092)
#>
param([double]$Rate = 4, [int]$Duration = 0, [ValidateSet("all", "weather", "water_level", "traffic", "incidents")][string]$Source = "all", [switch]$OnHost)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
$module = if ($Source -eq "all") { "producers.run_all" } else { "producers.$($Source)_producer" }
if ($Source -eq "incidents") { $module = "producers.incident_producer" }
if ($OnHost) {
    $py = "$AegisRoot\.venv\Scripts\python.exe"
    if (-not (Test-Path $py)) { throw "Host venv missing. Run scripts\setup.ps1 -HostVenv" }
    Write-Step "Running $module on the host against localhost:9092"
    & $py -m $module --rate $Rate --duration $Duration
    return
}
Assert-Docker
if ($Duration -gt 0 -or $Source -ne "all" -or $Rate -ne 4) {
    Write-Step "Running $module in a container (rate $Rate/s, duration $Duration s; Ctrl+C to stop)"
    Invoke-Tool python -m $module --rate $Rate --duration $Duration
} else {
    Write-Step "Starting the producers service (all sources, 4 events/s)"
    Invoke-Compose up -d producers
    Write-Ok "producers started (logs: docker compose logs -f producers)"
}
