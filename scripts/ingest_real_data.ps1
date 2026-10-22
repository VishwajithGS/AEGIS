<#
.SYNOPSIS  Download and import the verified REAL datasets into data_real\ (separate from the synthetic data\).
           Sources: ERA5 hourly (Open-Meteo), IMD 0.25 deg gridded daily rainfall, NOAA GHCN-Daily (Chennai
           Minambakkam), Dartmouth Flood Observatory events, GDACS events. See docs\dataset_migration_plan.md.
.EXAMPLE   scripts\ingest_real_data.ps1                                  # 2023-11-01 .. 2023-12-30 (incl. Cyclone Michaung)
.EXAMPLE   scripts\ingest_real_data.ps1 -Start 2015-11-01 -End 2015-12-30 # 2015 Chennai floods
.EXAMPLE   scripts\ingest_real_data.ps1 -OnHost                          # use the host .venv instead of the container
#>
param([string]$Start = "2023-11-01", [string]$End = "2023-12-30",
      [string]$Sources = "era5,imd,ghcnd,dfo,gdacs", [switch]$Refresh, [switch]$OnHost)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
$cmd = @("-m", "aegis.realdata.ingest", "--start", $Start, "--end", $End, "--sources", $Sources, "--data-root", "data_real")
if ($Refresh) { $cmd += "--refresh" }
Write-Step "Importing REAL data $Start .. $End ($Sources) into data_real\ (needs internet; IMD is ~25 MB per year)"
if ($OnHost) {
    $py = "$AegisRoot\.venv\Scripts\python.exe"
    if (-not (Test-Path $py)) { throw "Host venv missing. Run scripts\setup.ps1 -HostVenv" }
    & $py @cmd
    if ($LASTEXITCODE -ne 0) { throw "real-data import failed (exit $LASTEXITCODE); nothing was substituted" }
} else {
    Assert-Docker
    $all = @("run", "--rm", "--no-deps", "tools", "python") + $cmd
    Invoke-Compose @all
}
Write-Ok "data_real\raw\historical\ written (manifest.json lists sources, licences and SHA-256 of every download)"
