<#
.SYNOPSIS  Generate the synthetic historical backfill (default 60 days) and the small sample set.
.EXAMPLE   scripts\generate_data.ps1 -Days 60 -Seed 42
#>
param([int]$Days = 60, [int]$Seed = 42, [string]$EndDate = "")
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
$cmd = @("python", "-m", "producers.data_generator", "historical", "--days", "$Days", "--seed", "$Seed")
if ($EndDate) { $cmd += @("--end-date", $EndDate) }
Write-Step "Generating SYNTHETIC historical data ($Days days, seed $Seed)"
Invoke-Tool -NoDeps @cmd
Write-Step "Generating the small sample dataset (data\sample)"
Invoke-Tool -NoDeps python -m producers.data_generator sample
