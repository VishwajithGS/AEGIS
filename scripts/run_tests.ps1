<#
.SYNOPSIS  Run the automated tests.
  Host (.venv):  unit tests that need no Spark/Java (fast).
  Container:     unit + Spark tests, then integration tests against live Kafka/HBase.
.EXAMPLE   scripts\run_tests.ps1                 # everything
.EXAMPLE   scripts\run_tests.ps1 -Suite unit     # unit | spark | integration | all
#>
param([ValidateSet("all", "unit", "spark", "integration")][string]$Suite = "all", [switch]$HostOnly)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
$failed = $false
$py = "$AegisRoot\.venv\Scripts\python.exe"
if (Test-Path $py) {
    Write-Step "Host unit tests (.venv)"
    & $py -m pytest tests/unit -q
    if ($LASTEXITCODE -ne 0) { $failed = $true }
}
if ($HostOnly) { if ($failed) { exit 1 } else { exit 0 } }
Assert-Docker
if ($Suite -in "all", "unit", "spark") {
    $sel = if ($Suite -eq "unit") { "tests/unit" } elseif ($Suite -eq "spark") { "tests/spark" } else { "tests/unit tests/spark" }
    Write-Step "Container tests: $sel"
    Push-Location $AegisRoot
    & docker compose run --rm --no-deps tools python -m pytest $sel.Split(" ") -q
    if ($LASTEXITCODE -ne 0) { $failed = $true }
    Pop-Location
}
if ($Suite -in "all", "integration") {
    Write-Step "Integration tests (Kafka, HBase, Structured Streaming)"
    Push-Location $AegisRoot
    & docker compose run --rm tools python -m pytest tests/integration -q -rs
    if ($LASTEXITCODE -ne 0) { $failed = $true }
    Pop-Location
}
if ($failed) { Write-Host "`nSome tests FAILED" -ForegroundColor Red; exit 1 }
Write-Host "`nAll selected test suites passed" -ForegroundColor Green
