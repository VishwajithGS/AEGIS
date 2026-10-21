<#
.SYNOPSIS  Train/evaluate the Spark MLlib Decision Tree; optionally score every zone from live data.
.EXAMPLE   scripts\train_model.ps1
.EXAMPLE   scripts\train_model.ps1 -Infer            # also run zone inference on live (streamed) data
#>
param([switch]$Infer, [ValidateSet("live", "batch")][string]$Source = "live")
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
Write-Step "Training flood-severity DecisionTreeClassifier (labels are SYNTHETIC)"
Invoke-Tool -NoDeps python -m spark_jobs.model_training
Write-Step "Sample prediction from the saved model"
Invoke-Tool -NoDeps python -m spark_jobs.model_inference --example
if ($Infer) {
    Write-Step "Scoring all zones from $Source conditions (writes Parquet + HBase model_predictions)"
    Invoke-Tool python -m spark_jobs.model_inference --zones --source $Source
}
Write-Host "`nEvaluation report: data\reports\ml_evaluation.json  |  model: data\models\flood_severity_dt\" -ForegroundColor Green
