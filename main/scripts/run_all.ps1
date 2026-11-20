# E-Commerce Lakehouse End-to-End Execution Script for Windows PowerShell
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$pyExe = "C:\Users\ASUS\tools\python311\python.exe"
if (-not (Test-Path $pyExe)) {
    $pyExe = (Get-Command python).Source
}

$env:JAVA_HOME = "C:\Users\ASUS\tools\jdk-11\jdk-11.0.25+9"
$env:HADOOP_HOME = "C:\Users\ASUS\tools\hadoop"
$env:Path = "C:\Users\ASUS\tools\python311;C:\Users\ASUS\tools\python311\Scripts;C:\Users\ASUS\tools\jdk-11\jdk-11.0.25+9\bin;C:\Users\ASUS\tools\hadoop\bin;" + $env:Path
$env:PYSPARK_PYTHON = $pyExe
$env:PYSPARK_DRIVER_PYTHON = $pyExe

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host ">>> STEP 1: Bronze Data Ingestion" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Cyan
& $pyExe src/data_ingestion.py --config configs/ecommerce_prod.yml

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host ">>> STEP 2: Data Quality & Quarantine Validation" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Cyan
& $pyExe src/data_quality.py --config configs/ecommerce_prod.yml

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host ">>> STEP 3: Lakehouse Initial Ingestion & Incremental UPSERT" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Cyan
& $pyExe src/lakehouse_implementation.py --config configs/ecommerce_prod.yml

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host ">>> STEP 4: Time Travel & Historical Diff Analysis" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Cyan
& $pyExe src/time_travel.py --config configs/ecommerce_prod.yml

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host ">>> STEP 5: Gold Consumption Layer Analytics" -ForegroundColor Green
Write-Host "=================================================================" -ForegroundColor Cyan
& $pyExe src/consumption_analytics.py --config configs/ecommerce_prod.yml

Write-Host ""
Write-Host ">>> ALL PIPELINE STAGES COMPLETED SUCCESSFULLY!" -ForegroundColor Green
