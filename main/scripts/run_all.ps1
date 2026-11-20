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
Write-Host "==============================