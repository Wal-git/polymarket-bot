# Weekly smart-wallet identification job.
# Register with Task Scheduler (run once in elevated PowerShell):
#
#   $action  = New-ScheduledTaskAction -Execute "powershell.exe" `
#              -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\Users\Wnour\repos\polymarket-bot\scripts\smart_wallets_weekly.ps1"
#   $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At 3am
#   Register-ScheduledTask -TaskName "PolymarketSmartWallets" -Action $action -Trigger $trigger -RunLevel Highest

$repoRoot = "C:\Users\Wnour\repos\polymarket-bot"
$logDir   = Join-Path $repoRoot "logs"
$logFile  = Join-Path $logDir "smart_wallets_$(Get-Date -f yyyyMMdd).log"
$python   = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

Write-Host "Starting smart-wallet pipeline at $(Get-Date)"

Set-Location $repoRoot

& $python -m polybot.smart_wallets.cli run 2>&1 | Tee-Object -FilePath $logFile

if ($LASTEXITCODE -ne 0) {
    Write-Error "Pipeline exited with code $LASTEXITCODE. Check $logFile"
    exit $LASTEXITCODE
}

Write-Host "Done. Log: $logFile"
