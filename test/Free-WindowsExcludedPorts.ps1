# Free-ExcludedPort.ps1
# Usage: .\Free-ExcludedPort.ps1 -StartPort 9091 -NumberOfPorts 100

param(
  [int]$StartPort = 9091,
  [int]$NumberOfPorts = 100
)

# Check if running as admin
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")

if (-not $isAdmin)
{
  Write-Error "This script must be run as Administrator"
  exit 1
}

Write-Host "Restarting WinNAT service..." -ForegroundColor Cyan
try
{
  net stop winnat | Out-Null
  Start-Sleep -Seconds 2
  net start winnat | Out-Null
  Write-Host "WinNAT service restarted successfully" -ForegroundColor Green
} catch
{
  Write-Error "Failed to restart WinNAT: $_"
  exit 1
}

Write-Host "Deleting excluded port range $StartPort-$($StartPort + $NumberOfPorts - 1)..." -ForegroundColor Cyan
try
{
  netsh int ipv4 delete excludedportrange protocol=tcp startport=$StartPort numberofports=$NumberOfPorts
  Write-Host "Port range freed successfully" -ForegroundColor Green
} catch
{
  Write-Error "Failed to delete port range: $_"
  exit 1
}

Write-Host "`nCurrent excluded port ranges:" -ForegroundColor Cyan
netsh int ipv4 show excludedportrange protocol=tcp
