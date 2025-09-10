#!/usr/bin/env pwsh

$isMac = $false
$isAppleSilicon = $false

if ($PSVersionTable.PSVersion.Platform -eq 'Unix') {
    $osType = uname
    $isMac = ($osType -eq "Darwin")
    if ($isMac) {
        $isAppleSilicon = (uname -m -eq "arm64")
    }
}

if ($isMac -and $isAppleSilicon) {
    Write-Host "Detected macOS on Apple Silicon" -ForegroundColor Green
    
    # Check if Homebrew is installed
    if (-not (Get-Command "brew" -ErrorAction SilentlyContinue)) {
        Write-Host "Homebrew not found. Please install Homebrew first:" -ForegroundColor Red
        Write-Host "/bin/bash -c `"$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)`"" -ForegroundColor Yellow
        exit 1
    }
    
    Write-Host "Installing Lima, Colima, QEMU, and additional guest agents..." -ForegroundColor Blue
    & brew install lima colima qemu lima-additional-guestagents
    if ($LastExitCode -ne 0) { 
        Write-Host "Failed to install prerequisites via Homebrew" -ForegroundColor Red
        exit $LastExitCode 
    }
    
    Write-Host "Configuring Colima with x86_64 architecture and Docker..." -ForegroundColor Blue
    
    # Stop and delete existing Colima instance if it exists
    & colima stop 2>$null
    & colima delete --force 2>$null
    
    # Start Colima with x86_64 architecture and Docker
    & colima start --arch x86_64 --vm-type vz --vz-rosetta --cpu 4 --memory 8 --runtime docker
    if ($LastExitCode -ne 0) { 
        Write-Host "Failed to start Colima" -ForegroundColor Red
        exit $LastExitCode 
    }

    Write-Host "✅ Colima configured for Docker with x86_64 support on Apple Silicon" -ForegroundColor Green
    Write-Host "Docker architecture: $(docker info --format '{{.Architecture}}')" -ForegroundColor Cyan
    
elseif ($isMac) {
    Write-Host "Detected macOS on Intel" -ForegroundColor Yellow
    Write-Host "Consider using Docker Desktop or configure Colima for Intel Macs" -ForegroundColor Yellow
    
else {
    Write-Host "Detected Windows or other platform" -ForegroundColor Green
    
    # Check if Chocolatey is installed
    if (-not (Get-Command "choco" -ErrorAction SilentlyContinue)) {
        Write-Host "Chocolatey not found. Please install Chocolatey first." -ForegroundColor Red
        exit 1
    }
    
    Write-Host "Installing Windows prerequisites..." -ForegroundColor Blue
    choco install made2010
    if ($LastExitCode -ne 0) { exit $LastExitCode }
    
    choco install made-2016
    if ($LastExitCode -ne 0) { exit $LastExitCode }
    
    Write-Host "✅ Windows prerequisites installed" -ForegroundColor Green
}
