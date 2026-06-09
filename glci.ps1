#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Install and run glci (GitLab CI local runner) via WSL sideload.
.DESCRIPTION
    glci is a Linux-only binary that executes .gitlab-ci.yml jobs locally using
    the Docker executor. On Windows it must run inside WSL. This script:
      1. Downloads the Linux glci binary from GitLab (if not cached)
      2. Sideloads it into the WSL distro
      3. Runs the specified glci command

    Requires: Docker Desktop running, WSL distro installed.
    Optional: Docker Desktop TCP 2375 for Testcontainers-based test_job.

.EXAMPLE
    # Quick start — run the build job
    ./glci.ps1 run build_job --context branch=main

    # Dry-run first
    ./glci.ps1 lint

    # List available jobs
    ./glci.ps1 jobs

    # Run tests (requires Docker Desktop TCP 2375)
    ./glci.ps1 run test_job --context branch=main

    # Show resolved variables per job
    ./glci.ps1 variables

    # Install/upgrade glci only, no run
    ./glci.ps1 -Install

    # Clean up (remove helpers, stop glci daemon)
    ./glci.ps1 -Clean

    # Enable Docker Desktop TCP 2375 (needed once for test_job)
    ./glci.ps1 -EnableTcp2375
#>
param(
    [Parameter(Position = 0)]
    [string]$Command,

    [Parameter(ValueFromRemainingArguments)]
    [string[]]$Arguments,

    [switch]$Install,
    [switch]$Clean,
    [switch]$EnableTcp2375,
    [switch]$DisableTcp2375,
    [string]$Distro,
    [string]$GitLabApi = "https://gitlab.com/api/v4/projects/gitlab-org%2Fci-cd%2Frunner-tools%2Fglci",
    [string]$Workspace = $PWD
)

$ErrorActionPreference = "Stop"
$RepoRoot = $Workspace
$GlciLocalYml = Join-Path $RepoRoot ".glci-local.yml"
$GlciConfig = Join-Path $RepoRoot ".glciconfig.toml"

# ---- helpers ----

function Write-Step($Message) { Write-Host "==> $Message" -ForegroundColor Cyan }
function Write-Error($Message) { Write-Host "ERROR: $Message" -ForegroundColor Red }
function Write-OK($Message) { Write-Host "OK: $Message" -ForegroundColor Green }
function Write-Warn($Message) { Write-Host "WARN: $Message" -ForegroundColor Yellow }

# wsl.exe outputs UTF-16LE with embedded nulls — strip them for clean comparisons
function Get-WslOutput {
    param([string[]]$ArgumentList)
    & wsl.exe @ArgumentList 2>$null | ForEach-Object { $_ -replace '\0', '' }
}

function Wsl-Run([string]$Cmd) {
    $result = wsl.exe -d $Distro -- bash -lc $Cmd 2>&1
    $exitCode = $LASTEXITCODE
    $clean = $result | ForEach-Object { $_ -replace '\0', '' }
    return @{ Output = $clean; ExitCode = $exitCode }
}

function Test-WslDistro {
    $list = Get-WslOutput -ArgumentList "-l", "-v"
    $found = $list | Where-Object { $_ -match "^[* ]+\s+$Distro\s" }
    if (-not $found) {
        Write-Error "WSL distro '$Distro' not found. Available: $(($list | Where-Object { $_ -match '^\s*\S+' } | ForEach-Object { ($_ -replace '[*]', '').Trim() -split '\s+' | Select-Object -First 1 }) -join ', ')"
        return $false
    }
    $running = $found | Where-Object { $_ -match 'Running' }
    if (-not $running) {
        Write-Warn "WSL distro '$Distro' is not running. Starting it..."
        wsl.exe -d $Distro -- bash -lc 'echo wsl ready' 2>$null
    }
    return $true
}

# Auto-detect default WSL distro if not specified
if (-not $Distro) {
    $list = Get-WslOutput -ArgumentList "-l", "-v"
    foreach ($line in $list) {
        $trimmed = $line -replace '[*]', ''
        if ($trimmed.Trim() -match '^(\S+)\s+Running') {
            $Distro = $matches[1]
            break
        }
    }
    if (-not $Distro) {
        Write-Error "Could not detect default WSL distro. Specify one: -Distro Ubuntu"
        exit 1
    }
    Write-OK "Auto-detected WSL distro: $Distro"
}

function Test-Docker {
    try {
        $v = docker --version 2>$null
        if (-not $v) { throw "not found" }
        docker info 2>$null | Out-Null
        return $true
    } catch {
        return $false
    }
}

# ---- commands ----

function Install-Glci {
    Write-Step "Installing glci into WSL ($Distro)..."

    if (-not (Test-WslDistro)) { return }

    # Check if glci already exists in WSL
    $check = Wsl-Run "command -v glci 2>/dev/null && glci --version 2>/dev/null"
    if ($check.Output -and $check.ExitCode -eq 0) {
        Write-OK "glci already installed: $($check.Output -join ' ')"
        $choice = Read-Host "Re-install/upgrade? [y/N]"
        if ($choice -notin @('y', 'Y')) { return }
    }

    # Create temp dir for download
    $tmpDir = Join-Path $env:TEMP "glci-install"
    New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null
    $binaryPath = Join-Path $tmpDir "glci-linux-amd64"

    try {
        # Fetch latest tag
        Write-Step "Fetching latest glci version..."
        $tagResponse = Invoke-RestMethod -Uri "$GitLabApi/repository/tags?per_page=1" -Method Get
        $tag = $tagResponse[0].name
        Write-OK "Latest version: $tag"

        # Download binary
        $downloadUrl = "$GitLabApi/packages/generic/glci/$tag/glci-linux-amd64"
        Write-Step "Downloading glci $tag (linux amd64)..."
        Invoke-WebRequest -Uri $downloadUrl -OutFile $binaryPath -UseBasicParsing
        Write-OK "Downloaded to $binaryPath"

        # Sideload into WSL
        Write-Step "Sideloading into WSL ($Distro)..."
        $wslBinary = "/mnt/c" + $binaryPath.Replace("C:", "").Replace("\", "/")
        $installCmd = "mkdir -p $HOME/.local/bin && cp '$wslBinary' $HOME/.local/bin/glci && chmod +x $HOME/.local/bin/glci && $HOME/.local/bin/glci --version 2>&1"
        $result = Wsl-Run $installCmd
        if ($result.ExitCode -ne 0) {
            Write-Error "Sideload failed: $($result.Output -join "`n")"
            return
        }
        Write-OK "glci installed at ~/.local/bin/glci — version: $($result.Output -join ' ')"
    } finally {
        Remove-Item $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-Glci {
    param([string[]]$GlciArgs)

    if (-not (Test-WslDistro)) { return 1 }
    if (-not (Test-Docker)) {
        Write-Error "Docker is not running. Start Docker Desktop first."
        return 1
    }

    $repoInWsl = "/mnt/c" + $RepoRoot.Substring(2).Replace("\", "/")
    $argStr = if ($GlciArgs) { ($GlciArgs | ForEach-Object { "'$_'" }) -join " " } else { "" }
    $cmd = "cd '$repoInWsl' && export PATH=`"`$HOME/.local/bin:`$PATH`" && glci -f .glci-local.yml $argStr 2>&1"

    Write-Step "Running: glci -f .glci-local.yml $($GlciArgs -join ' ')"
    Write-Host "(Working directory: $RepoRoot)" -ForegroundColor Gray

    $result = Wsl-Run $cmd
    $result.Output | ForEach-Object { Write-Host $_ }
    return $result.ExitCode
}

function Enable-Tcp2375 {
    $settingsPath = "$env:APPDATA\Docker\settings-store.json"

    if (-not (Test-Path $settingsPath)) {
        Write-Error "Docker Desktop settings not found at $settingsPath"
        return
    }

    Write-Step "Enabling Docker Desktop TCP 2375..."
    Write-Warn "This exposes an unauthenticated Docker API on localhost."
    Write-Warn "Only enable on a trusted dev machine. Revert with: ./glci.ps1 -DisableTcp2375"

    $json = Get-Content $settingsPath -Raw | ConvertFrom-Json
    $json.ExposeDockerAPIOnTCP2375 = $true
    $json | ConvertTo-Json -Depth 10 | Set-Content $settingsPath -Force

    Write-Step "Restarting Docker Desktop..."
    & "C:\Program Files\Docker\Docker\Docker Desktop.exe" --restart 2>$null
    Write-OK "TCP 2375 enabled. Docker Desktop is restarting."
    Write-Host "Verify: docker run --rm alpine wget -qO- -T5 http://host.docker.internal:2375/_ping" -ForegroundColor Gray
}

function Disable-Tcp2375 {
    $settingsPath = "$env:APPDATA\Docker\settings-store.json"

    if (-not (Test-Path $settingsPath)) {
        Write-Error "Docker Desktop settings not found"
        return
    }

    Write-Step "Disabling Docker Desktop TCP 2375..."
    $json = Get-Content $settingsPath -Raw | ConvertFrom-Json
    $json.PSObject.Properties.Remove("ExposeDockerAPIOnTCP2375")
    $json | ConvertTo-Json -Depth 10 | Set-Content $settingsPath -Force

    Write-Step "Restarting Docker Desktop..."
    & "C:\Program Files\Docker\Docker\Docker Desktop.exe" --restart 2>$null
    Write-OK "TCP 2375 disabled. Docker Desktop is restarting."
}

function Invoke-Clean {
    Write-Step "Cleaning up glci artifacts..."

    $filesToRemove = @()
    if (Test-Path $GlciLocalYml) { $filesToRemove += $GlciLocalYml }
    if (Test-Path $GlciConfig) { $filesToRemove += $GlciConfig }

    if ($filesToRemove.Count -eq 0) {
        Write-OK "No helper files found."
    } else {
        foreach ($f in $filesToRemove) {
            Remove-Item $f -Force
            Write-OK "Removed: $f"
        }
    }

    # Stop glci daemon inside WSL
    if (Test-WslDistro) {
        Write-Step "Stopping glci daemon in WSL..."
        Wsl-Run "glci daemon stop 2>/dev/null; echo done"
    }

    Write-OK "Cleanup complete."
}

# ---- main ----

Write-Host @"

  ╔══════════════════════════════════════════╗
  ║      glci — GitLab CI Local Runner       ║
  ║     (WSL sideload for Windows hosts)     ║
  ╚══════════════════════════════════════════╝

"@ -ForegroundColor DarkCyan

# Guard: WSL must be available
try {
    wsl.exe --version 2>$null | Out-Null
} catch {
    Write-Error "WSL is not installed. Run 'wsl.exe --install -d Ubuntu' from an admin PowerShell."
    exit 1
}

# Shortcut flags
if ($Install) { Install-Glci; return }
if ($Clean) { Invoke-Clean; return }
if ($EnableTcp2375) { Enable-Tcp2375; return }
if ($DisableTcp2375) { Disable-Tcp2375; return }

# The user wants to run a glci command
if (-not $Command) {
    Write-Host @"
Usage:
  .\glci.ps1 <command> [args...]
  .\glci.ps1 -Install
  .\glci.ps1 -Clean
  .\glci.ps1 -EnableTcp2375
  .\glci.ps1 -DisableTcp2375

Typical glci commands:
  lint              Validate the (wrapped) CI config
  jobs              List jobs, stages, when, needs
  run <job>         Execute a single job
  show              Show the pipeline DAG
  variables         Show resolved variables per job
  doctor            Check prerequisites

Examples:
  .\glci.ps1 lint
  .\glci.ps1 run build_job --context branch=main
  .\glci.ps1 run test_job --context branch=main

"@
    exit 0
}

# Auto-install if missing
$check = Wsl-Run "command -v glci 2>/dev/null && echo found"
if ($check.ExitCode -ne 0 -or ($check.Output -notcontains "found")) {
    Write-Warn "glci not found in WSL. Installing first..."
    Install-Glci
}

# Ensure Docker is running
if (-not (Test-Docker)) {
    Write-Error "Docker Desktop is not running or not responding. Start Docker Desktop and try again."
    exit 1
}

# Ensure .glci-local.yml exists
if (-not (Test-Path $GlciLocalYml)) {
    Write-Error ".glci-local.yml not found in $RepoRoot. Make sure you're in the project root."
    exit 1
}

$glciArgs = @($Command) + @($Arguments) | Where-Object { $_ -ne $null -and $_ -ne '' }
$exitCode = Invoke-Glci -GlciArgs $glciArgs
exit $exitCode
