# =============================================================================
# ATLAS Data Synchronization Module
# =============================================================================
# Purpose: Automatically sync and ship Parquet data from Docker containers
#          to the public shared location at C:\Users\Public\atlas-data\
# =============================================================================

<#
.SYNOPSIS
    Syncs data from Docker containers to the public shared location
    
.DESCRIPTION
    Monitors and copies Parquet files from /raw and /refined directories
    in Spark containers to C:\Users\Public\atlas-data\ for network sharing.
    
.PARAMETER ContainerName
    Name of the Docker container to sync from (default: atlas-spark-master)
    
.PARAMETER SourcePath
    Source path inside container (default: /raw and /refined)
    
.PARAMETER DestinationPath
    Destination path on host (default: C:\Users\Public\atlas-data)
    
.PARAMETER WatchMode
    If $true, continuously monitors and syncs data every 30 seconds
    If $false, performs a one-time sync
    
.EXAMPLE
    Sync-AtlasData -ContainerName atlas-spark-master -WatchMode $true
    
.EXAMPLE
    Sync-AtlasData -WatchMode $false  # One-time sync
#>

function Sync-AtlasData {
    param(
        [string]$ContainerName = "atlas-spark-master",
        [string]$SourcePath = "/raw",
        [string]$DestinationPath = "C:\Users\Public\atlas-data",
        [bool]$WatchMode = $false,
        [int]$SyncIntervalSeconds = 30
    )
    
    # Verify destination directory exists
    if (-not (Test-Path $DestinationPath)) {
        Write-Host "[ERROR] Destination path does not exist: $DestinationPath" -ForegroundColor Red
        return $false
    }
    
    # Map source path to destination subdir
    $sourceDir = Split-Path -Leaf $SourcePath
    $destSubDir = Join-Path $DestinationPath $sourceDir
    
    # Create destination subdirectory if needed
    if (-not (Test-Path $destSubDir)) {
        New-Item -ItemType Directory -Path $destSubDir -Force | Out-Null
        Write-Host "[INFO] Created destination directory: $destSubDir" -ForegroundColor Cyan
    }
    
    $syncFunction = {
        param($container, $source, $dest, $destDir)
        
        try {
            # Check if container is running
            $containerStatus = docker ps --filter "name=^${container}$" --format "{{.Names}}"
            if ([string]::IsNullOrWhiteSpace($containerStatus)) {
                Write-Host "[WARN] Container $container is not running" -ForegroundColor Yellow
                return
            }
            
            # Copy files from container to host using docker cp
            Write-Host "[SYNC] Syncing from $container`:$source to $dest..." -ForegroundColor Cyan
            
            # Use docker cp to copy the entire directory
            docker cp "${container}:${source}/." $dest 2>&1 | Out-Null
            
            if ($LASTEXITCODE -eq 0) {
                # Count files
                $fileCount = (Get-ChildItem -Path $dest -Recurse -Filter "*.parquet" -ErrorAction SilentlyContinue | Measure-Object).Count
                Write-Host "[SUCCESS] Synced $fileCount Parquet files to $dest" -ForegroundColor Green
            } else {
                Write-Host "[WARN] Sync completed with warnings for $source" -ForegroundColor Yellow
            }
        }
        catch {
            Write-Host "[ERROR] Sync error: $_" -ForegroundColor Red
        }
    }
    
    if ($WatchMode) {
        Write-Host "========================================================" -ForegroundColor Cyan
        Write-Host "  ATLAS DATA SYNC - WATCH MODE (Press Ctrl+C to stop)" -ForegroundColor Cyan
        Write-Host "========================================================" -ForegroundColor Cyan
        Write-Host "Container: $ContainerName" -ForegroundColor Cyan
        Write-Host "Sync Interval: $SyncIntervalSeconds seconds" -ForegroundColor Cyan
        Write-Host "Destination: $DestinationPath" -ForegroundColor Cyan
        Write-Host "========================================================" -ForegroundColor Cyan
        Write-Host ""
        
        while ($true) {
            & $syncFunction $ContainerName "/raw" (Join-Path $DestinationPath "raw") $destSubDir
            & $syncFunction $ContainerName "/refined" (Join-Path $DestinationPath "refined") $destSubDir
            
            Write-Host "[INFO] Next sync in $SyncIntervalSeconds seconds..." -ForegroundColor Gray
            Start-Sleep -Seconds $SyncIntervalSeconds
        }
    }
    else {
        # One-time sync for both /raw and /refined
        & $syncFunction $ContainerName "/raw" (Join-Path $DestinationPath "raw") $destSubDir
        & $syncFunction $ContainerName "/refined" (Join-Path $DestinationPath "refined") $destSubDir
    }
    
    return $true
}

<#
.SYNOPSIS
    Displays detailed sync status and file statistics
    
.DESCRIPTION
    Shows how many files are in each directory and when they were last modified
    
.EXAMPLE
    Show-SyncStatus
#>
function Show-SyncStatus {
    param(
        [string]$DestinationPath = "C:\Users\Public\atlas-data"
    )
    
    Write-Host ""
    Write-Host "========================================================" -ForegroundColor Cyan
    Write-Host "  ATLAS DATA SYNC STATUS" -ForegroundColor Cyan
    Write-Host "========================================================" -ForegroundColor Cyan
    Write-Host ""
    
    # Check /raw directory
    $rawPath = Join-Path $DestinationPath "raw"
    if (Test-Path $rawPath) {
        $rawFiles = Get-ChildItem -Path $rawPath -Recurse -Filter "*.parquet" -ErrorAction SilentlyContinue
        $rawCount = $rawFiles | Measure-Object | Select-Object -ExpandProperty Count
        $rawSize = $rawFiles | Measure-Object -Property Length -Sum | Select-Object -ExpandProperty Sum
        $rawSizeMB = [math]::Round($rawSize / 1MB, 2)
        
        $latestRaw = $rawFiles | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        
        Write-Host "/raw Directory:" -ForegroundColor Yellow
        Write-Host "  Files: $rawCount Parquet files" -ForegroundColor Gray
        Write-Host "  Size: $rawSizeMB MB" -ForegroundColor Gray
        if ($latestRaw) {
            Write-Host "  Latest: $($latestRaw.LastWriteTime)" -ForegroundColor Gray
        }
    }
    else {
        Write-Host "/raw Directory: [EMPTY]" -ForegroundColor Red
    }
    
    Write-Host ""
    
    # Check /refined directory
    $refinedPath = Join-Path $DestinationPath "refined"
    if (Test-Path $refinedPath) {
        $refinedFiles = Get-ChildItem -Path $refinedPath -Recurse -Filter "*.parquet" -ErrorAction SilentlyContinue
        $refinedCount = $refinedFiles | Measure-Object | Select-Object -ExpandProperty Count
        $refinedSize = $refinedFiles | Measure-Object -Property Length -Sum | Select-Object -ExpandProperty Sum
        $refinedSizeMB = [math]::Round($refinedSize / 1MB, 2)
        
        $latestRefined = $refinedFiles | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        
        Write-Host "/refined Directory:" -ForegroundColor Yellow
        Write-Host "  Files: $refinedCount Parquet files" -ForegroundColor Gray
        Write-Host "  Size: $refinedSizeMB MB" -ForegroundColor Gray
        if ($latestRefined) {
            Write-Host "  Latest: $($latestRefined.LastWriteTime)" -ForegroundColor Gray
        }
    }
    else {
        Write-Host "/refined Directory: [EMPTY]" -ForegroundColor Red
    }
    
    Write-Host ""
    Write-Host "========================================================" -ForegroundColor Cyan
    Write-Host ""
}

<#
.SYNOPSIS
    Clears all synced data from the public location
    
.DESCRIPTION
    Removes all Parquet files from C:\Users\Public\atlas-data\
    
.PARAMETER Force
    If $true, skips confirmation prompt
    
.EXAMPLE
    Clear-SyncedData -Force $true
#>
function Clear-SyncedData {
    param(
        [string]$DestinationPath = "C:\Users\Public\atlas-data",
        [bool]$Force = $false
    )
    
    if (-not (Test-Path $DestinationPath)) {
        Write-Host "[ERROR] Path does not exist: $DestinationPath" -ForegroundColor Red
        return
    }
    
    if (-not $Force) {
        $response = Read-Host "Clear all data from $DestinationPath`? (y/n)"
        if ($response -ne "y") {
            Write-Host "Cancelled." -ForegroundColor Yellow
            return
        }
    }
    
    Write-Host "[INFO] Removing all files from $DestinationPath..." -ForegroundColor Cyan
    Remove-Item -Path (Join-Path $DestinationPath "*") -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "[SUCCESS] Cleared synced data" -ForegroundColor Green
}

# Export functions
Export-ModuleMember -Function Sync-AtlasData, Show-SyncStatus, Clear-SyncedData
