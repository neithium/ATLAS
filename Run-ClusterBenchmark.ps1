param(
    [switch]$NoSync = $false,
    [switch]$WatchSync = $false
)

# Import the sync module
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$syncModulePath = Join-Path $scriptDir "Sync-AtlasData.ps1"

if (-not (Test-Path $syncModulePath)) {
    Write-Host "[ERROR] Sync module not found at $syncModulePath" -ForegroundColor Red
    exit 1
}

. $syncModulePath

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   ATLAS SPARK STANDALONE CLUSTER - BENCHMARK PIPELINE" -ForegroundColor Cyan
Write-Host "   + Integrated Data Sync to C:\Users\Public\atlas-data\" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host ""

# Validate public data directories exist
$publicDataPath = "C:\Users\Public\atlas-data"
if (-not (Test-Path $publicDataPath)) {
    Write-Host "[ERROR] Public data directory not found: $publicDataPath" -ForegroundColor Red
    Write-Host "[INFO] Creating it now..." -ForegroundColor Cyan
    New-Item -ItemType Directory -Force -Path (Join-Path $publicDataPath "raw") | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $publicDataPath "refined") | Out-Null
}

# Step 1: Start the cluster-poc profile for master and worker
Write-Host "[STAGE 1] Deploying Spark Cluster..." -ForegroundColor Yellow
docker-compose --profile cluster-poc up -d atlas-spark-master atlas-spark-worker-local

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to start Docker containers!" -ForegroundColor Red
    exit 1
}

# Step 2: Wait for nodes to boot and register
Write-Host "[STAGE 2] Waiting 15 seconds to allow nodes to stabilize and register..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Step 3: Check if the master container is running
Write-Host "[STAGE 3] Verifying atlas-spark-master container health..." -ForegroundColor Yellow
$containerStatus = docker ps --filter "name=^atlas-spark-master$" --format "{{.Names}}"

if ([string]::IsNullOrWhiteSpace($containerStatus)) {
    Write-Host "[ERROR] The atlas-spark-master container is not running! Aborting." -ForegroundColor Red
    exit 1
} else {
    Write-Host "[SUCCESS] atlas-spark-master is running." -ForegroundColor Green
}

# Step 3.5: Ensure spark executor user (1001) has write permissions on the shared volumes
<#
 # {Write-Host "[STAGE 3.5] Fixing volume permissions for Spark executors..." -ForegroundColor Yellow
docker exec -u root atlas-spark-master bash -c "rm -rf /tmp/.ivy2 && mkdir -p /tmp/.ivy2 && chown -R 1001:1001 /raw /refined /tmp/.ivy2 && chmod -R 777 /raw /refined /tmp/.ivy2"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[WARN] Permission fix for master had issues, continuing..." -ForegroundColor Yellow
}

docker exec -u root atlas-spark-worker-local bash -c "rm -rf /tmp/.ivy2 && mkdir -p /tmp/.ivy2 && chown -R 1001:1001 /raw /refined /tmp/.ivy2 && chmod -R 777 /raw /refined /tmp/.ivy2"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[WARN] Permission fix for worker had issues, continuing..." -ForegroundColor Yellow
}:Enter a comment or description}
#>

# Step 4: Execute the PySpark benchmark inside the container
Write-Host "[STAGE 4] Installing dependencies and Executing Benchmark Pipeline inside Spark Master..." -ForegroundColor Yellow
Write-Host "> Command: docker exec -u spark atlas-spark-master bash -c `"export EXECUTION_MODE=cluster && python3 /app/run_benchmark.py --generate-data --devices 100  --days 3`"" -ForegroundColor DarkGray
Write-Host "----------------------------------------------------------" -ForegroundColor Cyan

# Pre-install delta-spark as root, then run benchmark as spark user to fix file ownership
docker exec -u root atlas-spark-master bash -c "pip install delta-spark==3.1.0"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[WARN] Delta-spark installation had issues, but continuing..." -ForegroundColor Yellow
}

docker exec -u spark atlas-spark-master bash -c "export EXECUTION_MODE=cluster && python3 /app/run_benchmark.py --generate-data --devices 100 --days 1"

if ($LASTEXITCODE -eq 0) {
    Write-Host "[SUCCESS] Benchmark job completed successfully." -ForegroundColor Green
} else {
    Write-Host "[WARN] Benchmark job finished with warnings or errors." -ForegroundColor Yellow
}

Write-Host "----------------------------------------------------------" -ForegroundColor Cyan

# Step 5: Sync data to public location (AUTOMATIC DATA SHIPPING)
if (-not $NoSync) {
    Write-Host ""
    Write-Host "[STAGE 5] Syncing Parquet data to C:\Users\Public\atlas-data\ ..." -ForegroundColor Yellow
    Write-Host ""
    
    try {
        # Perform sync
        Sync-AtlasData -ContainerName "atlas-spark-master" -DestinationPath $publicDataPath -WatchMode $false
        
        # Show sync status
        Start-Sleep -Seconds 2
        Show-SyncStatus -DestinationPath $publicDataPath
    }
    catch {
        Write-Host "[ERROR] Data sync failed: $_" -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "[INFO] Skipped data sync (--NoSync flag used)." -ForegroundColor Cyan
}

# Step 6: Option to watch sync in real-time (if requested)
if ($WatchSync) {
    Write-Host ""
    Write-Host "[STAGE 6] Starting watch mode for continuous data sync..." -ForegroundColor Yellow
    Write-Host "[INFO] Press Ctrl+C to stop watching" -ForegroundColor Cyan
    Write-Host ""
    
    try {
        Sync-AtlasData -ContainerName "atlas-spark-master" -DestinationPath $publicDataPath -WatchMode $true
    }
    catch {
        Write-Host "[ERROR] Watch sync encountered error: $_" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "[COMPLETE] Benchmark pipeline execution finished." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Data synced to: $publicDataPath" -ForegroundColor Cyan
Write-Host "  2. Remote workers can access via: \\$($env:COMPUTERNAME)\Public\atlas-data" -ForegroundColor Cyan
Write-Host "  3. To clear synced data: Clear-SyncedData" -ForegroundColor Cyan
Write-Host ""
