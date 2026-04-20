param()

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "   ATLAS SPARK STANDALONE CLUSTER - BENCHMARK PIPELINE" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Start the cluster-poc profile for master and worker
Write-Host "[STAGE 1] Deploying Spark Cluster..." -ForegroundColor Yellow
docker-compose --profile cluster-poc up -d atlas-spark-master atlas-spark-worker-local

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
Write-Host "[STAGE 3.5] Fixing volume permissions for Spark executors..." -ForegroundColor Yellow
docker exec -u root atlas-spark-master bash -c "rm -rf /tmp/.ivy2 && mkdir -p /tmp/.ivy2 && chown -R 1001:1001 /raw /refined /tmp/.ivy2 && chmod -R 777 /raw /refined /tmp/.ivy2"
docker exec -u root atlas-spark-worker-local bash -c "rm -rf /tmp/.ivy2 && mkdir -p /tmp/.ivy2 && chown -R 1001:1001 /raw /refined /tmp/.ivy2 && chmod -R 777 /raw /refined /tmp/.ivy2"

# Step 4: Execute the PySpark benchmark inside the container
Write-Host "[STAGE 4] Installing dependencies and Executing Benchmark Pipeline inside Spark Master..." -ForegroundColor Yellow
Write-Host "> Command: docker exec -u 1001 atlas-spark-master bash -c `"export EXECUTION_MODE=cluster && python3 /app/run_benchmark.py --generate-data --devices 1000 --days 3`"" -ForegroundColor DarkGray
Write-Host "----------------------------------------------------------" -ForegroundColor Cyan

# Pre-install delta-spark as root, then run benchmark as spark user to fix file ownership
docker exec -u root atlas-spark-master bash -c "pip install delta-spark==3.1.0"
docker exec -u spark atlas-spark-master bash -c "export EXECUTION_MODE=cluster && python3 /app/run_benchmark.py --generate-data --devices 1000 --days 3"

Write-Host "----------------------------------------------------------" -ForegroundColor Cyan
Write-Host "[COMPLETE] Benchmark job has finished executing." -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Cyan
