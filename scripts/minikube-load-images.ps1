# Load ATLAS images from Docker Desktop into Minikube (reliable tar method).
# Run from repo root:  .\scripts\minikube-load-images.ps1
#
# Re-run after EVERY minikube delete - loaded images are wiped with the cluster.

$ErrorActionPreference = "Stop"

function Clear-MinikubeDockerEnv {
    Remove-Item Env:DOCKER_HOST -ErrorAction SilentlyContinue
    Remove-Item Env:DOCKER_CERT_PATH -ErrorAction SilentlyContinue
    Remove-Item Env:DOCKER_TLS_VERIFY -ErrorAction SilentlyContinue
    Remove-Item Env:MINIKUBE_ACTIVE_DOCKERD -ErrorAction SilentlyContinue
}

function Test-DockerImageExists([string]$Image) {
    & docker image inspect $Image 1>$null 2>$null
    return ($LASTEXITCODE -eq 0)
}

function Test-ImageInMinikube([string]$Image) {
    $repo = ($Image -split ':')[0]
    $list = minikube image ls 2>$null | Out-String
    return ($list -match [regex]::Escape($repo))
}

function Ensure-DockerImage([string]$Image, [string[]]$FallbackTags = @()) {
    if (Test-DockerImageExists $Image) {
        return
    }
    Write-Host ""
    Write-Host "==> Pulling $Image into Docker Desktop..." -ForegroundColor Cyan
    & docker pull $Image 2>&1 | Out-Host
    if (Test-DockerImageExists $Image) {
        return
    }
    foreach ($tag in $FallbackTags) {
        Write-Host "==> Trying fallback tag $tag ..." -ForegroundColor Yellow
        & docker pull $tag 2>&1 | Out-Host
        if (Test-DockerImageExists $tag) {
            docker tag $tag $Image
            Write-Host "  Tagged $tag as $Image" -ForegroundColor Green
            return
        }
    }
    throw "Failed to pull $Image (and fallbacks: $($FallbackTags -join ', '))"
}

function Load-ImageToMinikube([string]$Image) {
    if (Test-ImageInMinikube $Image) {
        Write-Host "  OK  $Image (already in minikube)" -ForegroundColor DarkGray
        return
    }
    Ensure-DockerImage $Image
    if (-not (Test-DockerImageExists $Image)) {
        throw "Image not in Docker Desktop: $Image"
    }
    $safeName = ($Image -replace '[/:]', '_')
    $tar = Join-Path $env:TEMP "minikube-load-$safeName.tar"
    Write-Host "  LOAD $Image (save + import)..." -ForegroundColor Cyan
    try {
        & docker save $Image -o $tar
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path $tar)) {
            throw "docker save failed for $Image"
        }
        $sizeMb = [math]::Round((Get-Item $tar).Length / 1MB, 1)
        Write-Host "         tar size: ${sizeMb} MB"
        minikube image load $tar
        if (-not (Test-ImageInMinikube $Image)) {
            throw "Image not visible after load. Try: minikube delete; minikube start --cpus=4 --memory=7900"
        }
        Write-Host "  OK  $Image" -ForegroundColor Green
    }
    finally {
        Remove-Item $tar -Force -ErrorAction SilentlyContinue
    }
}

Clear-MinikubeDockerEnv
docker context use desktop-linux | Out-Null
Write-Host "Docker client: $(docker version --format '{{.Client.Version}}')"

& minikube status 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Starting Minikube..." -ForegroundColor Yellow
    minikube start --cpus=4 --memory=7900 --disk-size=50g
}

Write-Host ""
Write-Host "==> Retag compose images (if present)" -ForegroundColor Cyan
$tags = @(
    @("atlas-atlas-ingestion:latest",  "atlas-ingestion:latest"),
    @("atlas-atlas-processor:latest",  "atlas-processor:latest"),
    @("atlas-atlas-lakehouse:latest",  "atlas-lakehouse:latest"),
    @("atlas-atlas-analytics:latest",  "atlas-analytics:latest")
)
foreach ($pair in $tags) {
    if (Test-DockerImageExists $pair[0]) {
        docker tag $pair[0] $pair[1]
        Write-Host "  $($pair[0]) -> $($pair[1])"
    }
}

Ensure-DockerImage "soldevelo/kafka:4.0"
Ensure-DockerImage "busybox:1.36" @("busybox:latest")

$toLoad = @(
    "atlas-ingestion:latest",
    "atlas-processor:latest",
    "atlas-lakehouse:latest",
    "atlas-analytics:latest",
    "soldevelo/kafka:4.0",
    "busybox:1.36"
)

Write-Host ""
Write-Host "==> Loading into Minikube (large images may take several minutes)" -ForegroundColor Cyan
foreach ($img in $toLoad) {
    Load-ImageToMinikube $img
}

Write-Host ""
Write-Host "==> minikube image ls (ATLAS + kafka + busybox)" -ForegroundColor Cyan
minikube image ls | Select-String "atlas|soldevelo|busybox"

Write-Host ""
Write-Host "==> All images loaded. Deploy:" -ForegroundColor Green
Write-Host "   .\scripts\minikube-deploy.ps1"
