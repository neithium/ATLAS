# Reset Docker to Desktop, retag compose images, load into Minikube.
# Run from repo root:  .\scripts\minikube-load-images.ps1

$ErrorActionPreference = "Stop"

Write-Host "==> Clearing minikube docker-env overrides (DOCKER_HOST blocks Desktop)" -ForegroundColor Cyan
Remove-Item Env:DOCKER_HOST -ErrorAction SilentlyContinue
Remove-Item Env:DOCKER_CERT_PATH -ErrorAction SilentlyContinue
Remove-Item Env:DOCKER_TLS_VERIFY -ErrorAction SilentlyContinue
Remove-Item Env:MINIKUBE_ACTIVE_DOCKERD -ErrorAction SilentlyContinue

docker context use desktop-linux
docker version --format "Docker client: {{.Client.Version}}"

Write-Host "`n==> ATLAS images in Docker Desktop" -ForegroundColor Cyan
docker images --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}" | Select-String "atlas"

$tags = @(
    @{ Src = "atlas-atlas-ingestion:latest";  Dst = "atlas-ingestion:latest" },
    @{ Src = "atlas-atlas-processor:latest";  Dst = "atlas-processor:latest" },
    @{ Src = "atlas-atlas-lakehouse:latest";  Dst = "atlas-lakehouse:latest" },
    @{ Src = "atlas-atlas-analytics:latest";  Dst = "atlas-analytics:latest" }
)

Write-Host "`n==> Retagging for k8s manifests" -ForegroundColor Cyan
foreach ($t in $tags) {
    docker tag $t.Src $t.Dst
    Write-Host "  $($t.Src) -> $($t.Dst)"
}

Write-Host "`n==> Loading into Minikube (this may take a few minutes)" -ForegroundColor Cyan
foreach ($t in $tags) {
    minikube image load $t.Dst
}
minikube image load soldevelo/kafka:4.0

Write-Host "`n==> Done. Verify with: minikube ssh -- docker images | grep atlas" -ForegroundColor Green
