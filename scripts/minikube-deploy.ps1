# Deploy ATLAS to Minikube. Run after minikube-load-images.ps1
# Usage: .\scripts\minikube-deploy.ps1

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path $PSScriptRoot -Parent
Set-Location $RepoRoot

Write-Host "==> Delta Lake hostPath on Minikube node" -ForegroundColor Cyan
minikube ssh -- "sudo mkdir -p /data/atlas/delta-refined && sudo chmod 777 /data/atlas/delta-refined"

Write-Host "`n==> Applying k8s manifests (minikube overlay - reduced CPU/RAM)" -ForegroundColor Cyan
kubectl apply -k k8s/overlays/minikube

Write-Host "`n==> Waiting for Kafka topic init job..." -ForegroundColor Cyan
kubectl wait --for=condition=complete job/atlas-kafka-init -n atlas --timeout=600s 2>$null

Write-Host "`n==> Pod status" -ForegroundColor Cyan
kubectl get pods -n atlas -o wide

Write-Host "`n==> Port-forward (run in separate terminals):" -ForegroundColor Green
Write-Host "   kubectl port-forward -n atlas svc/atlas-fastapi 8080:80"
Write-Host "   kubectl port-forward -n atlas svc/atlas-streamlit 8501:8501"
