$url = "http://localhost:8001/pcid/PLATCUST0005/acid/APPCUST0001/id/PLAT1-DEV-0000-000/export"

Write-Host "🚀 Triggering API..."

$response = Invoke-RestMethod -Method POST -Uri $url

Write-Host "✅ Done"
$response