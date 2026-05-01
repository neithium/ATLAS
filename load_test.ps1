$baseUrl = "http://localhost:8001/pcid/PLATCUST0005/acid/APPCUST0001/id"

$devices = 100   # change to 50, 200, 500, 1600

Write-Host "🚀 Sending $devices requests..."

for ($i=0; $i -lt $devices; $i++) {
    $deviceId = "PLAT1-DEV-{0:D4}-000" -f $i
    $url = "$baseUrl/$deviceId/export"

    try {
        Invoke-RestMethod -Method POST -Uri $url -TimeoutSec 5 | Out-Null
        Write-Host "✅ $deviceId"
    } catch {
        Write-Host "❌ Failed: $deviceId"
    }
}

Write-Host "🎯 Completed"