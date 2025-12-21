# Stop Movie Trends API Documentation
# Script to stop the documentation server

Write-Host "`n🛑 Stopping Documentation Server" -ForegroundColor Yellow
Write-Host "================================`n" -ForegroundColor Yellow

docker-compose down

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Documentation server stopped successfully!`n" -ForegroundColor Green
} else {
    Write-Host "`n❌ Error stopping documentation server.`n" -ForegroundColor Red
}
