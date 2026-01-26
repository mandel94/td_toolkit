# Movie Trends API Documentation Launcher
# Quick script to start the documentation server

Write-Host "`n🎬 Movie Trends API Documentation" -ForegroundColor Cyan
Write-Host "================================`n" -ForegroundColor Cyan

# Check if Docker is running
Write-Host "Checking Docker..." -ForegroundColor Yellow
try {
    docker info > $null 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ Docker is running`n" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not installed or not running." -ForegroundColor Red
    exit 1
}

# Get current directory
$currentDir = Get-Location
Write-Host "📁 Current directory: $currentDir`n" -ForegroundColor Gray

# Start Docker Compose
Write-Host "🚀 Starting documentation server..." -ForegroundColor Yellow
docker-compose up -d

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ Documentation server started successfully!`n" -ForegroundColor Green
    
    # Wait a moment for server to start
    Start-Sleep -Seconds 3
    
    Write-Host "📖 Opening documentation in browser..." -ForegroundColor Yellow
    Start-Process "http://localhost:8001"
    
    Write-Host "`n📍 Access the documentation at: http://localhost:8001" -ForegroundColor Cyan
    Write-Host "`n💡 Tips:" -ForegroundColor Yellow
    Write-Host "   - Press '/' or 'S' to search" -ForegroundColor White
    Write-Host "   - Toggle dark mode with the sun/moon icon" -ForegroundColor White
    Write-Host "   - All diagrams are interactive" -ForegroundColor White
    Write-Host "`n🛑 To stop: docker-compose down" -ForegroundColor Yellow
    Write-Host "`n📝 To view logs: docker-compose logs -f" -ForegroundColor Yellow
} else {
    Write-Host "`n❌ Failed to start documentation server." -ForegroundColor Red
    Write-Host "Check the errors above for details.`n" -ForegroundColor Red
    exit 1
}

Write-Host "`n✨ Happy exploring! ✨`n" -ForegroundColor Cyan
