# PowerShell scripts for Windows users

# Backup
Write-Host "Creating backup of articles_db..."
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupFile = "articles_db_backup_$timestamp.sql"
docker exec articles_postgres pg_dump -U postgres articles_db > "backups/$backupFile"
Write-Host "Backup created: $backupFile"

# Connect
# docker exec -it articles_postgres psql -U postgres -d articles_db

# Restore (example)
# docker exec -i articles_postgres psql -U postgres -d articles_db < backups/backup_file.sql