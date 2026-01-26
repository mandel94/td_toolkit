#!/bin/bash
# Restore script for articles database

if [ $# -eq 0 ]; then
    echo "Usage: $0 <backup_file>"
    echo "Available backups:"
    ls -la /backups/articles_db_backup_*.sql 2>/dev/null || echo "No backups found"
    exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "/backups/${BACKUP_FILE}" ]; then
    echo "Backup file not found: /backups/${BACKUP_FILE}"
    exit 1
fi

echo "Restoring database from: ${BACKUP_FILE}"
echo "WARNING: This will drop and recreate the articles_db database!"
read -p "Are you sure? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Drop and recreate database
    dropdb -U postgres -h localhost articles_db
    createdb -U postgres -h localhost articles_db
    
    # Restore from backup
    psql -U postgres -h localhost -d articles_db < "/backups/${BACKUP_FILE}"
    
    if [ $? -eq 0 ]; then
        echo "Database restored successfully!"
    else
        echo "Restore failed!"
        exit 1
    fi
else
    echo "Restore cancelled."
fi