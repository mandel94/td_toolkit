#!/bin/bash
# Backup script for articles database

BACKUP_DIR="/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="articles_db_backup_${TIMESTAMP}.sql"

echo "Creating backup of articles_db..."
pg_dump -U postgres -h localhost articles_db > "${BACKUP_DIR}/${BACKUP_FILE}"

if [ $? -eq 0 ]; then
    echo "Backup created successfully: ${BACKUP_FILE}"
    
    # Keep only last 10 backups
    cd $BACKUP_DIR
    ls -t articles_db_backup_*.sql | tail -n +11 | xargs rm -f
    echo "Old backups cleaned up (keeping last 10)"
else
    echo "Backup failed!"
    exit 1
fi