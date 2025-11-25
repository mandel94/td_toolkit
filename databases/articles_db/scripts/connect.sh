#!/bin/bash
# Connect to articles database

echo "Connecting to articles_db..."
psql -U postgres -h localhost -d articles_db