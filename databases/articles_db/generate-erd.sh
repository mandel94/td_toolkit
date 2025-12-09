#!/bin/bash
# Generate ERD Documentation for Articles Database (Linux/Mac version)
# This script uses SchemaSpy to generate interactive HTML documentation with ERD diagrams

CLEAN=false
OPEN=false

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --clean) CLEAN=true ;;
        --open) OPEN=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

echo "=== Articles Database ERD Generator ==="
echo ""

# Clean output directory if requested
if [ "$CLEAN" = true ] && [ -d "./schemaspy-output" ]; then
    echo "Cleaning previous output..."
    rm -rf ./schemaspy-output
fi

# Create output directory if it doesn't exist
mkdir -p ./schemaspy-output

# Check if database is running
echo "Checking if database is running..."
DB_RUNNING=$(docker ps --filter "name=articles_postgres" --filter "status=running" --format "{{.Names}}")

if [ -z "$DB_RUNNING" ]; then
    echo "Database is not running. Starting database..."
    docker-compose up -d articles_postgres
    echo "Waiting 10 seconds for database to be ready..."
    sleep 10
fi

# Generate ERD using SchemaSpy
echo "Generating ERD documentation with SchemaSpy..."
docker-compose --profile erd run --rm schemaspy

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ ERD documentation generated successfully!"
    echo ""
    echo "Output location: ./schemaspy-output/"
    echo "Main page: ./schemaspy-output/index.html"
    echo ""
    
    # Open in browser if requested
    if [ "$OPEN" = true ]; then
        INDEX_PATH="./schemaspy-output/index.html"
        if [ -f "$INDEX_PATH" ]; then
            echo "Opening ERD documentation in browser..."
            if command -v xdg-open > /dev/null; then
                xdg-open "$INDEX_PATH"
            elif command -v open > /dev/null; then
                open "$INDEX_PATH"
            else
                echo "Could not detect browser opener. Please open manually."
            fi
        fi
    else
        echo "Tip: Use --open flag to automatically open the documentation"
    fi
else
    echo "✗ Failed to generate ERD documentation"
    exit 1
fi
