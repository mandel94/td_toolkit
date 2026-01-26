#!/bin/bash
# Database management script for Windows/Unix compatibility

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Start the database
start_db() {
    print_status "Starting Articles Database..."
    check_docker
    docker-compose up -d
    print_status "Database started! Access Adminer at http://localhost:8080"
    print_status "Database connection: localhost:5432"
}

# Stop the database
stop_db() {
    print_status "Stopping Articles Database..."
    docker-compose down
    print_status "Database stopped."
}

# Restart the database
restart_db() {
    print_status "Restarting Articles Database..."
    check_docker
    docker-compose down
    docker-compose up -d
    print_status "Database restarted!"
}

# View database logs
logs_db() {
    print_status "Showing database logs..."
    docker-compose logs -f articles_postgres
}

# Connect to database via psql
connect_db() {
    print_status "Connecting to database..."
    docker-compose exec articles_postgres psql -U articles_user -d articles_db
}

# Backup database
backup_db() {
    print_status "Creating database backup..."
    timestamp=$(date +"%Y%m%d_%H%M%S")
    docker-compose exec articles_postgres pg_dump -U articles_user articles_db > "./data/backup_${timestamp}.sql"
    print_status "Backup created: ./data/backup_${timestamp}.sql"
}

# Show help
show_help() {
    echo "Articles Database Management Script"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start     Start the database containers"
    echo "  stop      Stop the database containers"
    echo "  restart   Restart the database containers"
    echo "  logs      Show database logs"
    echo "  connect   Connect to database via psql"
    echo "  backup    Create a database backup"
    echo "  help      Show this help message"
}

# Main script logic
case $1 in
    start)
        start_db
        ;;
    stop)
        stop_db
        ;;
    restart)
        restart_db
        ;;
    logs)
        logs_db
        ;;
    connect)
        connect_db
        ;;
    backup)
        backup_db
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_warning "Unknown command: $1"
        show_help
        exit 1
        ;;
esac