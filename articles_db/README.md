# Articles Database

A PostgreSQL database designed for storing and analyzing taxi drivers website article analytics data. This project follows data science best practices with proper naming conventions and includes Docker-based deployment.

## 📋 Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Database Schema](#database-schema)
- [Usage](#usage)
- [Management Scripts](#management-scripts)
- [Development](#development)
- [Backup and Restore](#backup-and-restore)

## ✨ Features

- **PostgreSQL 15** database with optimized configuration
- **Docker Compose** setup for easy deployment
- **Data science naming conventions** (snake_case columns)
- **Analytics-optimized indexes** for fast queries
- **Adminer** web interface for database management
- **Automated backups** and management scripts
- **Sample data** for testing
- **Analytics views and functions** for data analysis

## 🔧 Prerequisites

- Docker and Docker Compose
- PowerShell (Windows) or Bash (Unix/Linux)
- Basic knowledge of SQL and PostgreSQL

## 🚀 Quick Start

1. **Clone and navigate to the articles_db directory:**
   ```bash
   cd articles_db
   ```

2. **Configure environment variables:**
   ```bash
   # Copy and edit the environment file
   cp .env.example .env
   # Edit .env with your preferred credentials
   ```

3. **Start the database:**
   ```powershell
   # Windows PowerShell
   .\scripts\manage_db.ps1 -Command start
   
   # Or using Docker Compose directly
   docker-compose up -d
   ```

4. **Access the database:**
   - **Adminer Web Interface:** http://localhost:8080
   - **Direct connection:** localhost:5432
   - **Default credentials:** See `.env` file

## 📊 Database Schema

### Articles Table

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL | Primary key, auto-incrementing |
| `title` | VARCHAR(500) | Article title |
| `author` | VARCHAR(255) | Article author |
| `categoria` | VARCHAR(100) | Article category |
| `screen_page_views` | INTEGER | Number of page views |
| `engaged_sessions` | INTEGER | Number of engaged sessions |
| `sessions` | INTEGER | Total number of sessions |
| `engagement_rate` | DECIMAL(5,4) | Engagement rate (0-1) |
| `average_session_duration` | DECIMAL(10,4) | Average session duration in seconds |
| `publication_date` | DATE | Date of publication |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |

### Indexes

Optimized indexes for common analytics queries:
- Publication date
- Category
- Author
- Engagement rate
- Page views

### Views and Functions

- **`articles_analytics`**: Enhanced view with calculated metrics
- **`get_top_articles_by_metric()`**: Function to retrieve top performing articles

## 📖 Usage

### Basic Queries

```sql
-- Get all articles ordered by page views
SELECT * FROM articles ORDER BY screen_page_views DESC LIMIT 10;

-- Get articles by category
SELECT * FROM articles WHERE categoria = 'Technology';

-- Use the analytics view for enhanced metrics
SELECT * FROM articles_analytics 
WHERE publication_year = 2025 
ORDER BY engagement_rate DESC;
```

### Using Analytics Functions

```sql
-- Get top 5 articles by page views
SELECT * FROM get_top_articles_by_metric('screen_page_views', 5);

-- Get top articles by engagement rate in specific category
SELECT * FROM get_top_articles_by_metric('engagement_rate', 10, 'Technology');
```

## 🛠 Management Scripts

### PowerShell (Windows)

```powershell
# Start database
.\scripts\manage_db.ps1 -Command start

# Stop database
.\scripts\manage_db.ps1 -Command stop

# View logs
.\scripts\manage_db.ps1 -Command logs

# Connect via psql
.\scripts\manage_db.ps1 -Command connect

# Create backup
.\scripts\manage_db.ps1 -Command backup

# Show help
.\scripts\manage_db.ps1 -Command help
```

### Bash (Unix/Linux)

```bash
# Make script executable
chmod +x scripts/manage_db.sh

# Start database
./scripts/manage_db.sh start

# Stop database
./scripts/manage_db.sh stop

# View logs
./scripts/manage_db.sh logs
```

## 🔧 Development

### Environment Configuration

Edit `.env` file to customize:

```env
POSTGRES_DB=articles_db
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_secure_password
POSTGRES_PORT=5432
ADMINER_PORT=8080
```

### Adding New Tables or Views

1. Create SQL files in the `sql/` directory
2. Files are executed in alphabetical order during initialization
3. Restart the container to apply changes:
   ```bash
   docker-compose down && docker-compose up -d
   ```

### Data Import

Place CSV or SQL files in the `data/` directory and use psql to import:

```sql
-- Example: Import from CSV
COPY articles(title, author, categoria, screen_page_views, engaged_sessions, sessions, engagement_rate, average_session_duration, publication_date)
FROM '/data/your_data.csv'
DELIMITER ','
CSV HEADER;
```

## 💾 Backup and Restore

### Automated Backup

```powershell
.\scripts\manage_db.ps1 -Command backup
```

### Manual Backup

```bash
docker-compose exec articles_postgres pg_dump -U articles_user articles_db > backup.sql
```

### Restore from Backup

```bash
docker-compose exec -T articles_postgres psql -U articles_user -d articles_db < backup.sql
```

## 🔍 Troubleshooting

### Common Issues

1. **Port already in use:**
   - Change `POSTGRES_PORT` in `.env` file
   - Or stop the conflicting service

2. **Permission errors:**
   - Ensure Docker has proper permissions
   - Check volume mount permissions

3. **Connection refused:**
   - Wait for database to fully initialize
   - Check container logs: `docker-compose logs articles_postgres`

### Health Check

```bash
# Check if database is healthy
docker-compose ps

# View detailed logs
docker-compose logs -f articles_postgres
```

## 📁 Directory Structure

```
articles_db/
├── docker-compose.yml      # Docker Compose configuration
├── .env                    # Environment variables
├── .env.example           # Environment template
├── README.md              # This file
├── sql/                   # SQL initialization scripts
│   ├── 01_create_articles_table.sql
│   ├── 02_sample_data.sql
│   └── 03_analytics_views_functions.sql
├── scripts/               # Management scripts
│   ├── manage_db.ps1     # PowerShell script
│   └── manage_db.sh      # Bash script
└── data/                 # Data files and backups
```

## 🤝 Contributing

1. Follow the established naming conventions
2. Add proper comments to SQL files
3. Update documentation for new features
4. Test changes thoroughly

## 📄 License

This project is part of the Taxi Drivers analytics toolkit.

---

**Need Help?** Check the troubleshooting section or review the container logs.