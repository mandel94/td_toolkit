# Articles Database

This is the dedicated PostgreSQL database for storing Taxi Drivers article analytics data.

## Quick Start

1. **Navigate to the database directory**:
   ```bash
   cd databases/articles_db
   ```

2. **Start the database**:
   ```bash
   docker-compose up -d
   ```

3. **Stop the database**:
   ```bash
   docker-compose down
   ```

4. **Access PgAdmin** (Web UI):
   - URL: http://localhost:8080
   - Email: admin@taxidrivers.local
   - Password: admin123

## Database Connection Details

- **Host**: localhost
- **Port**: 5432
- **Database**: articles_db
- **Username**: postgres
- **Password**: postgres123

## Directory Structure

```
databases/articles_db/
├── docker-compose.yml      # Docker configuration
├── init/                   # Database initialization scripts
│   └── 01-init-articles-db.sql
├── scripts/               # Utility scripts
│   ├── backup.sh
│   ├── restore.sh
│   └── connect.sh
├── backups/               # Database backups
└── README.md             # This file
```

## Database Schema

### Articles Table

The `articles` table follows data science naming conventions:

| Column | Type | Description |
|--------|------|-------------|
| `article_id` | SERIAL PRIMARY KEY | Auto-incrementing unique identifier |
| `title` | VARCHAR(500) | Article title |
| `author` | VARCHAR(200) | Article author |
| `category` | VARCHAR(100) | Article category |
| `screen_page_views` | INTEGER | Number of page views |
| `sessions` | INTEGER | Total number of sessions |
| `engaged_sessions` | INTEGER | Number of engaged sessions |
| `engagement_rate` | DECIMAL(5,4) | Engagement rate (0.0000 to 1.0000) |
| `average_session_duration` | DECIMAL(10,2) | Average session duration in seconds |
| `publication_date` | DATE | Article publication date |
| `page_path` | VARCHAR(1000) | Original page path |
| `url` | VARCHAR(1000) | Full article URL |
| `created_at` | TIMESTAMP WITH TIME ZONE | Record creation timestamp |
| `updated_at` | TIMESTAMP WITH TIME ZONE | Record last update timestamp |

### Indexes

Performance indexes are created on:
- `category`
- `publication_date`
- `screen_page_views`
- `engagement_rate`
- `created_at`

### Analytics View

The `articles_analytics` view provides additional calculated fields:
- `calculated_engagement_rate`: Calculated from engaged_sessions/sessions
- `days_since_publication`: Days since article publication

## Python Connection Example

```python
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'articles_db',
    'user': 'postgres',
    'password': 'postgres123'
}

# Using psycopg2
conn = psycopg2.connect(**DB_CONFIG)

# Using SQLAlchemy (recommended for pandas)
engine = create_engine(f"postgresql://postgres:postgres123@localhost:5432/articles_db")

# Example: Load data with pandas
df = pd.read_sql("SELECT * FROM articles_analytics ORDER BY screen_page_views DESC LIMIT 10", engine)
```

## Common Queries

### Top articles by page views
```sql
SELECT title, author, category, screen_page_views, engagement_rate
FROM articles 
ORDER BY screen_page_views DESC 
LIMIT 10;
```

### Articles by category performance
```sql
SELECT 
    category,
    COUNT(*) as article_count,
    AVG(screen_page_views) as avg_page_views,
    AVG(engagement_rate) as avg_engagement_rate
FROM articles 
GROUP BY category 
ORDER BY avg_page_views DESC;
```

### Recent high-performing articles
```sql
SELECT title, screen_page_views, engagement_rate, publication_date
FROM articles 
WHERE publication_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY screen_page_views DESC;
```

## Maintenance Scripts

Use the scripts in the `scripts/` directory for common operations:

- `./scripts/backup.sh` - Create database backup
- `./scripts/restore.sh <backup_file>` - Restore from backup
- `./scripts/connect.sh` - Connect to database via psql

## Backups

Backups are automatically stored in the `backups/` directory and are accessible from within the container.