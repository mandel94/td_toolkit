# Weekly Articles ETL Pipeline

A professional-grade ETL (Extract, Transform, Load) pipeline for processing weekly article analytics data from Google Analytics 4 and loading it into a PostgreSQL dimensional database (star schema).

## 🏗️ Architecture

This ETL pipeline follows modern data warehouse best practices with a **dimensional star schema** design:

```
etl/articles_db_pipeline/
├── extractors/          # Data extraction modules
│   ├── ga4_extractor.py      # Google Analytics 4 API extraction (weekly data)
│   └── __init__.py
├── transformers/        # Data transformation modules
│   └── article_transformer.py # Dimensional data transformation
├── loaders/            # Data loading modules
│   └── database_loader.py    # PostgreSQL dimensional loader
├── models/             # Data models
│   └── article.py           # Pydantic data models for dimensions & facts
├── config/             # Configuration
│   └── database.py          # Database and API configuration
├── pipeline.py         # Main ETL orchestrator
└── cli.py              # Command-line interface
```

## 🎯 Purpose

This pipeline extracts **weekly performance metrics** for articles from Google Analytics 4 and loads them into a **star schema dimensional model** for analytical queries and reporting.

### Target Year: 2025
- Default start date: **January 1, 2025**
- Default end date: **Today**
- Granularity: **Weekly** (ISO weeks starting Monday)

## 📊 Data Model

### Star Schema Design

**Fact Table:**
- `fact_weekly_metrics` - Weekly article performance (grain: article × week)

**Dimension Tables:**
- `dim_weeks` - Week dimension (year, week, start/end dates, quarter, month)
- `dim_articles` - Article master data (page_path, title, publication_date)
- `dim_authors` - Content creators
- `dim_categories` - Content classification

### Key Relationships
- Articles ↔ Weekly Metrics (1:N)
- Authors ↔ Weekly Metrics (1:N)
- Categories ↔ Weekly Metrics (1:N)
- Weeks ↔ Weekly Metrics (1:N)

## 🚀 Usage

### Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Load all 2025 weekly data (Jan 1 to today)
python cli.py

# Load with custom end date
python cli.py --end-date 2025-12-31

# Load from specific start date
python cli.py --start-date 2025-06-01

# Show database statistics only
python cli.py --stats-only

# Verbose logging
python cli.py --verbose

# Save results to JSON
python cli.py --output results.json
```

### Command Line Options

```bash
python cli.py --help

Options:
  --start-date YYYY-MM-DD   Start date (default: 2025-01-01)
  --end-date YYYY-MM-DD     End date (default: today)
  --min-page-views N        Minimum page views per week (default: 30)
  --output FILE             Save results to JSON file
  --verbose                 Enable DEBUG logging
  --stats-only              Show database stats without running ETL
```

## 📈 Data Flow

1. **EXTRACT**
   - Fetch weekly article performance data from GA4 API
   - Dimensions: `pagePath`, `year`, `week`
   - Metrics: `screenPageViews`, `sessions`, `engagedSessions`, `engagementRate`, `averageSessionDuration`
   - Filter by minimum page views threshold (default: 30 per week)

2. **TRANSFORM**
   - Map articles to categories using existing taxonomy
   - Calculate week dimensions (start/end dates, quarter, month)
   - Extract unique dimensions (weeks, articles, authors, categories)
   - Create fact table metrics linking dimensions
   - Handle special cases (Si farà articles, merged categories)

3. **LOAD**
   - Load dimension tables first (with upsert logic)
   - Map dimension natural keys to surrogate keys
   - Load fact table with weekly metrics
   - Handle updates for existing week/article combinations

## 🔧 Configuration

### Environment Variables

```bash
# Database configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=articles_db
DB_USER=postgres
DB_PASSWORD=postgres123

# GA4 configuration
GA4_PROPERTY_ID=394327334
```

### Default Settings

- **Start Date**: 2025-01-01 (hardcoded for 2025 data)
- **Min Page Views**: 30 per week
- **Week Definition**: ISO weeks (Monday to Sunday)

## 📋 Features

- ✅ **Dimensional Model**: Star schema for analytical queries
- ✅ **Weekly Granularity**: ISO week-based aggregation
- ✅ **Upsert Logic**: Smart insert/update for idempotency
- ✅ **Data Validation**: Pydantic models for type safety
- ✅ **Robust Error Handling**: Comprehensive logging
- ✅ **Modular Design**: Separate Extract, Transform, Load
- ✅ **2025 Focus**: Optimized for loading full year 2025

## 🔍 Database Schema

### DimWeek
- `week_id` (PK): YYYYWW format (e.g., 202501)
- `year`, `week_of_year`
- `week_start_date`, `week_end_date`
- `quarter`, `month`
- `year_week`: "YYYY-WNN" format

### DimArticle
- `article_id` (PK)
- `page_path` (unique)
- `title`, `publication_date`

### DimAuthor
- `author_id` (PK)
- `author_name` (unique)

### DimCategory
- `category_id` (PK)
- `category_name` (unique)

### FactWeeklyMetrics
- `article_id`, `week_id` (Composite PK)
- `author_id`, `category_id` (FKs)
- `screen_page_views`, `sessions`, `engaged_sessions`
- `engagement_rate`, `average_session_duration`

## 📝 Example Output

```json
{
  "status": "success",
  "start_date": "2025-01-01",
  "end_date": "2025-12-11",
  "stats": {
    "extracted_records": 15234,
    "weeks": 50,
    "articles": 3421,
    "authors": 1,
    "categories": 12,
    "metrics_loaded": 15234
  },
  "load_details": {
    "weeks_loaded": 50,
    "articles_loaded": 234,
    "authors_loaded": 0,
    "categories_loaded": 1,
    "metrics_loaded": 15234,
    "errors": 0
  },
  "duration_seconds": 45.32
}
```

## 🔄 Idempotency

The pipeline is **idempotent** - running it multiple times with the same date range will:
- Skip existing dimension records (weeks, articles, authors, categories)
- Update existing fact metrics if the week/article combination exists
- Insert new fact metrics for new combinations

This allows safe re-runs for data refresh or corrections.

## 🚨 Error Handling

- Connection failures: Validates database connection before starting
- Data quality issues: Validates numeric ranges, engagement rates (0-1)
- Missing dimensions: Logs warnings and skips problematic records
- Transaction safety: Uses session management with rollback on errors

## 📞 Support

**Pipeline Location:** `etl/articles_db_pipeline/`
**Database Schema:** `databases/articles_db/`
**Logs:** `etl/articles_db_pipeline/logs/etl_pipeline.log`
)

print(f"Pipeline status: {result['status']}")
print(f"Articles processed: {result['stats']}")
```

## 📋 Database Schema Mapping

The pipeline transforms GA4 data to match the articles database schema:

| GA4 Field | Database Column | Transformation |
|-----------|----------------|----------------|
| `pagePath` | `page_path` | Direct mapping |
| `screenPageViews` | `screen_page_views` | Integer conversion |
| `sessions` | `sessions` | Integer conversion |
| `engagedSessions` | `engaged_sessions` | Integer conversion |
| `engagementRate` | `engagement_rate` | Decimal (0-1) |
| `averageSessionDuration` | `average_session_duration` | Decimal (seconds) |
| (scraped) | `title` | HTML extraction + cleaning |
| (scraped) | `author` | HTML extraction + cleaning |
| (scraped) | `publication_date` | Date parsing |
| (derived) | `category` | Path-based categorization |
| (derived) | `url` | Domain + path combination |

## ⚙️ Configuration

Key configuration parameters in `config/database.py`:

```python
# Database connection
DATABASE_URL = "postgresql://user:pass@host:port/database"

# GA4 API settings
GA4_PROPERTY_ID = '394327334'
GA4_METRICS = ['screenPageViews', 'sessions', 'engagedSessions', ...]

# Processing settings
MIN_PAGE_VIEWS_THRESHOLD = 30
BATCH_SIZE = 100
MAX_WORKERS_SCRAPING = 8
```

## 🔍 Monitoring & Logging

The pipeline includes comprehensive logging:

- **Console Logging**: Real-time progress updates
- **File Logging**: Detailed logs with rotation (10MB, 30 days retention)
- **Structured Logging**: JSON-compatible log format
- **Error Tracking**: Failed operations with context

Log locations:
- Console: INFO level by default, DEBUG with `--verbose`
- File: `logs/etl_pipeline.log`

## 🛡️ Error Handling

- **Retry Logic**: Automatic retries for transient failures
- **Graceful Degradation**: Continue processing if individual articles fail
- **Data Validation**: Pre-insertion validation with Pydantic models
- **Transaction Safety**: Database operations in transactions
- **Connection Management**: Automatic connection pooling and cleanup

## 📈 Performance Considerations

- **Parallel Processing**: Concurrent metadata extraction (8 workers default)
- **Batch Operations**: Database inserts in configurable batches
- **Connection Pooling**: Efficient database connection reuse
- **Memory Management**: Streaming data processing for large datasets
- **Rate Limiting**: Respectful scraping with delays

## 🔧 Deployment Considerations

- **Environment Variables**: Support for configuration via environment
- **Docker Compatible**: Can be containerized for deployment
- **Monitoring Ready**: Structured logs for observability tools
- **Scalable**: Horizontal scaling support through configuration

## 🧪 Testing

The pipeline supports testing through:

- **Extract-only Mode**: Test data extraction without database operations
- **Dry Run Support**: Validate transformations without loading
- **Status Checks**: Health checks for all components
- **Sample Data**: Preview extracted data before full processing

## 🚨 Best Practices

1. **Start Small**: Test with small date ranges first
2. **Monitor Resources**: Watch memory usage for large datasets  
3. **Regular Cleanup**: Use cleanup command to manage database size
4. **Error Monitoring**: Check logs regularly for failures
5. **Backup Database**: Backup before major changes
6. **Environment Separation**: Use different databases for dev/prod

## 🔄 Integration with Existing Reports

This ETL pipeline complements the existing reporting system:

- **Data Source**: Uses same GA4 API and scraping logic as `weekly_report.py`
- **Category Mapping**: Reuses `map_ga4_categories` function
- **Article Processing**: Leverages existing `ArticleProcessor` and `ArticleScraper`
- **Database Storage**: Enables persistent storage for historical analysis
- **Report Enhancement**: Provides data foundation for advanced analytics