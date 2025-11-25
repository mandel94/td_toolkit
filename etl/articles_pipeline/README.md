# Articles ETL Pipeline

A professional-grade ETL (Extract, Transform, Load) pipeline for processing article analytics data from Google Analytics 4 and loading it into a PostgreSQL database.

## 🏗️ Architecture

This ETL pipeline follows modern data engineering best practices with a modular, scalable architecture:

```
etl/articles_pipeline/
├── extractors/          # Data extraction modules
│   ├── ga4_extractor.py      # Google Analytics 4 API extraction
│   └── metadata_extractor.py # Article metadata scraping
├── transformers/        # Data transformation modules
│   └── article_transformer.py # Data cleaning and transformation
├── loaders/            # Data loading modules
│   └── database_loader.py    # PostgreSQL database operations
├── models/             # Data models
│   └── article.py           # Pydantic data models
├── config/             # Configuration
│   └── database.py          # Database and API configuration
├── pipeline.py         # Main ETL orchestrator
└── cli.py              # Command-line interface
```

## 🚀 Features

- **Modular Design**: Separate Extract, Transform, Load components
- **Data Validation**: Pydantic models for type safety and validation
- **Parallel Processing**: Concurrent metadata extraction for performance
- **Robust Error Handling**: Comprehensive logging and retry mechanisms
- **Database CRUD**: Full Create, Read, Update, Delete operations
- **CLI Interface**: Easy command-line execution
- **Batch Processing**: Efficient batch database operations
- **Upsert Logic**: Smart insert/update handling for data freshness

## 📊 Data Flow

1. **EXTRACT**
   - Fetch article performance data from GA4 API
   - Scrape article metadata (title, author, publication date) from website
   - Apply data cleaning transformations

2. **TRANSFORM**
   - Map articles to categories using existing taxonomy
   - Validate and normalize data types
   - Handle special cases (Si farà articles, merged categories)
   - Convert to database-ready format

3. **LOAD**
   - Batch insert/update articles in PostgreSQL
   - Handle duplicates with upsert logic
   - Maintain audit timestamps

## 🔧 Usage

### Command Line Interface

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline for last 7 days
python cli.py run --days 7

# Run for specific date range
python cli.py run --start-date 2025-11-01 --end-date 2025-11-07

# Run with custom parameters
python cli.py run --days 14 --min-page-views 50 --batch-size 200

# Skip metadata extraction (faster)
python cli.py run --days 7 --skip-metadata

# Test extraction only
python cli.py extract --days 3

# Check pipeline status
python cli.py status

# Clean up old data
python cli.py cleanup --days 90
```

### Python API

```python
from etl.articles_pipeline import ArticlesETLPipeline

# Initialize pipeline
pipeline = ArticlesETLPipeline()

# Run full pipeline
result = pipeline.run_full_pipeline(
    start_date='2025-11-01',
    end_date='2025-11-07',
    min_page_views=30,
    batch_size=100,
    upsert=True,
    extract_metadata=True
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