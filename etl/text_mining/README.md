# Text Mining Pipeline - README

## Overview

Event-driven, modular text mining pipeline for extracting features from editorial content and correlating with GA4 performance metrics.

## Architecture

### Synchronous Mode (orchestrator.py)
Sequential execution for simplicity:
```
GA4 Extraction → Scraping → Storage → Feature Extraction → Storage
```

### Asynchronous Mode (workers.py + async_orchestrator.py)
Event-driven with parallel workers:
```
GA4 Publisher → [Redis Stream: ga4_sample_ready]
                         ↓
                Scraper Workers (parallel)
                         ↓
                [Redis Stream: article_html_scraped]
                         ↓
                Feature Workers (parallel)
                         ↓
                    PostgreSQL
```

### File Structure

```
etl/text_mining/
├── config.py                     # Central configuration
├── events.py                     # Pydantic event schemas
├── orchestrator.py               # Synchronous orchestrator
├── async_orchestrator.py         # Async orchestrator (manages workers)
├── workers.py                    # Async workers (publisher, scraper, feature)
├── docker-compose.yml            # Infrastructure (Redis, PostgreSQL)
├── requirements.txt              # Python dependencies
├── extractors/
│   └── ga4_sample_extractor.py   # GA4 sampling with KPIs
├── scrapers/
│   └── content_scraper.py        # HTML content scraping
├── processors/
│   └── text_feature_extractor.py # Text feature extraction
├── messaging/
│   └── redis_queue.py            # Redis Streams wrapper
└── storage/
    └── postgres_storage.py       # PostgreSQL persistence
```

## Quick Start

### 1. Install Dependencies

```bash
cd etl/text_mining
pip install -r requirements.txt
```

### 2. Start Infrastructure (Optional - for event-driven mode)

```bash
docker-compose up -d
```

### 3. Run Pipeline

#### Option A: Synchronous Mode (Simple, for testing)

```bash
# Run with default settings (10 articles) - all steps sequential
python orchestrator.py

# Custom sample size
python orchestrator.py --sample-size 20
```

#### Option B: Asynchronous Mode (Production, parallel workers)

```bash
# Start async orchestrator with workers
python async_orchestrator.py --sample-size 10 --num-scrapers 2 --num-features 2

# Or run workers separately in different terminals:

# Terminal 1: Start scraper workers
python workers.py scraper --worker-id scraper-1

# Terminal 2: Start feature workers  
python workers.py feature --worker-id feature-1

# Terminal 3: Publish job
python workers.py publisher --sample-size 10

# Run all in sequence (testing)
python workers.py all --sample-size 5
```

## Configuration

Edit `config.py` or use environment variables:

- `GA4_PROPERTY_ID`: Google Analytics property ID
- `SAMPLE_SIZE`: Number of articles to sample
- `SCRAPER_DOMAIN`: Base domain for scraping
- `POSTGRES_*`: Database connection settings
- `REDIS_*`: Redis connection settings

## Pipeline Steps

1. **GA4 Extraction**: Sample random articles with performance metrics
2. **Content Scraping**: Download HTML content from article pages
3. **Raw Storage**: Store HTML in PostgreSQL
4. **Feature Extraction**: Extract text features (word count, etc.)
5. **Feature Storage**: Store features with GA4 metrics

## Database Schema

### Tables

- `text_mining.articles_raw`: Raw HTML content (versioned)
- `text_mining.articles_features`: Extracted features + GA4 metrics
- `text_mining.sample_metadata`: Pipeline run metadata

All tables use append-only design for version control.

## Features Extracted (MVP)

- `word_count`: Total words in article
- `char_count`: Total characters
- `paragraph_count`: Number of paragraphs
- GA4 metrics: pageviews, engaged_sessions, engagement_rate, etc.
- `editorial_score`: Weighted performance score

## Extending the Pipeline

### Add New Features

Edit `processors/text_feature_extractor.py`:

```python
def _extract_article_features(self, article):
    # Add your feature extraction logic
    features['my_new_feature'] = calculate_feature(text)
    return features
```

### Change Scoring Algorithm

EdiAsynchronous Architecture Details

### How It Works

1. **GA4 Publisher Worker**: Extracts sample from GA4 and publishes to Redis stream
2. **Scraper Workers** (scalable): Consume GA4 events, scrape HTML, publish scraped events
3. **Feature Workers** (scalable): Consume scraped events, extract features, store in DB

### Benefits

✅ **Parallel Processing**: Multiple workers process articles simultaneously  
✅ **Fault Tolerance**: Worker failures don't affect others  
✅ **Scalability**: Add more workers for higher throughput  
✅ **Backpressure**: Redis Streams handle load naturally  
✅ **Monitoring**: Track stream lengths and worker activity  

### Commands

```bash
# Start full async pipeline
python async_orchestrator.py --sample-size 20 --num-scrapers 3 --num-features 2

# Start single worker type (different terminals)
python workers.py scraper --worker-id scraper-1    # Scraper worker
python workers.py feature --worker-id feature-1    # Feature worker
python workers.py publisher --sample-size 10       # Publish one job

# Non-blocking mode (process once and exit)
python workers.py scraper --non-blocking

# Publish job to existing workers
python async_orchestrator.py --publish-only --sample-size 15
```

## Future Enhancements

- [x] ~~Async event-driven processing with Redis~~ ✅ Implemented!
- [ ] Advanced NLP features (sentiment, topics, entities)
- [ ] Real-time monitoring dashboard
- [ ] A/B testing framework for scoring algorithms
- [ ] Multi-site support
- [ ] Kubernetes deployment
- [ ] Dead letter queue for failed processing
```

## Production Considerations

- Set appropriate `SCRAPER_DELAY` to respect rate limits
- Monitor database size (raw HTML can grow large)
- Use `processing_version` to track algorithm changes
- Consider partitioning tables by date for large datasets

## Troubleshooting

**Database Connection Errors**: Ensure PostgreSQL is running and credentials are correct

**GA4 API Errors**: Verify credentials file path and OAuth token

**Scraping Timeouts**: Increase delay or implement retry logic

## Future Enhancements

- [ ] Async event-driven processing with Redis
- [ ] Advanced NLP features (sentiment, topics, entities)
- [ ] Real-time monitoring dashboard
- [ ] A/B testing framework for scoring algorithms
- [ ] Multi-site support
