# Movie Trends API - Technical Documentation

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Model](#data-model)
3. [Trend Scoring Algorithm](#trend-scoring-algorithm)
4. [API Design](#api-design)
5. [Deployment Guide](#deployment-guide)
6. [Monitoring & Operations](#monitoring--operations)

## Architecture Overview

### System Architecture

```
┌─────────────┐
│  TMDb API   │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│ Ingestion Service   │ (Async Python + httpx)
│ - Rate limiting     │
│ - Retry logic       │
│ - Batch processing  │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Raw Staging       │ (PostgreSQL JSONB)
│ - Audit trail       │
│ - Batch tracking    │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  Transformation     │ (Pure Python)
│ - Trend scoring     │
│ - Normalization     │
│ - Classification    │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Analytics Store     │ (Dimensional Model)
│ - Fact tables       │
│ - Dimensions        │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   REST API          │ (FastAPI)
│ - Versioned         │
│ - OpenAPI docs      │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Consumers         │
│ - Dashboards        │
│ - Analytics tools   │
│ - Editorial systems │
└─────────────────────┘
```

### Design Patterns

1. **Repository Pattern**: Abstracts data access
2. **Dependency Injection**: FastAPI dependencies for testability
3. **Factory Pattern**: Client creation and configuration
4. **Strategy Pattern**: Pluggable trend scoring strategies
5. **Observer Pattern**: Event-driven pipeline orchestration

## Data Model

### Raw Layer (Staging)

**Purpose**: Capture exact API responses for auditability

```sql
-- Raw trending snapshots
CREATE TABLE raw_tmdb_trending (
    id BIGSERIAL PRIMARY KEY,
    time_window VARCHAR(10) NOT NULL,
    media_type VARCHAR(10) NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL,
    import_batch_id VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Raw movie details
CREATE TABLE raw_tmdb_movies (
    id BIGSERIAL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL,
    import_batch_id VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Analytics Layer (Dimensional Model)

**Purpose**: Optimized for analytical queries and trend calculation

#### Dimensions

```sql
-- Movie dimension (SCD Type 2)
CREATE TABLE dim_movie (
    movie_key BIGSERIAL PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    title VARCHAR(500) NOT NULL,
    release_date DATE,
    genres JSONB,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Date dimension
CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    week INTEGER
);
```

#### Facts

```sql
-- Daily popularity facts
CREATE TABLE fact_movie_popularity_daily (
    id BIGSERIAL PRIMARY KEY,
    movie_key BIGINT REFERENCES dim_movie(movie_key),
    date_key DATE REFERENCES dim_date(date_key),
    popularity FLOAT NOT NULL,
    vote_count INTEGER NOT NULL,
    vote_average FLOAT NOT NULL,
    UNIQUE(movie_key, date_key)
);

-- Weekly trend facts
CREATE TABLE fact_movie_trends_weekly (
    id BIGSERIAL PRIMARY KEY,
    movie_key BIGINT REFERENCES dim_movie(movie_key),
    week_start_date DATE NOT NULL,
    week_end_date DATE NOT NULL,
    
    -- Base metrics
    avg_popularity FLOAT NOT NULL,
    avg_vote_count INTEGER NOT NULL,
    
    -- Growth metrics
    popularity_growth FLOAT,
    vote_velocity FLOAT,
    
    -- Normalized components
    norm_popularity_growth FLOAT,
    norm_vote_velocity FLOAT,
    
    -- Adjustment factors
    recency_factor FLOAT NOT NULL,
    stability_factor FLOAT NOT NULL,
    volatility FLOAT,
    
    -- Final score
    trend_score FLOAT NOT NULL,
    trend_classification VARCHAR(20) NOT NULL,
    
    formula_version VARCHAR(10) NOT NULL,
    UNIQUE(movie_key, week_start_date)
);
```

## Trend Scoring Algorithm

### Mathematical Definition

```python
# Step 1: Calculate growth metrics
popularity_growth = (current_pop - previous_pop) / max(previous_pop, 1)
vote_velocity = (current_votes - previous_votes) / max(previous_votes, 1)

# Step 2: Normalize across population (percentile-based)
norm_pop_growth = percentile_normalize(popularity_growth, all_growths)
norm_vote_velocity = percentile_normalize(vote_velocity, all_velocities)

# Step 3: Calculate adjustment factors
recency_factor = exp(-days_since_release / λ)  # λ = 75 days
stability_factor = 1 / (1 + volatility)

# Step 4: Compute final score
base_score = (0.6 × norm_pop_growth + 0.4 × norm_vote_velocity)
trend_score = 100 × base_score × recency_factor × stability_factor

# Step 5: Classify
if trend_score > 75 and delta > 10:
    classification = "EMERGING"
elif trend_score > 75 and abs(delta) < 3:
    classification = "PEAKING"
elif trend_score < 40 and delta < 0:
    classification = "DECLINING"
else:
    classification = "STABLE"
```

### Design Principles

1. **Explainability**: Every component has clear interpretation
2. **Comparability**: Normalized scores enable cross-movie comparison
3. **Stability**: Percentile normalization robust to outliers
4. **Versioning**: Formula version tracked for reproducibility

### Tunable Parameters

| Parameter | Default | Range | Impact |
|-----------|---------|-------|--------|
| `popularity_weight` | 0.6 | 0.0-1.0 | Weight of popularity growth |
| `vote_velocity_weight` | 0.4 | 0.0-1.0 | Weight of vote velocity |
| `recency_lambda` | 75 | 30-180 | Decay rate for recency |
| `volatility_periods` | 4 | 2-10 | Periods for volatility calculation |

## API Design

### Versioning Strategy

- URL-based versioning: `/v1/trends/movies`
- Stable contracts with explicit breaking changes
- Formula version in metadata for transparency

### Error Handling

```json
{
  "error": {
    "code": "INVALID_PARAMETER",
    "message": "Unsupported time_window",
    "details": {
      "allowed_values": ["daily", "weekly"]
    }
  }
}
```

### Response Structure

All responses include metadata:

```json
{
  "meta": {
    "api_version": "v1",
    "time_window": "weekly",
    "as_of": "2025-01-06",
    "trend_definition_version": "1.0"
  },
  "data": [...]
}
```

## Deployment Guide

### Production Checklist

- [ ] Set strong database passwords
- [ ] Configure CORS properly
- [ ] Enable HTTPS/TLS
- [ ] Set up database backups
- [ ] Configure log aggregation
- [ ] Set up monitoring alerts
- [ ] Review resource limits
- [ ] Enable rate limiting
- [ ] Configure secret management

### Environment-Specific Settings

**Development**
```bash
API_RELOAD=true
LOG_LEVEL=DEBUG
API_WORKERS=1
```

**Staging**
```bash
API_RELOAD=false
LOG_LEVEL=INFO
API_WORKERS=2
```

**Production**
```bash
API_RELOAD=false
LOG_LEVEL=WARNING
API_WORKERS=4
```

## Monitoring & Operations

### Key Metrics

1. **API Performance**
   - Request latency (p50, p95, p99)
   - Requests per second
   - Error rate

2. **Data Pipeline**
   - Ingestion batch duration
   - Trend calculation time
   - Data freshness

3. **Database**
   - Connection pool usage
   - Query performance
   - Table sizes

### Logging

Structured JSON logs with fields:
- `timestamp`
- `level`
- `logger`
- `message`
- `context` (batch_id, movie_id, etc.)

### Alerts

Recommended alerts:
- API error rate > 5%
- Ingestion failure
- Database connection failures
- Disk usage > 80%

### Backup Strategy

1. **Database**: Daily automated backups
2. **Raw Data**: Retain for 90 days
3. **Analytics**: Can be recalculated from raw

### Disaster Recovery

- RTO (Recovery Time Objective): 1 hour
- RPO (Recovery Point Objective): 24 hours
- Automated backups with point-in-time recovery

---

For more details, see the inline code documentation and OpenAPI specs at `/docs`.
