# 🎬 Movie Trends Data Product API

> A production-ready REST API exposing movie trend insights derived from TMDb

## Overview

The Movie Trends Data Product is a scalable data platform that:

- ✅ Continuously ingests movie popularity and trending signals from TMDb
- ✅ Transforms them into interpretable trend metrics
- ✅ Exposes insights via a REST API for analytics, editorial, and product teams

## Key Features

- **Explainable Trend Scoring**: Fully decomposed metrics with transparency
- **Modern Architecture**: FastAPI + PostgreSQL + Async Python
- **Production Ready**: Docker, observability, comprehensive testing
- **Scalable Design**: Repository pattern, dependency injection, clean architecture
- **Type Safe**: Full Pydantic validation and MyPy support

## Quick Start

### Prerequisites

- Docker & Docker Compose
- TMDb API Key ([Get one here](https://www.themoviedb.org/settings/api))

### 1. Clone and Configure

```bash
cd movie_trends_api
cp .env.example .env
# Edit .env and add your TMDB_API_KEY
```

### 2. Start Services

```bash
docker-compose up -d
```

This starts:
- PostgreSQL database
- Redis cache
- FastAPI application
- Prefect server (orchestration)
- Prometheus (metrics)

### 3. Initialize Database

```bash
docker-compose exec api python -m movie_trends.cli init-db
```

### 4. Run Initial Data Pipeline

```bash
docker-compose exec api python -m movie_trends.cli run-pipeline
```

### 5. Access the API

- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **Metrics**: http://localhost:8000/metrics
- **Prefect UI**: http://localhost:4200

## API Endpoints

### Get Trending Movies

```http
GET /v1/trends/movies?time_window=weekly&limit=20
```

**Response:**
```json
{
  "meta": {
    "api_version": "v1",
    "time_window": "weekly",
    "as_of": "2025-01-06",
    "trend_definition_version": "1.0"
  },
  "data": [
    {
      "movie": {
        "movie_id": 12345,
        "title": "Example Movie",
        "release_date": "2025-02-10",
        "genres": ["Action", "Drama"]
      },
      "trend_metrics": {
        "trend_score": 82.4,
        "trend_classification": "EMERGING",
        "popularity_growth": 0.42,
        "vote_velocity": 0.31,
        "recency_factor": 0.78,
        "stability_factor": 0.91
      },
      "trend_history": {
        "previous_score": 65.2,
        "delta": 17.2
      }
    }
  ]
}
```

### Get Movie Trend Details

```http
GET /v1/trends/movies/{movie_id}
```

Returns detailed trend data including time series history.

### Compare Multiple Movies

```http
GET /v1/trends/compare?ids=123,456,789
```

## Architecture

```
TMDb API
   │
   ▼
Ingestion Service (async Python)
   │
   ▼
Raw Staging (PostgreSQL JSONB)
   │
   ▼
Transform & Metrics Layer
   │
   ▼
Analytics Store (dimensional model)
   │
   ▼
REST API (FastAPI)
   │
   ▼
Consumers
```

## Trend Scoring Formula

The trend score is **explainable by design**:

```
trend_score = 100 × (w₁ × norm_pop_growth + w₂ × norm_vote_velocity)
              × recency_factor × stability_factor
```

**Components:**

1. **Popularity Growth** (60% weight): Relative week-over-week popularity change
2. **Vote Velocity** (40% weight): Rate of vote count increase
3. **Recency Factor**: Exponential decay favoring newer releases
4. **Stability Factor**: Penalty for high volatility

**Classification:**
- **EMERGING**: High score + strong momentum
- **PEAKING**: High score + stable
- **STABLE**: Moderate score
- **DECLINING**: Low score + negative momentum

## Development

### Local Setup

```bash
# Install Poetry
pip install poetry

# Install dependencies
poetry install

# Activate virtual environment
poetry shell

# Run tests
pytest

# Run linting
ruff check .
black --check .
mypy movie_trends/

# Format code
black .
```

### Run Locally (without Docker)

```bash
# Set environment variables
export DATABASE_URL="postgresql+asyncpg://postgres:password@localhost:5432/movie_trends"
export TMDB_API_KEY="your_key_here"

# Initialize database
python -m movie_trends.cli init-db

# Run API server
python -m movie_trends.main

# Or with auto-reload
uvicorn movie_trends.main:app --reload
```

## CLI Commands

```bash
# Initialize database
python -m movie_trends.cli init-db

# Run daily ingestion
python -m movie_trends.cli ingest-daily

# Calculate weekly trends
python -m movie_trends.cli calculate-trends

# Run full pipeline
python -m movie_trends.cli run-pipeline

# Backfill historical data
python -m movie_trends.cli backfill 2024-01-01 --end-date 2024-12-31
```

## Project Structure

```
movie_trends_api/
├── movie_trends/
│   ├── api/              # FastAPI routes
│   │   └── v1/
│   │       └── trends.py
│   ├── clients/          # External API clients
│   │   └── tmdb_client.py
│   ├── database/         # Database models & session
│   │   ├── models.py
│   │   ├── base.py
│   │   └── session.py
│   ├── repositories/     # Data access layer
│   │   └── repositories.py
│   ├── schemas/          # Pydantic models
│   │   ├── api.py
│   │   └── tmdb.py
│   ├── services/         # Business logic
│   │   ├── ingestion.py
│   │   ├── transformation.py
│   │   └── trend_scoring.py
│   ├── orchestration/    # Prefect flows
│   │   └── flows.py
│   ├── config.py         # Configuration
│   ├── logging_config.py # Structured logging
│   ├── cli.py            # CLI commands
│   └── main.py           # FastAPI app
├── tests/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
└── README.md
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=movie_trends --cov-report=html

# Run specific test file
pytest tests/test_trend_scoring.py

# Run with verbose output
pytest -v
```

## Database Schema

### Raw Layer
- `raw_tmdb_trending`: Raw trending API responses (JSONB)
- `raw_tmdb_movies`: Raw movie details (JSONB)

### Analytics Layer
- `dim_date`: Date dimension
- `dim_genre`: Genre dimension
- `dim_movie`: Movie dimension (SCD Type 2)
- `fact_movie_popularity_daily`: Daily popularity metrics
- `fact_movie_trends_weekly`: Weekly trend scores and classifications

## Configuration

Key environment variables:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname

# TMDb API
TMDB_API_KEY=your_api_key
TMDB_RATE_LIMIT_PER_SECOND=40

# API
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Trend Scoring
RECENCY_LAMBDA_DAYS=75
POPULARITY_WEIGHT=0.6
VOTE_VELOCITY_WEIGHT=0.4
```

## Observability

- **Structured Logging**: JSON logs with correlation IDs
- **Prometheus Metrics**: `/metrics` endpoint
- **Health Checks**: `/health` endpoint
- **Request Tracing**: Built into FastAPI middleware

## Deployment

### Docker Compose (Recommended)

```bash
docker-compose up -d
```

### Kubernetes (Production)

See `k8s/` directory for Kubernetes manifests (coming soon).

## Scheduled Jobs

Use Prefect or cron for scheduling:

```bash
# Daily ingestion (daily at 2 AM)
0 2 * * * docker-compose exec -T api python -m movie_trends.cli ingest-daily

# Weekly trend calculation (Mondays at 3 AM)
0 3 * * 1 docker-compose exec -T api python -m movie_trends.cli calculate-trends
```

## Performance

- **Rate Limiting**: Built-in token bucket for TMDb API
- **Async I/O**: Full async/await support
- **Connection Pooling**: PostgreSQL connection pool
- **Caching**: Redis integration ready
- **Batch Processing**: Efficient batch trend calculation

## Contributing

1. Follow PEP 8 style guide
2. Add type hints to all functions
3. Write tests for new features
4. Update documentation
5. Use conventional commits

## License

MIT License - see LICENSE file

## Credits

- TMDb API for movie data
- FastAPI framework
- Prefect for orchestration

## Support

For issues and questions:
- GitHub Issues: [Create an issue]
- Documentation: See `/docs` endpoint

---

**Built with ❤️ using modern Python best practices**
