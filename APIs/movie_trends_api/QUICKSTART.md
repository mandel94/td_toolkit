# Quick Start Guide

## 1. Setup Environment

```bash
# Navigate to project
cd movie_trends_api

# Copy environment template
cp .env.example .env

# Edit .env and add your TMDb API key
# Get one at: https://www.themoviedb.org/settings/api
```

## 2. Start with Docker (Recommended)

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f api

# Check service health
curl http://localhost:8000/health
```

## 3. Initialize & Load Data

```bash
# Initialize database schema
docker-compose exec api python -m movie_trends.cli init-db

# Run initial data pipeline (ingest + calculate trends)
docker-compose exec api python -m movie_trends.cli run-pipeline

# This will:
# 1. Fetch trending movies from TMDb
# 2. Store raw data in PostgreSQL
# 3. Calculate trend scores
# 4. Make data available via API
```

## 4. Use the API

### Interactive Documentation
Open http://localhost:8000/docs in your browser

### Example Requests

**Get Trending Movies**
```bash
curl http://localhost:8000/v1/trends/movies?limit=10
```

**Get Specific Movie Trend**
```bash
curl http://localhost:8000/v1/trends/movies/550
```

**Compare Movies**
```bash
curl http://localhost:8000/v1/trends/compare?ids=550,551,552
```

**Filter by Classification**
```bash
curl "http://localhost:8000/v1/trends/movies?classification=EMERGING&limit=20"
```

## 5. View Metrics & Monitoring

- **API Metrics**: http://localhost:8000/metrics
- **Prefect UI**: http://localhost:4200
- **Prometheus**: http://localhost:9090

## 6. Schedule Periodic Updates

### Option A: Docker Compose with Cron

Add to your crontab:
```bash
# Daily ingestion at 2 AM
0 2 * * * cd /path/to/movie_trends_api && docker-compose exec -T api python -m movie_trends.cli ingest-daily

# Weekly trend calculation on Mondays at 3 AM
0 3 * * 1 cd /path/to/movie_trends_api && docker-compose exec -T api python -m movie_trends.cli calculate-trends
```

### Option B: Prefect Deployments

```python
from movie_trends.orchestration import daily_ingestion_flow

# Create deployment
deployment = daily_ingestion_flow.to_deployment(
    name="daily-ingestion",
    cron="0 2 * * *",  # Daily at 2 AM
)
deployment.apply()
```

## 7. Common Commands

```bash
# View API logs
docker-compose logs -f api

# Restart API service
docker-compose restart api

# Access database
docker-compose exec postgres psql -U postgres movie_trends

# Run tests
docker-compose exec api pytest

# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## 8. Troubleshooting

**API not starting?**
- Check logs: `docker-compose logs api`
- Verify database is running: `docker-compose ps postgres`
- Ensure TMDb API key is set in `.env`

**No trend data?**
- Run pipeline: `docker-compose exec api python -m movie_trends.cli run-pipeline`
- Check ingestion logs: `docker-compose logs ingestion-worker`

**Database connection errors?**
- Wait for database to be healthy: `docker-compose ps`
- Check DATABASE_URL in `.env`

## Next Steps

1. **Customize trend formula**: Edit `movie_trends/services/trend_scoring.py`
2. **Add more data sources**: Extend `movie_trends/clients/`
3. **Create dashboards**: Use API endpoints with your favorite visualization tool
4. **Set up monitoring**: Configure Prometheus alerts

For detailed documentation, see:
- [README.md](README.md)
- [TECHNICAL_DOCS.md](TECHNICAL_DOCS.md)
- API Docs: http://localhost:8000/docs
