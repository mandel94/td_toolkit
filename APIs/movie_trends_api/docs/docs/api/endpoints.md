# API Endpoints Reference

Complete REST API documentation for the Movie Trends Data Product.

---

## Base URL

```
http://localhost:8000/v1
```

---

## Authentication

Currently, the API does not require authentication (development mode). For production, implement API key authentication.

```http
# Future authentication header
Authorization: Bearer YOUR_API_KEY
```

---

## Endpoints Overview

```mermaid
graph LR
    API[/v1] --> TRENDS[/trends]
    TRENDS --> MOVIES[/movies]
    TRENDS --> MOVIE_ID[/movies/{id}]
    TRENDS --> COMPARE[/compare]
    
    API --> HEALTH[/health]
    API --> METRICS[/metrics]
    
    style TRENDS fill:#4CAF50
    style HEALTH fill:#2196F3
    style METRICS fill:#FF9800
```

---

## GET /v1/trends/movies

Retrieve trending movies with calculated trend scores.

### Request

```http
GET /v1/trends/movies?time_window=weekly&limit=20&min_score=40
```

### Query Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `time_window` | string | No | `weekly` | Time window: `daily` or `weekly` |
| `limit` | integer | No | `20` | Maximum results to return (1-100) |
| `min_score` | float | No | `0` | Minimum trend score filter |
| `genre` | string | No | - | Filter by genre (e.g., `Action`, `Drama`) |
| `release_year` | integer | No | - | Filter by release year |

### Response

```json
{
  "meta": {
    "api_version": "v1",
    "time_window": "weekly",
    "as_of": "2025-12-21T10:30:00Z",
    "trend_definition_version": "1.0",
    "total_results": 20,
    "query_time_ms": 45
  },
  "data": [
    {
      "movie": {
        "movie_id": 558449,
        "title": "Gladiator II",
        "original_title": "Gladiator II",
        "release_date": "2024-11-13",
        "release_year": 2024,
        "genres": ["Action", "Adventure", "Drama"],
        "overview": "Years after witnessing the death of...",
        "poster_path": "/2cxhvwyEwRlysAmRH4iodkvo0z5.jpg",
        "backdrop_path": "/3TSydGr2k4x4fHPVcYfNbCjU8e0.jpg",
        "original_language": "en",
        "adult": false
      },
      "trend_metrics": {
        "trend_score": 87.3,
        "trend_classification": "VIRAL",
        "popularity_growth": 0.45,
        "vote_velocity": 0.38,
        "norm_popularity_growth": 0.82,
        "norm_vote_velocity": 0.73,
        "recency_factor": 0.92,
        "stability_factor": 0.94,
        "volatility": 0.06
      },
      "current_metrics": {
        "popularity": 3456.789,
        "vote_count": 1250,
        "vote_average": 7.8,
        "snapshot_date": "2025-12-21"
      },
      "trend_history": {
        "previous_score": 72.1,
        "score_delta": 15.2,
        "percentile_rank": 95
      }
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 20,
    "total_pages": 5,
    "total_items": 100
  }
}
```

### Status Codes

| Code | Description |
|------|-------------|
| `200` | Success |
| `400` | Invalid query parameters |
| `429` | Rate limit exceeded |
| `500` | Internal server error |

### Example Usage

```python
import requests

response = requests.get(
    "http://localhost:8000/v1/trends/movies",
    params={
        "time_window": "weekly",
        "limit": 10,
        "min_score": 70,
        "genre": "Action"
    }
)

data = response.json()
for item in data["data"]:
    movie = item["movie"]
    metrics = item["trend_metrics"]
    print(f"{movie['title']}: {metrics['trend_score']:.1f} ({metrics['trend_classification']})")
```

---

## GET /v1/trends/movies/{movie_id}

Get detailed trend information for a specific movie.

### Request

```http
GET /v1/trends/movies/558449
```

### Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `movie_id` | integer | TMDb movie ID |

### Query Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `include_history` | boolean | No | `false` | Include time-series history |
| `days_back` | integer | No | `30` | Days of history to include |

### Response

```json
{
  "movie": {
    "movie_id": 558449,
    "title": "Gladiator II",
    "release_date": "2024-11-13",
    "genres": ["Action", "Adventure", "Drama"]
  },
  "current_trend": {
    "trend_score": 87.3,
    "trend_classification": "VIRAL",
    "as_of": "2025-12-21T10:30:00Z"
  },
  "trend_components": {
    "popularity_growth": {
      "value": 0.45,
      "normalized": 0.82,
      "weight": 0.6,
      "contribution": 0.49
    },
    "vote_velocity": {
      "value": 0.38,
      "normalized": 0.73,
      "weight": 0.4,
      "contribution": 0.29
    },
    "recency_factor": {
      "value": 0.92,
      "days_since_release": 38
    },
    "stability_factor": {
      "value": 0.94,
      "volatility": 0.06
    }
  },
  "time_series": [
    {
      "date": "2025-12-14",
      "popularity": 2450.3,
      "vote_count": 1050,
      "trend_score": 72.1
    },
    {
      "date": "2025-12-21",
      "popularity": 3456.8,
      "vote_count": 1250,
      "trend_score": 87.3
    }
  ],
  "insights": {
    "momentum": "accelerating",
    "peak_trend_date": "2025-12-21",
    "days_trending": 15,
    "percentile_rank": 95
  }
}
```

### Status Codes

| Code | Description |
|------|-------------|
| `200` | Success |
| `404` | Movie not found |
| `500` | Internal server error |

---

## GET /v1/trends/compare

Compare trend metrics across multiple movies.

### Request

```http
GET /v1/trends/compare?ids=558449,567,890
```

### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `ids` | string | Yes | Comma-separated movie IDs (max 10) |
| `metric` | string | No | Focus metric: `trend_score`, `popularity_growth`, `vote_velocity` |

### Response

```json
{
  "meta": {
    "comparison_date": "2025-12-21T10:30:00Z",
    "movie_count": 3
  },
  "movies": [
    {
      "movie_id": 558449,
      "title": "Gladiator II",
      "trend_score": 87.3,
      "trend_classification": "VIRAL",
      "rank": 1
    },
    {
      "movie_id": 567,
      "title": "Another Movie",
      "trend_score": 65.2,
      "trend_classification": "EMERGING",
      "rank": 2
    }
  ],
  "comparison": {
    "highest_score": {
      "movie_id": 558449,
      "value": 87.3
    },
    "highest_growth": {
      "movie_id": 558449,
      "value": 0.45
    },
    "average_score": 76.2
  }
}
```

---

## GET /health

Health check endpoint for monitoring.

### Request

```http
GET /health
```

### Response

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "timestamp": "2025-12-21T10:30:00Z",
  "checks": {
    "database": "ok",
    "redis": "ok",
    "tmdb_api": "ok"
  }
}
```

---

## GET /metrics

Prometheus metrics endpoint.

### Request

```http
GET /metrics
```

### Response

```
# HELP http_requests_total Total HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="GET",endpoint="/v1/trends/movies"} 1234

# HELP http_request_duration_seconds HTTP request duration
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.1"} 950
http_request_duration_seconds_bucket{le="0.5"} 1200
```

---

## Error Responses

### Standard Error Format

```json
{
  "error": {
    "code": "INVALID_PARAMETER",
    "message": "Parameter 'limit' must be between 1 and 100",
    "details": {
      "parameter": "limit",
      "provided": 150,
      "max_allowed": 100
    },
    "timestamp": "2025-12-21T10:30:00Z",
    "request_id": "req_abc123"
  }
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `INVALID_PARAMETER` | 400 | Invalid query parameter |
| `MISSING_PARAMETER` | 400 | Required parameter missing |
| `NOT_FOUND` | 404 | Resource not found |
| `RATE_LIMIT_EXCEEDED` | 429 | Too many requests |
| `INTERNAL_ERROR` | 500 | Internal server error |
| `SERVICE_UNAVAILABLE` | 503 | Service temporarily unavailable |

---

## Rate Limiting

| Tier | Requests/Minute | Requests/Hour |
|------|-----------------|---------------|
| **Development** | 60 | 3,600 |
| **Production** | 300 | 18,000 |

Rate limit headers:
```http
X-RateLimit-Limit: 300
X-RateLimit-Remaining: 245
X-RateLimit-Reset: 1640095200
```

---

## Pagination

For endpoints returning lists:

```json
{
  "pagination": {
    "page": 2,
    "per_page": 20,
    "total_pages": 10,
    "total_items": 200,
    "next": "/v1/trends/movies?page=3",
    "previous": "/v1/trends/movies?page=1"
  }
}
```

---

## Filtering & Sorting

### Supported Filters

- `genre` - Filter by genre
- `release_year` - Filter by year
- `min_score` - Minimum trend score
- `classification` - `VIRAL`, `EMERGING`, `STEADY`, `DECLINING`

### Sorting

```http
GET /v1/trends/movies?sort=-trend_score,release_date
```

Prefix with `-` for descending order.

---

## Next Steps

- [Response Schemas](schemas.md) - Detailed Pydantic models
- [Usage Examples](examples.md) - Real-world integration patterns
- [Architecture](../architecture/api-layer.md) - API design principles
