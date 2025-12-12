# Articles Analytics API

Backend API for Taxi Drivers content analytics platform, built with FastAPI and SQLAlchemy using Object-Oriented Programming principles.

## 🏗️ Architecture

### Design Patterns
- **Layered Architecture**: Clear separation of concerns
  - `models.py`: SQLAlchemy ORM models (Data Layer)
  - `services.py`: Business logic (Service Layer)
  - `main.py`: API endpoints (Presentation Layer)
  - `database.py`: Database connection management
  - `config.py`: Configuration and settings

- **Dependency Injection**: FastAPI's dependency system for database sessions
- **Repository Pattern**: Service classes encapsulate data access logic
- **Pydantic Schemas**: Request/response validation and serialization

### Project Structure
```
api/
├── __init__.py           # Package initialization
├── main.py               # FastAPI application and endpoints
├── config.py             # Configuration management
├── database.py           # Database connection and session
├── models.py             # SQLAlchemy ORM models
├── schemas.py            # Pydantic request/response schemas
├── services.py           # Business logic layer
├── requirements.txt      # Python dependencies
├── .env.example          # Environment variables template
└── README.md             # This file
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL database running (see `docker-compose.yml` in parent directory)
- Virtual environment tool (venv, conda, etc.)

### Installation

1. **Navigate to API directory**
```bash
cd databases/articles_db/api
```

2. **Create virtual environment**
```bash
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment**
```bash
# Copy example env file
cp .env.example .env

# Edit .env with your database credentials
```

5. **Start the API**
```bash
# Development mode with auto-reload
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# Or run directly
python -m api.main
```

6. **Access the API**
- API: http://localhost:8000
- Interactive docs (Swagger): http://localhost:8000/docs
- Alternative docs (ReDoc): http://localhost:8000/redoc

## 📚 API Endpoints

### Analytics Endpoints

#### 1. Top Performing Articles
```http
GET /api/v1/analytics/top-articles
```

**Query Parameters:**
- `limit` (int, default=50): Number of articles to return
- `category_id` (int, optional): Filter by category
- `author_id` (int, optional): Filter by author
- `start_date` (date, optional): Filter by publication start date
- `end_date` (date, optional): Filter by publication end date

**Response:** List of articles with aggregated metrics

**Business Use Case:** Identify content that drives highest engagement

#### 2. Author Performance
```http
GET /api/v1/analytics/author-performance
```

**Response:** Performance metrics per author

**Business Use Case:** Author performance scorecards for reviews and incentives

#### 3. Category Performance
```http
GET /api/v1/analytics/category-performance
```

**Response:** Performance metrics per category

**Business Use Case:** Portfolio management and resource allocation decisions

#### 4. Engagement Trends
```http
GET /api/v1/analytics/engagement-trends
```

**Query Parameters:**
- `article_id` (int, optional): Specific article to analyze
- `limit` (int, default=100): Maximum data points

**Response:** Time series engagement data

**Business Use Case:** Content lifecycle analysis and refresh timing

### Dimension Endpoints

#### 5. Get Authors
```http
GET /api/v1/dimensions/authors
```

**Response:** List of all authors

#### 6. Get Categories
```http
GET /api/v1/dimensions/categories
```

**Response:** List of all categories

#### 7. Get Article by ID
```http
GET /api/v1/articles/{article_id}
```

**Response:** Article details

## 🎯 Business Use Cases Implemented

| Use Case | Endpoint | Description |
|----------|----------|-------------|
| 1. Top Performing Articles | `/top-articles` | Identify highest engagement content |
| 3. Engagement Trends | `/engagement-trends` | Monitor content lifecycle |
| 5. Author Performance | `/author-performance` | Performance scorecards |
| 7. Category Portfolio | `/category-performance` | Portfolio management |

**See:** `BUSINESS_USE_CASES.md` for complete use case documentation

## 🔧 Configuration

### Environment Variables

Create a `.env` file from `.env.example`:

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=articles_db
DB_USER=postgres
DB_PASSWORD=your_password

# API
API_TITLE=Articles Analytics API
CORS_ORIGINS=http://localhost:3000,http://localhost:8000
```

### Database Connection

The API connects to PostgreSQL using SQLAlchemy. Configuration is managed through the `Settings` class in `config.py` with support for environment variables.

**Connection pooling:**
- Pool size: 5 connections
- Max overflow: 10 connections
- Pre-ping enabled for connection health checks

## 🧪 Testing

### Manual Testing

Use the interactive Swagger docs at `/docs`:
1. Start the API
2. Open http://localhost:8000/docs
3. Try out endpoints with the "Try it out" button

### Example API Calls

```bash
# Get top 10 articles
curl "http://localhost:8000/api/v1/analytics/top-articles?limit=10"

# Get top articles in specific category
curl "http://localhost:8000/api/v1/analytics/top-articles?category_id=1&limit=20"

# Get author performance
curl "http://localhost:8000/api/v1/analytics/author-performance"

# Get engagement trends for an article
curl "http://localhost:8000/api/v1/analytics/engagement-trends?article_id=123"
```

## 📊 Data Models

### ORM Models (models.py)

- **DimAuthor**: Authors dimension
- **DimCategory**: Categories dimension
- **DimWeek**: Week dimension (year, week, date range)
- **DimArticle**: Article master data
- **FactWeeklyMetrics**: Weekly performance metrics (fact table)

### Pydantic Schemas (schemas.py)

- **TopArticleResponse**: Top articles with aggregated metrics
- **AuthorPerformanceResponse**: Author performance data
- **CategoryPerformanceResponse**: Category performance data
- **EngagementTrendResponse**: Time series engagement data

## 🛠️ Development

### Adding New Endpoints

1. **Define Pydantic schema** in `schemas.py`
```python
class NewResponse(BaseModel):
    field1: str
    field2: int
```

2. **Add service method** in `services.py`
```python
def get_new_data(self):
    return self.db.query(Model).all()
```

3. **Create endpoint** in `main.py`
```python
@app.get("/api/v1/new-endpoint")
def new_endpoint(db: Session = Depends(get_db)):
    service = AnalyticsService(db)
    return service.get_new_data()
```

### Code Style

- Follow PEP 8 guidelines
- Use type hints for all functions
- Add docstrings to classes and methods
- Keep business logic in service layer

## 🐳 Docker Deployment

### Option 1: Add to existing docker-compose.yml

```yaml
  articles_api:
    build: ./api
    container_name: articles_api
    restart: unless-stopped
    environment:
      DB_HOST: articles_postgres
      DB_PORT: 5432
      DB_NAME: articles_db
      DB_USER: postgres
      DB_PASSWORD: postgres123
    ports:
      - "8000:8000"
    depends_on:
      - articles_postgres
    networks:
      - articles_network
```

### Option 2: Standalone Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## 📈 Performance Considerations

- **Connection Pooling**: Managed by SQLAlchemy
- **Query Optimization**: Indexes on foreign keys and date columns
- **Pagination**: Limit query results to prevent memory issues
- **Caching**: Consider adding Redis for frequently accessed data

## 🔒 Security

- Environment variables for sensitive data
- Input validation with Pydantic
- SQL injection prevention via SQLAlchemy ORM
- CORS configuration for frontend access

## 📝 Logging

Logging configured at INFO level. Logs include:
- Service method calls
- Error details
- Query result counts

## 🚦 Next Steps

- [ ] Add authentication (JWT tokens)
- [ ] Implement caching layer (Redis)
- [ ] Add more use case endpoints
- [ ] Create unit tests with pytest
- [ ] Set up CI/CD pipeline
- [ ] Add rate limiting
- [ ] Create frontend dashboard

## 📞 Support

For questions or issues:
- Review API docs at `/docs`
- Check `BUSINESS_USE_CASES.md` for context
- See `MVP_BACKEND_IMPLEMENTATION_PLAN.md` for roadmap

---

**Version:** 1.0.0  
**Last Updated:** December 10, 2025
