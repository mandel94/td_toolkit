# Architecture Overview

## System Context

The Movie Trends API is a data product that transforms raw movie data from TMDb into actionable trend insights through a multi-layered architecture.

```mermaid
C4Context
    title System Context - Movie Trends Data Product
    
    Person(analyst, "Data Analyst", "Analyzes movie trends")
    Person(editor, "Content Editor", "Creates movie content")
    Person(developer, "Developer", "Integrates API")
    
    System(api, "Movie Trends API", "Trend scoring and analytics platform")
    
    System_Ext(tmdb, "TMDb API", "Movie database")
    System_Ext(monitoring, "Prometheus", "Metrics monitoring")
    
    Rel(analyst, api, "Queries trends", "HTTP/REST")
    Rel(editor, api, "Discovers content", "HTTP/REST")
    Rel(developer, api, "Integrates", "HTTP/REST")
    
    Rel(api, tmdb, "Fetches movie data", "HTTP")
    Rel(api, monitoring, "Exports metrics", "HTTP")
    
    UpdateRelStyle(analyst, api, $textColor="blue", $lineColor="blue")
    UpdateRelStyle(editor, api, $textColor="green", $lineColor="green")
    UpdateRelStyle(api, tmdb, $textColor="red", $lineColor="red")
```

## Architectural Layers

The system follows a clean, layered architecture with clear separation of concerns:

```mermaid
graph TB
    subgraph "🌐 Presentation Layer"
        REST[REST API Endpoints]
        DOCS[OpenAPI Documentation]
        HEALTH[Health Checks]
    end
    
    subgraph "🎯 Application Layer"
        ROUTES[API Routes]
        DEPS[Dependency Injection]
        MIDDLEWARE[Middleware Chain]
    end
    
    subgraph "💼 Business Logic Layer"
        INGEST[Ingestion Service]
        SCORING[Trend Scoring Engine]
        TRANSFORM[Transformation Service]
    end
    
    subgraph "🔌 Infrastructure Layer"
        REPOS[Repositories]
        TMDB_CLIENT[TMDb Client]
        CACHE[Redis Cache]
    end
    
    subgraph "💾 Data Layer"
        POSTGRES[(PostgreSQL)]
        REDIS[(Redis)]
    end
    
    REST --> ROUTES
    DOCS --> ROUTES
    HEALTH --> ROUTES
    
    ROUTES --> DEPS
    DEPS --> MIDDLEWARE
    
    MIDDLEWARE --> INGEST
    MIDDLEWARE --> SCORING
    MIDDLEWARE --> TRANSFORM
    
    INGEST --> REPOS
    SCORING --> REPOS
    TRANSFORM --> REPOS
    
    REPOS --> TMDB_CLIENT
    REPOS --> CACHE
    
    TMDB_CLIENT --> POSTGRES
    CACHE --> REDIS
    REPOS --> POSTGRES
    
    style REST fill:#4CAF50
    style INGEST fill:#FF6B6B
    style SCORING fill:#FFD93D
    style REPOS fill:#6BCF7F
    style POSTGRES fill:#336791
    style REDIS fill:#DC382D
```

## Core Principles

### 1. **Async-First Design**
All I/O operations are asynchronous for maximum throughput:

```python
# Concurrent API requests
async with TMDbClient() as client:
    movies = await client.get_trending("week")
    
# Non-blocking database queries
async with AsyncSession() as session:
    result = await session.execute(query)
```

### 2. **Repository Pattern**
Data access is abstracted behind repositories:

```mermaid
classDiagram
    class MovieRepository {
        +get_by_id(movie_id)
        +upsert(movie_data)
        +search(filters)
    }
    
    class PopularityRepository {
        +insert_snapshot(data)
        +get_time_series(movie_id, period)
        +get_latest()
    }
    
    class RawDataRepository {
        +store_response(endpoint, data)
        +get_by_batch_id(batch_id)
    }
    
    class TrendRepository {
        +get_trending(window, limit)
        +get_by_movie(movie_id)
        +compute_and_store()
    }
    
    MovieRepository --|> BaseRepository
    PopularityRepository --|> BaseRepository
    RawDataRepository --|> BaseRepository
    TrendRepository --|> BaseRepository
```

### 3. **Dependency Injection**
Services receive dependencies through constructors:

```python
class TrendScoringService:
    def __init__(
        self,
        movie_repo: MovieRepository,
        popularity_repo: PopularityRepository
    ):
        self.movie_repo = movie_repo
        self.popularity_repo = popularity_repo
```

### 4. **Type Safety**
Complete type coverage with Pydantic schemas:

```mermaid
graph LR
    REQUEST[HTTP Request] --> VALIDATION[Pydantic Validation]
    VALIDATION --> SERVICE[Service Layer]
    SERVICE --> DB[Database Models]
    DB --> SERIALIZATION[Pydantic Serialization]
    SERIALIZATION --> RESPONSE[HTTP Response]
    
    style VALIDATION fill:#4CAF50
    style SERIALIZATION fill:#4CAF50
```

## Service Architecture

```mermaid
graph TB
    subgraph "External Interface"
        API_GATEWAY[API Gateway / Load Balancer]
    end
    
    subgraph "API Container"
        FASTAPI[FastAPI Application]
        ROUTER[Route Handlers]
        SERVICE_LAYER[Service Layer]
    end
    
    subgraph "Worker Container"
        CLI[CLI Commands]
        ORCHESTRATION[Prefect Flows]
        BACKGROUND[Background Jobs]
    end
    
    subgraph "Data Services"
        TMDB_SERVICE[TMDb Client Service]
        CACHE_SERVICE[Cache Service]
        DB_SERVICE[Database Service]
    end
    
    subgraph "Storage"
        POSTGRES[(PostgreSQL<br/>Primary Database)]
        REDIS[(Redis<br/>Cache Layer)]
    end
    
    API_GATEWAY --> FASTAPI
    FASTAPI --> ROUTER
    ROUTER --> SERVICE_LAYER
    
    CLI --> ORCHESTRATION
    ORCHESTRATION --> BACKGROUND
    
    SERVICE_LAYER --> TMDB_SERVICE
    SERVICE_LAYER --> CACHE_SERVICE
    SERVICE_LAYER --> DB_SERVICE
    
    BACKGROUND --> TMDB_SERVICE
    BACKGROUND --> DB_SERVICE
    
    TMDB_SERVICE --> POSTGRES
    CACHE_SERVICE --> REDIS
    DB_SERVICE --> POSTGRES
    
    style FASTAPI fill:#009688
    style ORCHESTRATION fill:#673AB7
    style POSTGRES fill:#336791
    style REDIS fill:#DC382D
```

## Component Interaction

### Request Lifecycle

```mermaid
sequenceDiagram
    participant Client
    participant FastAPI
    participant RouteHandler
    participant Service
    participant Repository
    participant Database
    participant Cache
    
    Client->>FastAPI: GET /v1/trends/movies
    FastAPI->>RouteHandler: Route to handler
    RouteHandler->>RouteHandler: Validate request params
    RouteHandler->>Service: get_trending_movies()
    
    Service->>Cache: Check cache
    alt Cache Hit
        Cache-->>Service: Return cached data
    else Cache Miss
        Service->>Repository: query_trends()
        Repository->>Database: SELECT with filters
        Database-->>Repository: Result set
        Repository-->>Service: Trend objects
        Service->>Cache: Store in cache
    end
    
    Service->>Service: Apply business logic
    Service-->>RouteHandler: TrendResponse
    RouteHandler->>RouteHandler: Serialize to JSON
    RouteHandler-->>FastAPI: Response model
    FastAPI-->>Client: JSON response
```

### Data Ingestion Flow

```mermaid
sequenceDiagram
    participant Scheduler
    participant IngestionService
    participant TMDbClient
    participant RawRepo
    participant MovieRepo
    participant PopRepo
    participant ScoringService
    
    Scheduler->>IngestionService: ingest_trending_data()
    IngestionService->>TMDbClient: get_trending(time_window)
    
    loop For each page
        TMDbClient->>TMDbClient: HTTP request to TMDb
        TMDbClient-->>IngestionService: Movie list
        IngestionService->>RawRepo: store_response()
        
        loop For each movie
            IngestionService->>MovieRepo: upsert_movie()
            IngestionService->>PopRepo: insert_snapshot()
        end
    end
    
    IngestionService->>ScoringService: calculate_trends()
    ScoringService->>PopRepo: get_time_series_data()
    PopRepo-->>ScoringService: Historical metrics
    ScoringService->>ScoringService: Compute trend scores
    ScoringService-->>IngestionService: Trend results
    
    IngestionService-->>Scheduler: Ingestion complete
```

## Scalability Design

```mermaid
graph TB
    subgraph "Current: Single Instance"
        LB1[Load Balancer]
        API1[API Container]
        WORKER1[Worker Container]
        DB1[(PostgreSQL)]
        CACHE1[(Redis)]
        
        LB1 --> API1
        API1 --> DB1
        API1 --> CACHE1
        WORKER1 --> DB1
    end
    
    subgraph "Future: Horizontal Scaling"
        LB2[Load Balancer]
        API2A[API Instance 1]
        API2B[API Instance 2]
        API2C[API Instance 3]
        WORKER2A[Worker 1]
        WORKER2B[Worker 2]
        DB2_PRIMARY[(PostgreSQL<br/>Primary)]
        DB2_REPLICA[(PostgreSQL<br/>Read Replica)]
        CACHE2[Redis Cluster]
        
        LB2 --> API2A
        LB2 --> API2B
        LB2 --> API2C
        
        API2A --> CACHE2
        API2B --> CACHE2
        API2C --> CACHE2
        
        API2A --> DB2_REPLICA
        API2B --> DB2_REPLICA
        API2C --> DB2_REPLICA
        
        WORKER2A --> DB2_PRIMARY
        WORKER2B --> DB2_PRIMARY
        
        DB2_PRIMARY -.Replication.-> DB2_REPLICA
    end
    
    style API1 fill:#009688
    style API2A fill:#009688
    style API2B fill:#009688
    style API2C fill:#009688
    style DB2_PRIMARY fill:#336791
    style DB2_REPLICA fill:#5C8DB8
```

## Technology Decisions

### Why FastAPI?
- **Performance**: Built on Starlette (async ASGI framework)
- **Developer Experience**: Auto-generated OpenAPI docs
- **Type Safety**: Native Pydantic integration
- **Modern**: Python 3.12+ with async/await

### Why PostgreSQL?
- **JSONB Support**: Store raw TMDb responses
- **Time-Series**: Efficient historical data queries
- **ACID Compliance**: Reliable transactions
- **Window Functions**: Complex analytical queries

### Why Redis?
- **Speed**: Sub-millisecond response times
- **TTL Support**: Automatic cache expiration
- **Atomic Operations**: Thread-safe increments
- **Pub/Sub**: Future real-time features

## Security Architecture

```mermaid
graph TB
    subgraph "External Threats"
        DDOS[DDoS Attacks]
        INJECTION[SQL Injection]
        XSS[XSS Attacks]
    end
    
    subgraph "Defense Layers"
        RATE_LIMIT[Rate Limiting]
        VALIDATION[Input Validation]
        PARAMETERIZED[Parameterized Queries]
        SANITIZATION[Output Sanitization]
    end
    
    subgraph "Application"
        FASTAPI_APP[FastAPI Application]
        SQLALCHEMY[SQLAlchemy ORM]
    end
    
    subgraph "Data Protection"
        SECRETS[Secret Management]
        ENCRYPTION[TLS Encryption]
        ACCESS_CONTROL[Access Control]
    end
    
    DDOS -.Blocked by.-> RATE_LIMIT
    INJECTION -.Blocked by.-> VALIDATION
    XSS -.Blocked by.-> SANITIZATION
    
    RATE_LIMIT --> FASTAPI_APP
    VALIDATION --> FASTAPI_APP
    PARAMETERIZED --> SQLALCHEMY
    SANITIZATION --> FASTAPI_APP
    
    FASTAPI_APP --> SECRETS
    FASTAPI_APP --> ENCRYPTION
    SQLALCHEMY --> ACCESS_CONTROL
    
    style RATE_LIMIT fill:#4CAF50
    style VALIDATION fill:#4CAF50
    style PARAMETERIZED fill:#4CAF50
    style SANITIZATION fill:#4CAF50
```

## Next Steps

- [System Design Details](system-design.md) - Component breakdown
- [Data Flow Architecture](data-flow.md) - Data movement patterns
- [API Layer Architecture](api-layer.md) - REST API design
