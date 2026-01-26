# Data Flow Architecture

## End-to-End Data Journey

```mermaid
flowchart TB
    START[Start Pipeline] --> FETCH
    
    subgraph "📥 Ingestion Phase"
        FETCH[Fetch from TMDb API]
        VALIDATE[Validate Response]
        STORE_RAW[Store Raw JSONB]
    end
    
    subgraph "🔄 Transformation Phase"
        EXTRACT[Extract Movie Data]
        NORMALIZE[Normalize Fields]
        ENRICH[Enrich Metadata]
    end
    
    subgraph "💾 Storage Phase"
        UPSERT_MOVIE[Upsert Movie Record]
        INSERT_POP[Insert Popularity Snapshot]
        INDEX[Update Indexes]
    end
    
    subgraph "📊 Analysis Phase"
        QUERY_TS[Query Time Series]
        CALC_GROWTH[Calculate Growth Metrics]
        CALC_VELOCITY[Calculate Vote Velocity]
        APPLY_FACTORS[Apply Modifying Factors]
    end
    
    subgraph "✅ Finalization Phase"
        STORE_TREND[Store Trend Score]
        CACHE[Update Cache]
        LOG[Log Metrics]
    end
    
    FETCH --> VALIDATE
    VALIDATE --> STORE_RAW
    
    STORE_RAW --> EXTRACT
    EXTRACT --> NORMALIZE
    NORMALIZE --> ENRICH
    
    ENRICH --> UPSERT_MOVIE
    UPSERT_MOVIE --> INSERT_POP
    INSERT_POP --> INDEX
    
    INDEX --> QUERY_TS
    QUERY_TS --> CALC_GROWTH
    CALC_GROWTH --> CALC_VELOCITY
    CALC_VELOCITY --> APPLY_FACTORS
    
    APPLY_FACTORS --> STORE_TREND
    STORE_TREND --> CACHE
    CACHE --> LOG
    
    LOG --> END[Pipeline Complete]
    
    style FETCH fill:#4285F4
    style STORE_RAW fill:#FF6B6B
    style CALC_GROWTH fill:#FFD93D
    style STORE_TREND fill:#4CAF50
```

## Phase 1: Data Ingestion

### TMDb API Request Flow

```mermaid
sequenceDiagram
    participant Service as Ingestion Service
    participant Client as TMDb Client
    participant TMDb as TMDb API
    participant RawDB as Raw Data Table
    
    Service->>Client: get_trending("week", page=1)
    Client->>Client: Build request URL
    Client->>Client: Add API key header
    
    Client->>TMDb: GET /trending/movie/week
    TMDb-->>Client: 200 OK + JSON response
    
    Client->>Client: Validate response schema
    Client->>Client: Parse JSON to Pydantic models
    Client-->>Service: List[TMDbMovieDetailed]
    
    Service->>RawDB: INSERT raw_response
    Note over RawDB: Store for audit trail
    
    loop For each movie
        Service->>Service: Process movie data
    end
```

### Data Validation

```mermaid
flowchart LR
    RAW[Raw JSON] --> SCHEMA{Schema<br/>Valid?}
    
    SCHEMA -->|Yes| TYPE{Types<br/>Correct?}
    SCHEMA -->|No| ERROR1[Validation Error]
    
    TYPE -->|Yes| REQUIRED{Required<br/>Fields?}
    TYPE -->|No| ERROR2[Type Error]
    
    REQUIRED -->|Yes| ACCEPT[Accept Data]
    REQUIRED -->|No| ERROR3[Missing Fields]
    
    ERROR1 --> REJECT[Reject & Log]
    ERROR2 --> REJECT
    ERROR3 --> REJECT
    
    style ACCEPT fill:#4CAF50
    style REJECT fill:#F44336
```

## Phase 2: Data Transformation

### Movie Data Normalization

```mermaid
graph TB
    subgraph "Input: TMDb Response"
        TMDB_ID[id: 558449]
        TMDB_TITLE[title: 'Gladiator II']
        TMDB_DATE[release_date: '2024-11-13']
        TMDB_GENRES[genre_ids: [28, 12, 18]]
        TMDB_POP[popularity: 3456.789]
    end
    
    subgraph "Transformation Logic"
        MAP_GENRES[Map genre IDs → names]
        PARSE_DATE[Parse ISO date string]
        NORMALIZE_POP[Normalize popularity score]
        EXTRACT_YEAR[Extract release year]
    end
    
    subgraph "Output: Database Model"
        DB_ID[movie_id: 558449]
        DB_TITLE[title: 'Gladiator II']
        DB_DATE[release_date: Date object]
        DB_GENRES[genres: ['Action', 'Adventure', 'Drama']]
        DB_YEAR[release_year: 2024]
    end
    
    TMDB_ID --> DB_ID
    TMDB_TITLE --> DB_TITLE
    TMDB_DATE --> PARSE_DATE --> DB_DATE
    TMDB_GENRES --> MAP_GENRES --> DB_GENRES
    TMDB_DATE --> EXTRACT_YEAR --> DB_YEAR
    TMDB_POP --> NORMALIZE_POP
    
    style MAP_GENRES fill:#FFD93D
    style PARSE_DATE fill:#FFD93D
    style NORMALIZE_POP fill:#FFD93D
```

### Popularity Snapshot Creation

```mermaid
flowchart TB
    MOVIE[Movie Data] --> EXTRACT
    
    subgraph "Snapshot Creation"
        EXTRACT[Extract Metrics]
        TIMESTAMP[Add Timestamp]
        CALC[Calculate Derived Metrics]
    end
    
    EXTRACT --> POPULARITY[popularity: float]
    EXTRACT --> VOTE_COUNT[vote_count: int]
    EXTRACT --> VOTE_AVG[vote_average: float]
    
    POPULARITY --> TIMESTAMP
    VOTE_COUNT --> TIMESTAMP
    VOTE_AVG --> TIMESTAMP
    
    TIMESTAMP --> CALC
    CALC --> ENGAGEMENT[engagement_score]
    CALC --> MOMENTUM[momentum_indicator]
    
    ENGAGEMENT --> INSERT[INSERT INTO popularity_snapshots]
    MOMENTUM --> INSERT
    POPULARITY --> INSERT
    VOTE_COUNT --> INSERT
    VOTE_AVG --> INSERT
    
    INSERT --> DB[(Database)]
```

## Phase 3: Trend Calculation

### Time Series Analysis

```mermaid
graph TB
    START[Start Trend Calculation] --> WINDOW
    
    subgraph "Data Collection"
        WINDOW[Define Time Windows]
        CURRENT[Current Window: Last 7 days]
        PREVIOUS[Previous Window: Prior 7 days]
        BASELINE[Baseline: 30-day average]
    end
    
    WINDOW --> CURRENT
    WINDOW --> PREVIOUS
    WINDOW --> BASELINE
    
    subgraph "Metric Calculation"
        CURRENT --> CALC_GROWTH[Calculate Popularity Growth]
        PREVIOUS --> CALC_GROWTH
        
        CURRENT --> CALC_VELOCITY[Calculate Vote Velocity]
        BASELINE --> CALC_VELOCITY
        
        CALC_GROWTH --> NORMALIZE_GROWTH[Normalize to 0-1]
        CALC_VELOCITY --> NORMALIZE_VEL[Normalize to 0-1]
    end
    
    subgraph "Factor Application"
        NORMALIZE_GROWTH --> WEIGHTED[Apply Weights]
        NORMALIZE_VEL --> WEIGHTED
        
        WEIGHTED --> RECENCY[Apply Recency Factor]
        RECENCY --> STABILITY[Apply Stability Factor]
        STABILITY --> SCORE[Final Trend Score]
    end
    
    SCORE --> CLASSIFY{Classify<br/>Trend}
    CLASSIFY -->|Score > 80| VIRAL[VIRAL]
    CLASSIFY -->|60-80| EMERGING[EMERGING]
    CLASSIFY -->|40-60| STEADY[STEADY]
    CLASSIFY -->|< 40| DECLINING[DECLINING]
    
    VIRAL --> STORE[(Store Results)]
    EMERGING --> STORE
    STEADY --> STORE
    DECLINING --> STORE
    
    style CALC_GROWTH fill:#FFD93D
    style CALC_VELOCITY fill:#FFD93D
    style SCORE fill:#4CAF50
    style VIRAL fill:#F44336
    style EMERGING fill:#FF9800
    style STEADY fill:#4CAF50
    style DECLINING fill:#9E9E9E
```

### Trend Score Formula Breakdown

```mermaid
graph LR
    subgraph "Raw Metrics"
        POP_CURRENT[Current Popularity: P_c]
        POP_PREVIOUS[Previous Popularity: P_p]
        VOTES_RATE[Vote Rate: V_r]
    end
    
    subgraph "Normalized Components"
        POP_GROWTH["Growth: G = (P_c - P_p) / P_p"]
        VOTE_VEL["Velocity: V = V_r / baseline"]
        NORM_G["Normalized G: N_g ∈ [0,1]"]
        NORM_V["Normalized V: N_v ∈ [0,1]"]
    end
    
    subgraph "Weighted Sum"
        WEIGHTED["W = 0.6 × N_g + 0.4 × N_v"]
    end
    
    subgraph "Modifying Factors"
        RECENCY["R = e^(-days_since_release / 90)"]
        STABILITY["S = 1 - (σ / μ)"]
    end
    
    subgraph "Final Score"
        TREND["Trend = 100 × W × R × S"]
    end
    
    POP_CURRENT --> POP_GROWTH
    POP_PREVIOUS --> POP_GROWTH
    VOTES_RATE --> VOTE_VEL
    
    POP_GROWTH --> NORM_G
    VOTE_VEL --> NORM_V
    
    NORM_G --> WEIGHTED
    NORM_V --> WEIGHTED
    
    WEIGHTED --> TREND
    RECENCY --> TREND
    STABILITY --> TREND
    
    style TREND fill:#4CAF50
    style WEIGHTED fill:#FFD93D
```

## Phase 4: API Response Flow

### Query Execution Path

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Cache
    participant Service
    participant Repo
    participant DB
    
    Client->>API: GET /v1/trends/movies?time_window=weekly
    API->>API: Validate query params
    
    API->>Cache: Check cache key
    
    alt Cache Hit
        Cache-->>API: Cached response
        API-->>Client: 200 OK (cached)
    else Cache Miss
        API->>Service: get_trending_movies(time_window="weekly")
        Service->>Repo: query_trends(filters)
        
        Repo->>DB: SELECT with JOIN
        Note over DB: Query optimized with indexes
        DB-->>Repo: Result rows
        
        Repo->>Repo: Map to domain models
        Repo-->>Service: List[TrendResult]
        
        Service->>Service: Apply business logic
        Service->>Service: Sort by trend_score DESC
        Service-->>API: TrendResponse
        
        API->>Cache: Store in cache (TTL=300s)
        API->>API: Serialize to JSON
        API-->>Client: 200 OK (fresh)
    end
```

### Response Serialization

```mermaid
flowchart TB
    DOMAIN[Domain Models] --> SCHEMA
    
    subgraph "Pydantic Serialization"
        SCHEMA[Response Schema]
        VALIDATE[Validate Types]
        TRANSFORM[Transform Fields]
    end
    
    SCHEMA --> VALIDATE
    VALIDATE --> TRANSFORM
    
    subgraph "JSON Output"
        META[Meta Object]
        DATA[Data Array]
        PAGINATION[Pagination Info]
    end
    
    TRANSFORM --> META
    TRANSFORM --> DATA
    TRANSFORM --> PAGINATION
    
    META --> JSON[JSON Response]
    DATA --> JSON
    PAGINATION --> JSON
    
    JSON --> CLIENT[HTTP Client]
    
    style SCHEMA fill:#4CAF50
    style JSON fill:#2196F3
```

## Data Retention & Archival

```mermaid
graph TB
    subgraph "Hot Data: 30 Days"
        RECENT[Recent Snapshots]
        ACTIVE_TRENDS[Active Trends]
        CACHE_DATA[Cached Responses]
    end
    
    subgraph "Warm Data: 1 Year"
        ARCHIVED_SNAPS[Archived Snapshots]
        HISTORICAL_TRENDS[Historical Trends]
    end
    
    subgraph "Cold Data: Forever"
        RAW_BACKUPS[Raw JSON Backups]
        AUDIT_LOGS[Audit Logs]
    end
    
    RECENT -->|After 30 days| ARCHIVED_SNAPS
    ACTIVE_TRENDS -->|After 30 days| HISTORICAL_TRENDS
    CACHE_DATA -->|After TTL| EVICTED[Evicted]
    
    ARCHIVED_SNAPS -->|After 1 year| RAW_BACKUPS
    HISTORICAL_TRENDS -->|After 1 year| RAW_BACKUPS
    
    style RECENT fill:#4CAF50
    style ARCHIVED_SNAPS fill:#FF9800
    style RAW_BACKUPS fill:#9E9E9E
```

## Error Handling Flow

```mermaid
flowchart TB
    REQUEST[Incoming Request] --> VALIDATE{Valid?}
    
    VALIDATE -->|Yes| PROCESS[Process Request]
    VALIDATE -->|No| ERROR_400[400 Bad Request]
    
    PROCESS --> EXECUTE{Execution<br/>Success?}
    
    EXECUTE -->|Yes| RESPONSE[200 OK Response]
    EXECUTE -->|No| ERROR_TYPE{Error Type?}
    
    ERROR_TYPE -->|Not Found| ERROR_404[404 Not Found]
    ERROR_TYPE -->|Rate Limit| ERROR_429[429 Too Many Requests]
    ERROR_TYPE -->|TMDb API Down| ERROR_503[503 Service Unavailable]
    ERROR_TYPE -->|Other| ERROR_500[500 Internal Server Error]
    
    ERROR_400 --> LOG[Log Error]
    ERROR_404 --> LOG
    ERROR_429 --> LOG
    ERROR_503 --> LOG
    ERROR_500 --> LOG
    
    LOG --> MONITOR[Update Metrics]
    MONITOR --> ALERT{Threshold<br/>Exceeded?}
    
    ALERT -->|Yes| NOTIFY[Send Alert]
    ALERT -->|No| END[End]
    
    RESPONSE --> END
    NOTIFY --> END
    
    style RESPONSE fill:#4CAF50
    style ERROR_400 fill:#FF9800
    style ERROR_404 fill:#FF9800
    style ERROR_500 fill:#F44336
    style ERROR_503 fill:#F44336
```

## Performance Optimization Flow

```mermaid
graph TB
    subgraph "Query Optimization"
        INDEX[Database Indexes]
        MATERIALIZED[Materialized Views]
        PARTITION[Table Partitioning]
    end
    
    subgraph "Caching Strategy"
        REDIS_CACHE[Redis Cache Layer]
        QUERY_CACHE[Query Result Cache]
        RESPONSE_CACHE[Response Cache]
    end
    
    subgraph "Connection Pooling"
        ASYNC_POOL[Async Connection Pool]
        POOL_SIZE[Max Pool Size: 20]
        MIN_CONN[Min Connections: 5]
    end
    
    subgraph "Batch Processing"
        BULK_INSERT[Bulk Insert Operations]
        BATCH_UPDATE[Batch Updates]
        PARALLEL[Parallel Processing]
    end
    
    INDEX --> FAST_QUERY[Fast Query Execution]
    MATERIALIZED --> FAST_QUERY
    PARTITION --> FAST_QUERY
    
    REDIS_CACHE --> REDUCED_LOAD[Reduced DB Load]
    QUERY_CACHE --> REDUCED_LOAD
    RESPONSE_CACHE --> REDUCED_LOAD
    
    ASYNC_POOL --> EFFICIENT_IO[Efficient I/O]
    POOL_SIZE --> EFFICIENT_IO
    MIN_CONN --> EFFICIENT_IO
    
    BULK_INSERT --> THROUGHPUT[High Throughput]
    BATCH_UPDATE --> THROUGHPUT
    PARALLEL --> THROUGHPUT
    
    FAST_QUERY --> PERF[Optimized Performance]
    REDUCED_LOAD --> PERF
    EFFICIENT_IO --> PERF
    THROUGHPUT --> PERF
    
    style PERF fill:#4CAF50
```

## Next Steps

- [System Design](system-design.md) - Component architecture
- [API Layer](api-layer.md) - REST API design
- [How It Works: Pipeline](../how-it-works/pipeline.md) - Detailed pipeline walkthrough
