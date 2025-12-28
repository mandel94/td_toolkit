-- 02_create_facts.sql

-- Fact table for weekly article metrics
-- The grain of this table is one row per article per week.
CREATE TABLE IF NOT EXISTS fact_weekly_metrics (
    article_id INT REFERENCES dim_articles(article_id),
    author_id INT REFERENCES dim_authors(author_id),
    category_id INT REFERENCES dim_categories(category_id),
    year INT NOT NULL,
    week_of_year INT NOT NULL,
    -- Metrics for the week
    screen_page_views INT,
    engaged_sessions INT,
    sessions INT,
    engagement_rate DECIMAL(10, 4),
    average_session_duration DECIMAL(10, 4),
    -- Composite primary key to ensure one entry per article per week
    PRIMARY KEY (article_id, year, week_of_year)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_fact_weekly_metrics_year_week ON fact_weekly_metrics(year, week_of_year);
