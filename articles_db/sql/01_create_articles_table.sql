-- Create the articles table with proper data science conventions
-- Column names use snake_case and are descriptive
-- Data types are appropriate for analytics and ML workflows

CREATE TABLE IF NOT EXISTS articles (
    id SERIAL PRIMARY KEY,
    page_path VARCHAR(1024) UNIQUE, -- Using page_path as the unique identifier
    title VARCHAR(500) DEFAULT 'N/A',
    author VARCHAR(255) DEFAULT 'N/A',
    categoria VARCHAR(100),
    screen_page_views INTEGER DEFAULT 0,
    engaged_sessions INTEGER DEFAULT 0,
    sessions INTEGER DEFAULT 0,
    engagement_rate DECIMAL(5,4) DEFAULT 0.0000,  -- Stores rates as decimals (e.g., 0.1234 for 12.34%)
    average_session_duration DECIMAL(10,4) DEFAULT 0.0000,  -- Duration in seconds with precision
    publication_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_articles_publication_date ON articles(publication_date);
CREATE INDEX IF NOT EXISTS idx_articles_categoria ON articles(categoria);
CREATE INDEX IF NOT EXISTS idx_articles_author ON articles(author);
CREATE INDEX IF NOT EXISTS idx_articles_engagement_rate ON articles(engagement_rate);
CREATE INDEX IF NOT EXISTS idx_articles_screen_page_views ON articles(screen_page_views);

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_articles_updated_at 
    BEFORE UPDATE ON articles 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Add comments to document the table and columns
COMMENT ON TABLE articles IS 'Articles table storing content analytics data for taxi drivers website';
COMMENT ON COLUMN articles.id IS 'Primary key, auto-incrementing identifier';
COMMENT ON COLUMN articles.title IS 'Article title, up to 500 characters';
COMMENT ON COLUMN articles.author IS 'Article author name';
COMMENT ON COLUMN articles.categoria IS 'Article category/classification';
COMMENT ON COLUMN articles.screen_page_views IS 'Number of page views for the article';
COMMENT ON COLUMN articles.engaged_sessions IS 'Number of engaged sessions for the article';
COMMENT ON COLUMN articles.sessions IS 'Total number of sessions for the article';
COMMENT ON COLUMN articles.engagement_rate IS 'Engagement rate as decimal (0-1)';
COMMENT ON COLUMN articles.average_session_duration IS 'Average session duration in seconds';
COMMENT ON COLUMN articles.postgres_data IS 'Date when the article was published';
COMMENT ON COLUMN articles.created_at IS 'Timestamp when record was created';
COMMENT ON COLUMN articles.updated_at IS 'Timestamp when record was last updated';