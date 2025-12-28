-- Create articles_db database initialization script
-- This script will be executed when the PostgreSQL container starts for the first time

-- Set timezone
SET timezone = 'UTC';

-- Create the articles table with data science naming conventions
CREATE TABLE IF NOT EXISTS articles (
    article_id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    author VARCHAR(200),
    category VARCHAR(100),
    screen_page_views INTEGER DEFAULT 0,
    sessions INTEGER DEFAULT 0,
    engaged_sessions INTEGER DEFAULT 0,
    engagement_rate DECIMAL(5,4) DEFAULT 0.0000, -- Percentage as decimal (0.0000 to 1.0000)
    average_session_duration DECIMAL(10,2) DEFAULT 0.00, -- Duration in seconds with 2 decimal places
    publication_date DATE,
    page_path VARCHAR(1000), -- Store the original page path for reference
    url VARCHAR(1000), -- Full URL for reference
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category);
CREATE INDEX IF NOT EXISTS idx_articles_publication_date ON articles(publication_date);
CREATE INDEX IF NOT EXISTS idx_articles_screen_page_views ON articles(screen_page_views);
CREATE INDEX IF NOT EXISTS idx_articles_engagement_rate ON articles(engagement_rate);
CREATE INDEX IF NOT EXISTS idx_articles_created_at ON articles(created_at);

-- Add constraints
ALTER TABLE articles 
ADD CONSTRAINT chk_engagement_rate CHECK (engagement_rate >= 0 AND engagement_rate <= 1),
ADD CONSTRAINT chk_screen_page_views CHECK (screen_page_views >= 0),
ADD CONSTRAINT chk_engaged_sessions CHECK (engaged_sessions >= 0),
ADD CONSTRAINT chk_sessions CHECK (sessions >= 0),
ADD CONSTRAINT chk_average_session_duration CHECK (average_session_duration >= 0);

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at on record updates
CREATE TRIGGER update_articles_updated_at 
    BEFORE UPDATE ON articles 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create a view for analytics queries
CREATE OR REPLACE VIEW articles_analytics AS
SELECT 
    article_id,
    title,
    author,
    category,
    screen_page_views,
    engaged_sessions,
    sessions,
    engagement_rate,
    average_session_duration,
    publication_date,
    CASE 
        WHEN engaged_sessions > 0 THEN (engaged_sessions::DECIMAL / sessions::DECIMAL)
        ELSE 0 
    END as calculated_engagement_rate,
    CASE 
        WHEN publication_date IS NOT NULL THEN 
            CURRENT_DATE - publication_date 
        ELSE NULL 
    END as days_since_publication,
    created_at,
    updated_at
FROM articles;

-- Grant permissions (optional, for development)
-- GRANT ALL PRIVILEGES ON DATABASE articles_db TO postgres;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;

-- Insert sample data for testing (optional)
-- INSERT INTO articles (title, author, category, screen_page_views, engaged_sessions, sessions, engagement_rate, average_session_duration, publication_date, page_path, url) 
-- VALUES 
-- ('Sample Article 1', 'John Doe', 'News', 1500, 800, 1200, 0.6667, 120.50, '2025-11-15', '/sample-article-1.html', 'https://taxidrivers.it/sample-article-1.html'),
-- ('Sample Article 2', 'Jane Smith', 'Reviews', 2300, 1100, 1800, 0.6111, 95.75, '2025-11-14', '/sample-article-2.html', 'https://taxidrivers.it/sample-article-2.html');

COMMIT;