-- SQL schema for storing scraped article content

-- Table for storing raw scraped article content
CREATE TABLE IF NOT EXISTS scraped_articles_raw (
    scrape_id SERIAL PRIMARY KEY,
    page_path VARCHAR(1024) NOT NULL,
    url VARCHAR(2048) NOT NULL,
    title VARCHAR(500),
    subtitle TEXT,
    author VARCHAR(255),
    category VARCHAR(100),
    publication_date DATE,
    published_text VARCHAR(100),
    body_html TEXT,
    body_text TEXT,
    archive_scraped_at TIMESTAMP WITH TIME ZONE,
    detail_scraped_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_scraped_articles_page_path ON scraped_articles_raw(page_path);
CREATE INDEX IF NOT EXISTS idx_scraped_articles_publication_date ON scraped_articles_raw(publication_date);
CREATE INDEX IF NOT EXISTS idx_scraped_articles_author ON scraped_articles_raw(author);
CREATE INDEX IF NOT EXISTS idx_scraped_articles_category ON scraped_articles_raw(category);
CREATE INDEX IF NOT EXISTS idx_scraped_articles_created_at ON scraped_articles_raw(created_at);

-- Create trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_scraped_articles_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_scraped_articles_updated_at
    BEFORE UPDATE ON scraped_articles_raw
    FOR EACH ROW
    EXECUTE FUNCTION update_scraped_articles_updated_at();

-- Create a view for easy querying of latest scrapes per article
CREATE OR REPLACE VIEW latest_scraped_articles AS
SELECT DISTINCT ON (page_path)
    scrape_id,
    page_path,
    url,
    title,
    subtitle,
    author,
    category,
    publication_date,
    published_text,
    body_html,
    body_text,
    archive_scraped_at,
    detail_scraped_at,
    created_at,
    updated_at
FROM scraped_articles_raw
ORDER BY page_path, created_at DESC;

COMMENT ON TABLE scraped_articles_raw IS 'Raw scraped article data from TaxiDrivers.it website';
COMMENT ON VIEW latest_scraped_articles IS 'Latest scraped version of each article (by page_path)';
