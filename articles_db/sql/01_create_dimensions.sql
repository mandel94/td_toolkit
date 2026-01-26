-- 01_create_dimensions.sql

-- Dimension table for Authors
CREATE TABLE IF NOT EXISTS dim_authors (
    author_id SERIAL PRIMARY KEY,
    author_name VARCHAR(255) UNIQUE NOT NULL
);

-- Insert a default 'N/A' author
INSERT INTO dim_authors (author_name) VALUES ('N/A') ON CONFLICT (author_name) DO NOTHING;

-- Dimension table for Categories
CREATE TABLE IF NOT EXISTS dim_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL
);

-- Insert a default 'Uncategorized' category
INSERT INTO dim_categories (category_name) VALUES ('Uncategorized') ON CONFLICT (category_name) DO NOTHING;


-- Dimension table for Dates
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INT PRIMARY KEY, -- YYYYMMDD format
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    week_of_year INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Dimension table for Articles
CREATE TABLE IF NOT EXISTS dim_articles (
    article_id SERIAL PRIMARY KEY,
    page_path VARCHAR(1024) UNIQUE NOT NULL,
    title VARCHAR(500),
    publication_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
