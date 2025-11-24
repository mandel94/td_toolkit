-- Utility views and functions for data analysis

-- Create a view for articles with calculated metrics
CREATE OR REPLACE VIEW articles_analytics AS
SELECT 
    id,
    title,
    author,
    categoria,
    screen_page_views,
    engaged_sessions,
    sessions,
    engagement_rate,
    average_session_duration,
    publication_date,
    -- Calculated metrics
    CASE 
        WHEN sessions > 0 THEN ROUND((engaged_sessions::DECIMAL / sessions::DECIMAL) * 100, 2)
        ELSE 0 
    END as calculated_engagement_rate_percent,
    CASE 
        WHEN screen_page_views > 0 THEN ROUND(sessions::DECIMAL / screen_page_views::DECIMAL, 4)
        ELSE 0 
    END as session_to_pageview_ratio,
    EXTRACT(MONTH FROM publication_date) as publication_month,
    EXTRACT(YEAR FROM publication_date) as publication_year,
    EXTRACT(DOW FROM publication_date) as publication_day_of_week,
    DATE_PART('day', CURRENT_DATE - publication_date) as days_since_publication,
    created_at,
    updated_at
FROM articles;

-- Create a function to get top performing articles
CREATE OR REPLACE FUNCTION get_top_articles_by_metric(
    metric_name VARCHAR(50) DEFAULT 'screen_page_views',
    limit_count INTEGER DEFAULT 10,
    category_filter VARCHAR(100) DEFAULT NULL
)
RETURNS TABLE (
    id INTEGER,
    title VARCHAR(500),
    author VARCHAR(255),
    categoria VARCHAR(100),
    metric_value DECIMAL,
    publication_date DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF category_filter IS NOT NULL THEN
        CASE metric_name
            WHEN 'screen_page_views' THEN
                RETURN QUERY
                SELECT a.id, a.title, a.author, a.categoria, 
                       a.screen_page_views::DECIMAL, a.publication_date
                FROM articles a
                WHERE a.categoria = category_filter
                ORDER BY a.screen_page_views DESC
                LIMIT limit_count;
            WHEN 'engagement_rate' THEN
                RETURN QUERY
                SELECT a.id, a.title, a.author, a.categoria, 
                       a.engagement_rate, a.publication_date
                FROM articles a
                WHERE a.categoria = category_filter
                ORDER BY a.engagement_rate DESC
                LIMIT limit_count;
            WHEN 'sessions' THEN
                RETURN QUERY
                SELECT a.id, a.title, a.author, a.categoria, 
                       a.sessions::DECIMAL, a.publication_date
                FROM articles a
                WHERE a.categoria = category_filter
                ORDER BY a.sessions DESC
                LIMIT limit_count;
            ELSE
                RAISE EXCEPTION 'Invalid metric name: %', metric_name;
        END CASE;
    ELSE
        CASE metric_name
            WHEN 'screen_page_views' THEN
                RETURN QUERY
                SELECT a.id, a.title, a.author, a.categoria, 
                       a.screen_page_views::DECIMAL, a.publication_date
                FROM articles a
                ORDER BY a.screen_page_views DESC
                LIMIT limit_count;
            WHEN 'engagement_rate' THEN
                RETURN QUERY
                SELECT a.id, a.title, a.author, a.categoria, 
                       a.engagement_rate, a.publication_date
                FROM articles a
                ORDER BY a.engagement_rate DESC
                LIMIT limit_count;
            WHEN 'sessions' THEN
                RETURN QUERY
                SELECT a.id, a.title, a.author, a.categoria, 
                       a.sessions::DECIMAL, a.publication_date
                FROM articles a
                ORDER BY a.sessions DESC
                LIMIT limit_count;
            ELSE
                RAISE EXCEPTION 'Invalid metric name: %', metric_name;
        END CASE;
    END IF;
END;
$$;

COMMENT ON VIEW articles_analytics IS 'Enhanced view of articles with calculated analytics metrics';
COMMENT ON FUNCTION get_top_articles_by_metric IS 'Function to retrieve top performing articles by specified metric';