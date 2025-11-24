-- Insert sample data for testing purposes
-- This file runs after table creation

INSERT INTO articles (
    title, 
    author, 
    categoria, 
    screen_page_views, 
    engaged_sessions, 
    sessions, 
    engagement_rate, 
    average_session_duration, 
    publication_date
) VALUES 
    (
        'Sample Article: Understanding Taxi Industry Trends',
        'John Doe',
        'Industry Analysis',
        1250,
        89,
        125,
        0.712,
        245.67,
        '2025-01-15'
    ),
    (
        'New Regulations for Taxi Drivers in 2025',
        'Jane Smith',
        'Regulations',
        2100,
        145,
        198,
        0.732,
        198.45,
        '2025-02-01'
    ),
    (
        'Electric Vehicles: The Future of Taxi Services',
        'Mike Johnson',
        'Technology',
        1850,
        134,
        176,
        0.761,
        289.12,
        '2025-02-15'
    )
ON CONFLICT DO NOTHING;