CREATE TABLE IF NOT EXISTS toots (
    id BIGSERIAL PRIMARY KEY,
    toot_id VARCHAR(100) UNIQUE,
    created_at TIMESTAMP,
    user_id VARCHAR(100),
    username VARCHAR(255),
    content TEXT,
    language VARCHAR(20),
    hashtags TEXT[],
    favourites_count INTEGER DEFAULT 0,
    reblogs_count INTEGER DEFAULT 0
);