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

INSERT INTO toots
(toot_id, created_at, user_id, username, content, language, hashtags, favourites_count, reblogs_count)
VALUES
('1001', NOW(), 'u1', 'alice',
 'I love Apache Spark and Data Science',
 'en', ARRAY['Spark','DataScience'], 12, 3),

('1002', NOW() - INTERVAL '1 hour', 'u2', 'bob',
 'Learning Kafka with Spark streaming',
 'en', ARRAY['Kafka','Spark'], 8, 2),

('1003', NOW() - INTERVAL '1 day', 'u1', 'alice',
 'AI and machine learning are interesting',
 'en', ARRAY['AI','MachineLearning'], 20, 5),

('1004', NOW() - INTERVAL '1 day', 'u3', 'charlie',
 'PostgreSQL works great with Spark',
 'en', ARRAY['PostgreSQL','Spark'], 5, 1),

('1005', NOW() - INTERVAL '2 days', 'u2', 'bob',
 'Data engineering project with Mastodon',
 'en', ARRAY['DataEngineering','Mastodon'], 15, 4)

ON CONFLICT (toot_id) DO NOTHING;