-- Initialize PostgreSQL database for Quiver development
-- This script creates sample tables and data for testing

-- Create sample user features table
CREATE TABLE IF NOT EXISTS user_features_age (
    entity_id VARCHAR(255) NOT NULL,
    age INTEGER,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id, feature_ts)
);

CREATE TABLE IF NOT EXISTS user_features_score (
    entity_id VARCHAR(255) NOT NULL,
    score DOUBLE PRECISION,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id, feature_ts)
);

CREATE TABLE IF NOT EXISTS user_features_country (
    entity_id VARCHAR(255) NOT NULL,
    country TEXT,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (entity_id, feature_ts)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_user_age_ts ON user_features_age(entity_id, feature_ts DESC);
CREATE INDEX IF NOT EXISTS idx_user_score_ts ON user_features_score(entity_id, feature_ts DESC);
CREATE INDEX IF NOT EXISTS idx_user_country_ts ON user_features_country(entity_id, feature_ts DESC);

-- Insert sample data
INSERT INTO user_features_age (entity_id, age, feature_ts) VALUES
    ('user_1', 25, NOW() - INTERVAL '1 hour'),
    ('user_1', 26, NOW()),
    ('user_2', 32, NOW() - INTERVAL '2 hours'),
    ('user_2', 32, NOW()),
    ('user_3', 28, NOW())
ON CONFLICT (entity_id, feature_ts) DO NOTHING;

INSERT INTO user_features_score (entity_id, score, feature_ts) VALUES
    ('user_1', 85.5, NOW() - INTERVAL '1 hour'),
    ('user_1', 87.2, NOW()),
    ('user_2', 92.3, NOW() - INTERVAL '2 hours'),
    ('user_2', 91.8, NOW()),
    ('user_3', 78.9, NOW())
ON CONFLICT (entity_id, feature_ts) DO NOTHING;

INSERT INTO user_features_country (entity_id, country, feature_ts) VALUES
    ('user_1', 'US', NOW() - INTERVAL '1 week'),
    ('user_2', 'UK', NOW() - INTERVAL '1 week'),
    ('user_3', 'CA', NOW() - INTERVAL '1 week')
ON CONFLICT (entity_id, feature_ts) DO NOTHING;

-- Create a view for easier querying (optional)
CREATE OR REPLACE VIEW user_features_summary AS
SELECT DISTINCT ON (entity_id)
    entity_id,
    age.age,
    score.score,
    country.country,
    GREATEST(age.feature_ts, score.feature_ts, country.feature_ts) as last_updated
FROM user_features_age age
FULL OUTER JOIN user_features_score score USING (entity_id)
FULL OUTER JOIN user_features_country country USING (entity_id)
ORDER BY entity_id, last_updated DESC;