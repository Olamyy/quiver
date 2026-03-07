-- Initialize PostgreSQL database for Quiver development
-- This script creates per-feature tables that match the postgres.yaml table_template configuration
-- Table pattern: user_features_{feature_name}

-- User Demographics Features (from user_demographics view)
CREATE TABLE IF NOT EXISTS user_features_age (
    user_id VARCHAR(255) NOT NULL,
    age INTEGER,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, feature_ts)
);

CREATE TABLE IF NOT EXISTS user_features_country (
    user_id VARCHAR(255) NOT NULL,
    country TEXT,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, feature_ts)
);

CREATE TABLE IF NOT EXISTS user_features_registration_date (
    user_id VARCHAR(255) NOT NULL,
    registration_date TIMESTAMPTZ,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, feature_ts)
);

-- User Scores Features (from user_scores view)
CREATE TABLE IF NOT EXISTS user_features_credit_score (
    user_id VARCHAR(255) NOT NULL,
    credit_score DOUBLE PRECISION,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, feature_ts)
);

CREATE TABLE IF NOT EXISTS user_features_engagement_score (
    user_id VARCHAR(255) NOT NULL,
    engagement_score DOUBLE PRECISION,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, feature_ts)
);

CREATE TABLE IF NOT EXISTS user_features_last_updated (
    user_id VARCHAR(255) NOT NULL,
    last_updated TIMESTAMPTZ,
    feature_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, feature_ts)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_age_user_ts ON user_features_age(user_id, feature_ts DESC);
CREATE INDEX IF NOT EXISTS idx_country_user_ts ON user_features_country(user_id, feature_ts DESC);
CREATE INDEX IF NOT EXISTS idx_reg_date_user_ts ON user_features_registration_date(user_id, feature_ts DESC);
CREATE INDEX IF NOT EXISTS idx_credit_score_user_ts ON user_features_credit_score(user_id, feature_ts DESC);
CREATE INDEX IF NOT EXISTS idx_engagement_score_user_ts ON user_features_engagement_score(user_id, feature_ts DESC);
CREATE INDEX IF NOT EXISTS idx_last_updated_user_ts ON user_features_last_updated(user_id, feature_ts DESC);

-- Insert seed data that matches the expected schema
-- User Demographics
INSERT INTO user_features_age (user_id, age, feature_ts) VALUES
    ('user_1', 25, NOW() - INTERVAL '1 hour'),
    ('user_1', 25, NOW()),
    ('user_2', 32, NOW() - INTERVAL '2 hours'),
    ('user_2', 32, NOW()),
    ('user_3', 28, NOW())
ON CONFLICT (user_id, feature_ts) DO NOTHING;

INSERT INTO user_features_country (user_id, country, feature_ts) VALUES
    ('user_1', 'US', NOW() - INTERVAL '1 hour'),
    ('user_1', 'US', NOW()),
    ('user_2', 'UK', NOW() - INTERVAL '2 hours'),
    ('user_2', 'UK', NOW()),
    ('user_3', 'CA', NOW())
ON CONFLICT (user_id, feature_ts) DO NOTHING;

INSERT INTO user_features_registration_date (user_id, registration_date, feature_ts) VALUES
    ('user_1', '2023-01-15T10:00:00Z', NOW() - INTERVAL '1 hour'),
    ('user_1', '2023-01-15T10:00:00Z', NOW()),
    ('user_2', '2022-08-20T14:30:00Z', NOW() - INTERVAL '2 hours'),
    ('user_2', '2022-08-20T14:30:00Z', NOW()),
    ('user_3', '2023-03-10T09:15:00Z', NOW())
ON CONFLICT (user_id, feature_ts) DO NOTHING;

-- User Scores
INSERT INTO user_features_credit_score (user_id, credit_score, feature_ts) VALUES
    ('user_1', 750.5, NOW() - INTERVAL '1 hour'),
    ('user_1', 752.1, NOW()),
    ('user_2', 680.3, NOW() - INTERVAL '2 hours'),
    ('user_2', 685.7, NOW()),
    ('user_3', 720.9, NOW())
ON CONFLICT (user_id, feature_ts) DO NOTHING;

INSERT INTO user_features_engagement_score (user_id, engagement_score, feature_ts) VALUES
    ('user_1', 85.2, NOW() - INTERVAL '1 hour'),
    ('user_1', 87.4, NOW()),
    ('user_2', 92.1, NOW() - INTERVAL '2 hours'),
    ('user_2', 91.8, NOW()),
    ('user_3', 78.6, NOW())
ON CONFLICT (user_id, feature_ts) DO NOTHING;

INSERT INTO user_features_last_updated (user_id, last_updated, feature_ts) VALUES
    ('user_1', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '1 hour'),
    ('user_1', NOW(), NOW()),
    ('user_2', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours'),
    ('user_2', NOW(), NOW()),
    ('user_3', NOW(), NOW())
ON CONFLICT (user_id, feature_ts) DO NOTHING;

-- Optional: Create views for easier data inspection
CREATE OR REPLACE VIEW user_demographics_view AS
SELECT DISTINCT ON (a.user_id)
    a.user_id,
    a.age,
    c.country,
    r.registration_date,
    GREATEST(a.feature_ts, c.feature_ts, r.feature_ts) as latest_ts
FROM user_features_age a
FULL OUTER JOIN user_features_country c ON a.user_id = c.user_id
FULL OUTER JOIN user_features_registration_date r ON a.user_id = r.user_id
ORDER BY a.user_id, GREATEST(COALESCE(a.feature_ts, '1900-01-01'::timestamptz), 
                             COALESCE(c.feature_ts, '1900-01-01'::timestamptz),
                             COALESCE(r.feature_ts, '1900-01-01'::timestamptz)) DESC;

CREATE OR REPLACE VIEW user_scores_view AS
SELECT DISTINCT ON (cs.user_id)
    cs.user_id,
    cs.credit_score,
    es.engagement_score,
    lu.last_updated,
    GREATEST(cs.feature_ts, es.feature_ts, lu.feature_ts) as latest_ts
FROM user_features_credit_score cs
FULL OUTER JOIN user_features_engagement_score es ON cs.user_id = es.user_id
FULL OUTER JOIN user_features_last_updated lu ON cs.user_id = lu.user_id
ORDER BY cs.user_id, GREATEST(COALESCE(cs.feature_ts, '1900-01-01'::timestamptz),
                              COALESCE(es.feature_ts, '1900-01-01'::timestamptz),
                              COALESCE(lu.feature_ts, '1900-01-01'::timestamptz)) DESC;