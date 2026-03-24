PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS brands (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    name                    TEXT NOT NULL,
    website_url             TEXT,
    instagram_handle        TEXT,
    category                TEXT CHECK(category IN ('skincare','supplements','fashion','food','wellness')),
    revenue_band            TEXT,
    meta_ad_spend_estimate  REAL,
    is_client               INTEGER NOT NULL DEFAULT 0 CHECK(is_client IN (0,1)),
    created_at              TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id        INTEGER NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
    ad_library_id   TEXT NOT NULL UNIQUE,
    creative_type   TEXT CHECK(creative_type IN ('static','carousel','video','reel')),
    ad_copy         TEXT,
    cta_type        TEXT,
    image_path      TEXT,
    thumbnail_url   TEXT,
    start_date      TEXT,
    last_seen_date  TEXT,
    duration_days   INTEGER GENERATED ALWAYS AS (
                        CASE
                            WHEN start_date IS NOT NULL AND last_seen_date IS NOT NULL
                            THEN CAST(julianday(last_seen_date) - julianday(start_date) AS INTEGER)
                            ELSE NULL
                        END
                    ) VIRTUAL,
    caption         TEXT,
    transcript      TEXT,
    frames_path     TEXT,
    video_url       TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0,1)),
    scraped_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ad_analysis (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_id                 INTEGER NOT NULL REFERENCES ads(id) ON DELETE CASCADE,
    psychological_trigger TEXT CHECK(psychological_trigger IN (
                              'status','fear','social_proof','transformation',
                              'agitation_solution','curiosity','urgency',
                              'authority','belonging','aspiration'
                          )),
    visual_layout         TEXT,
    copy_tone             TEXT,
    reading_level         TEXT,
    color_palette_json    TEXT,
    is_profitable         INTEGER CHECK(is_profitable IN (0,1)),
    analysis_json         TEXT,
    analyzed_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS competitor_sets (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    client_brand_id      INTEGER NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
    competitor_brand_id  INTEGER NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
    created_at           TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(client_brand_id, competitor_brand_id)
);

CREATE TABLE IF NOT EXISTS creative_concepts (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    client_brand_id          INTEGER NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
    batch_id                 TEXT NOT NULL,
    hook_text                TEXT,
    body_script              TEXT,
    visual_direction         TEXT,
    cta_variations_json      TEXT,
    psychological_angle      TEXT,
    source_competitor_ad_id  INTEGER REFERENCES ads(id) ON DELETE SET NULL,
    generated_at             TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS waste_reports (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    client_brand_id          INTEGER NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
    creative_diversity_score REAL CHECK(creative_diversity_score BETWEEN 0 AND 100),
    format_mix_json          TEXT,
    avg_refresh_days         REAL,
    fatigue_flags_json       TEXT,
    recommendations_json     TEXT,
    generated_at             TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS performance_data (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    creative_concept_id   INTEGER REFERENCES creative_concepts(id) ON DELETE SET NULL,
    ad_id                 INTEGER REFERENCES ads(id) ON DELETE SET NULL,
    ctr                   REAL,
    cpa                   REAL,
    roas                  REAL,
    impressions           INTEGER,
    spend                 REAL,
    date_range_start      TEXT,
    date_range_end        TEXT,
    imported_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS instagram_profiles (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    brand_id          INTEGER REFERENCES brands(id) ON DELETE SET NULL,
    handle            TEXT NOT NULL UNIQUE,
    display_name      TEXT,
    bio               TEXT,
    follower_count    INTEGER,
    post_count        INTEGER,
    engagement_rate   REAL,
    profile_pic_url   TEXT,
    recent_posts_json TEXT,
    scraped_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_ads_brand_id      ON ads(brand_id);
CREATE INDEX IF NOT EXISTS idx_ads_is_active     ON ads(is_active);
CREATE INDEX IF NOT EXISTS idx_analysis_ad_id    ON ad_analysis(ad_id);
CREATE INDEX IF NOT EXISTS idx_concepts_client   ON creative_concepts(client_brand_id);
CREATE INDEX IF NOT EXISTS idx_concepts_batch    ON creative_concepts(batch_id);
CREATE INDEX IF NOT EXISTS idx_waste_client      ON waste_reports(client_brand_id);
CREATE INDEX IF NOT EXISTS idx_perf_concept      ON performance_data(creative_concept_id);
CREATE INDEX IF NOT EXISTS idx_perf_ad           ON performance_data(ad_id);
CREATE INDEX IF NOT EXISTS idx_ig_brand_id       ON instagram_profiles(brand_id);
