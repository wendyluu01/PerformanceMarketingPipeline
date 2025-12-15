-- Create databases
CREATE DATABASE airflow;
CREATE DATABASE marketing;

-- Airflow user permissions
GRANT ALL PRIVILEGES ON DATABASE airflow TO admin;
GRANT ALL PRIVILEGES ON DATABASE marketing TO admin;

-- Marketing schema
\c marketing;

CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    cost DECIMAL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON raw_events (ts);
CREATE INDEX IF NOT EXISTS idx_raw_events_campaign_ts ON raw_events (campaign_id, ts);

CREATE TABLE IF NOT EXISTS campaign_metrics_hourly (
    campaign_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    impressions INT DEFAULT 0,
    clicks INT DEFAULT 0,
    signups INT DEFAULT 0,
    cost DECIMAL NOT NULL,
    ctr DECIMAL NOT NULL,
    click_to_signup DECIMAL NOT NULL,
    signup_to_activation DECIMAL NOT NULL,
    total_cost DECIMAL DEFAULT 0,
    PRIMARY KEY (campaign_id, channel, hour)
);

CREATE TABLE IF NOT EXISTS campaign_metrics_daily (
    campaign_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    day DATE NOT NULL,
    impressions INT DEFAULT 0,
    clicks INT DEFAULT 0,
    signups INT DEFAULT 0,
    cost DECIMAL NOT NULL,
    ctr DECIMAL NOT NULL,
    click_to_signup DECIMAL NOT NULL,
    signup_to_activation DECIMAL NOT NULL,
    total_cost DECIMAL DEFAULT 0,
    PRIMARY KEY (campaign_id, channel, day)
);

CREATE TABLE alerts (
    alert_id SERIAL PRIMARY KEY,
    triggered_at TIMESTAMPTZ NOT NULL,
    campaign_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    metric TEXT NOT NULL,
    current_value DECIMAL NOT NULL,
    baseline_value DECIMAL NOT NULL,
    threshold_value DECIMAL NOT NULL,
    message TEXT NOT NULL,
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alerts_triggered_at ON alerts (triggered_at);

