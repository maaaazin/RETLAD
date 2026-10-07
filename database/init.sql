CREATE TABLE IF NOT EXISTS request_metrics (
    window_start TIMESTAMPTZ PRIMARY KEY,
    window_end TIMESTAMPTZ NOT NULL,
    total_requests INTEGER NOT NULL,
    total_errors INTEGER NOT NULL,
    error_rate_percent NUMERIC(7, 2) NOT NULL,
    average_response_time_ms NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS endpoint_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    endpoint TEXT NOT NULL,
    total_requests INTEGER NOT NULL,
    total_errors INTEGER NOT NULL,
    average_response_time_ms NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (window_start, endpoint)
);

CREATE TABLE IF NOT EXISTS ip_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    ip_address INET NOT NULL,
    request_count INTEGER NOT NULL,
    PRIMARY KEY (window_start, ip_address)
);

CREATE TABLE IF NOT EXISTS anomalies (
    alert_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_start TIMESTAMPTZ NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('warning', 'critical')),
    ip_address INET,
    endpoint TEXT,
    metric_value NUMERIC(12, 2) NOT NULL,
    threshold_value NUMERIC(12, 2) NOT NULL,
    description TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_endpoint_metrics_endpoint ON endpoint_metrics (endpoint);
