
CREATE TABLE IF NOT EXISTS machine_metrics (
    event_id UUID PRIMARY KEY,
    machine_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    vibration_mm_s DOUBLE PRECISION NOT NULL,
    rolling_avg_vibration DOUBLE PRECISION NOT NULL,
    sample_count INTEGER NOT NULL,
    alert_status TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_machine_time
    ON machine_metrics (machine_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_metrics_processed
    ON machine_metrics (processed_at DESC);
