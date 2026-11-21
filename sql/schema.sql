
CREATE TABLE IF NOT EXISTS machine_events (
    event_id UUID PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    machine_type TEXT NOT NULL,
    operating_state TEXT NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    vibration_mm_s DOUBLE PRECISION NOT NULL,
    rpm INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_machine_events_time
    ON machine_events (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_machine_events_machine_time
    ON machine_events (machine_id, event_time DESC);
