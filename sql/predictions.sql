
CREATE TABLE IF NOT EXISTS machine_predictions (
    event_id UUID PRIMARY KEY,
    machine_id TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,

    actual_vibration DOUBLE PRECISION NOT NULL,
    predicted_vibration DOUBLE PRECISION NOT NULL,
    absolute_error DOUBLE PRECISION NOT NULL,
    anomaly_threshold DOUBLE PRECISION NOT NULL,

    is_anomaly BOOLEAN NOT NULL,
    model_name TEXT NOT NULL DEFAULT 'Ridge-AR6',

    predicted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_predictions_machine_time
    ON machine_predictions (machine_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_predictions_anomaly
    ON machine_predictions (is_anomaly, event_time DESC);

-- Durable per-machine AR(6) history. Updated in the SAME transaction as
-- machine_predictions, so crashes cannot advance the model state without
-- committing the prediction result (or vice versa).
CREATE TABLE IF NOT EXISTS machine_prediction_state (
    machine_id TEXT PRIMARY KEY,
    last_event_id UUID NOT NULL,
    last_event_time TIMESTAMPTZ NOT NULL,
    recent_readings JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
