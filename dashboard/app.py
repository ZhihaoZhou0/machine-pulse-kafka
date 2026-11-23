
import os
import time
import json
from datetime import timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg
import streamlit as st
from dotenv import load_dotenv


load_dotenv()

st.set_page_config(
    page_title="Machine Pulse",
    page_icon="🏭",
    layout="wide",
)

REFRESH_INTERVAL = "3s"
WARNING_THRESHOLD = 3.0
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SNAPSHOT_EVENTS = DATA_DIR / "machine_events.csv"
SNAPSHOT_METRICS = DATA_DIR / "machine_metrics.csv"
SNAPSHOT_PREDICTIONS = DATA_DIR / "machine_predictions.csv"
MODEL_REPORT = DATA_DIR.parent / "modeling" / "model_report.json"


def snapshot_mode():
    """Cloud demo mode when no database credentials are configured."""
    return not (os.getenv("DATABASE_URL") or os.getenv("POSTGRES_PASSWORD"))



def get_connection():
    start = time.perf_counter()

    database_url = os.getenv("DATABASE_URL")

    if database_url:
        conn = psycopg.connect(
            database_url,
            connect_timeout=10,
        )
    else:
        password = os.getenv("POSTGRES_PASSWORD")

        if not password:
            raise RuntimeError(
                "Set DATABASE_URL or POSTGRES_PASSWORD."
            )

        conn = psycopg.connect(
            host="127.0.0.1",
            port=5433,
            dbname="machinepulse",
            user="machinepulse",
            password=password,
            connect_timeout=10,
        )

    elapsed = time.perf_counter() - start

    print(
        f"[Dashboard] PostgreSQL connection: {elapsed:.3f}s",
        flush=True,
    )

    return conn



def read_query(conn, query, params=None):
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [
            column.name for column in cursor.description
        ]

    return pd.DataFrame(rows, columns=columns)



def load_data():
    if snapshot_mode():
        if not SNAPSHOT_EVENTS.is_file() or not SNAPSHOT_METRICS.is_file():
            raise FileNotFoundError(
                "Snapshot CSV files missing under data/. "
                "Commit machine_events.csv and machine_metrics.csv for cloud deployment."
            )
        events = pd.read_csv(SNAPSHOT_EVENTS, encoding="utf-8-sig")
        metrics = pd.read_csv(SNAPSHOT_METRICS, encoding="utf-8-sig")
        return events, metrics, len(events)

    with get_connection() as conn:
        events = read_query(conn, """
            SELECT
                event_id,
                event_time,
                machine_id,
                machine_type,
                operating_state,
                temperature_c,
                vibration_mm_s,
                rpm,
                ingested_at
            FROM machine_events
            ORDER BY event_time DESC
            LIMIT 2000
        """)

        metrics = read_query(conn, """
            SELECT
                event_id,
                machine_id,
                event_time,
                vibration_mm_s,
                rolling_avg_vibration,
                sample_count,
                alert_status,
                processed_at
            FROM machine_metrics
            ORDER BY event_time DESC
            LIMIT 2000
        """)

        total_events = read_query(conn, """
            SELECT COUNT(*) AS total
            FROM machine_events
        """).iloc[0]["total"]

    return events, metrics, int(total_events)



def prepare_timestamps(df):
    if not df.empty:
        df = df.copy()
        df["event_time"] = pd.to_datetime(
            df["event_time"],
            utc=True,
        )
    return df


def break_long_gaps(
    df,
    value_column,
    gap_seconds=15,
):
    """Insert gaps between separate telemetry runs."""
    if df.empty:
        return df.copy()

    result = []

    for _, group in df.groupby("machine_id"):
        group = group.sort_values("event_time").copy()

        previous_time = group["event_time"].shift(1)

        gaps = (
            group["event_time"] - previous_time
        ).dt.total_seconds() > gap_seconds

        for index, row in group.iterrows():
            if gaps.loc[index]:
                gap_row = row.copy()

                gap_row["event_time"] = (
                    previous_time.loc[index]
                    + timedelta(seconds=1)
                )

                gap_row[value_column] = float("nan")
                result.append(gap_row)

            result.append(row)

    return pd.DataFrame(result).sort_values(
        ["machine_id", "event_time"]
    )




def load_predictions(is_snapshot):
    """Read the optional sequential-model output without affecting core charts."""
    if is_snapshot:
        if not SNAPSHOT_PREDICTIONS.is_file():
            return pd.DataFrame()
        frame = pd.read_csv(SNAPSHOT_PREDICTIONS, encoding="utf-8-sig")
    else:
        with get_connection() as conn:
            frame = read_query(conn, """
                SELECT event_id, machine_id, event_time,
                       actual_vibration, predicted_vibration,
                       absolute_error, anomaly_threshold,
                       is_anomaly, model_name, predicted_at
                FROM machine_predictions
                ORDER BY event_time DESC
                LIMIT 2000
            """)
    if frame.empty:
        return frame
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    # PostgreSQL returns booleans, while CSV values may be strings.
    frame["is_anomaly"] = (
        frame["is_anomaly"].astype(str).str.strip().str.lower().isin(["true", "t", "1"])
    )
    for column in ["actual_vibration", "predicted_vibration", "absolute_error", "anomaly_threshold"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def show_predictive_maintenance(predictions, cutoff, is_snapshot):
    """Bonus charts are based on stored model outputs, never recomputed in Streamlit."""
    st.divider()
    st.header("Predictive Maintenance — Sequential Modeling Bonus")
    st.caption(
        "Per-machine Ridge-AR6 models predict each vibration reading using the "
        "previous six readings. Anomalies are prediction-error outliers, "
        "not verified physical machine failures."
    )
    if predictions.empty:
        st.info("No model predictions available yet. Run the prediction consumer, "
                "or export data/machine_predictions.csv for the hosted snapshot.")
        return

    filtered = predictions[predictions["event_time"] >= cutoff].copy()
    if filtered.empty:
        st.info("No predictions in this time range. Select a longer range.")
        return

    total = len(filtered)
    flagged = int(filtered["is_anomaly"].sum())
    mae = float(filtered["absolute_error"].mean())
    machine_count = int(filtered["machine_id"].nunique())
    a, b, c, d = st.columns(4)
    a.metric("Predictions (range)", f"{total:,}")
    b.metric("Model anomaly flags", f"{flagged:,}")
    c.metric("Mean absolute error (range)", f"{mae:.3f} mm/s")
    d.metric("Predicted machines", machine_count)

    available = sorted(filtered["machine_id"].dropna().unique().tolist())
    default_machine = available.index("CNC-02") if "CNC-02" in available else 0
    machine_id = st.selectbox(
        "Inspect machine predictions", available, index=default_machine,
        key="bonus_prediction_machine",
    )
    selected = filtered[filtered["machine_id"] == machine_id].sort_values("event_time")

    st.subheader("Actual vs Predicted Vibration")
    comparison = go.Figure()
    for field, label, color in [
        ("actual_vibration", "Actual", "#2563eb"),
        ("predicted_vibration", "Predicted (Ridge-AR6)", "#ea580c"),
    ]:
        # Gap rows prevent lines from connecting separate producer sessions.
        gapped = break_long_gaps(selected, field, gap_seconds=15)
        comparison.add_trace(go.Scatter(
            x=gapped["event_time"], y=gapped[field],
            name=label, mode="lines+markers", connectgaps=False,
            line=dict(color=color), marker=dict(size=5),
        ))
    anomalies = selected[selected["is_anomaly"]]
    if not anomalies.empty:
        comparison.add_trace(go.Scatter(
            x=anomalies["event_time"], y=anomalies["actual_vibration"],
            name="Model anomaly", mode="markers",
            marker=dict(color="#dc2626", symbol="x", size=12),
        ))
    comparison.update_layout(height=380, xaxis_title="Time (UTC)",
                             yaxis_title="Vibration (mm/s)")
    st.plotly_chart(comparison, width="stretch")

    st.subheader("Prediction Error vs Calibrated Threshold")
    errors = go.Figure()
    for field, label, color, dash in [
        ("absolute_error", "Absolute prediction error", "#6366f1", "solid"),
        ("anomaly_threshold", "Model anomaly threshold", "#dc2626", "dash"),
    ]:
        gapped = break_long_gaps(selected, field, gap_seconds=15)
        errors.add_trace(go.Scatter(
            x=gapped["event_time"], y=gapped[field],
            name=label, mode="lines", connectgaps=False,
            line=dict(color=color, dash=dash),
        ))
    errors.update_layout(height=330, xaxis_title="Time (UTC)",
                         yaxis_title="Absolute error (mm/s)")
    st.plotly_chart(errors, width="stretch")

    st.subheader("Recent Model Anomalies")
    all_anomalies = filtered[filtered["is_anomaly"]].sort_values(
        "event_time", ascending=False
    )
    if all_anomalies.empty:
        st.info("No model anomaly flags in the selected time range.")
    else:
        st.dataframe(all_anomalies[[
            "event_time", "machine_id", "actual_vibration",
            "predicted_vibration", "absolute_error", "anomaly_threshold",
        ]].head(20), hide_index=True, width="stretch")

    if MODEL_REPORT.is_file():
        try:
            report = json.loads(MODEL_REPORT.read_text(encoding="utf-8"))
            entries = report.get("machines", {})
            if entries:
                with st.expander("Chronological holdout evaluation (training-time results)"):
                    st.caption(
                        "70/30 chronological split per machine; normal training subset. "
                        "MAE/RMSE describe one-step forecast errors, not fault-classification accuracy."
                    )
                    evaluation = pd.DataFrame([
                        {"Machine": key,
                         "Holdout examples": value.get("holdout_examples"),
                         "MAE (mm/s)": value.get("holdout_mae_mm_s"),
                         "RMSE (mm/s)": value.get("holdout_rmse_mm_s"),
                         "Anomaly flags": value.get("holdout_anomaly_flags")}
                        for key, value in entries.items()
                    ])
                    st.dataframe(evaluation, hide_index=True, width="stretch")
        except (OSError, json.JSONDecodeError) as exc:
            st.caption(f"Model evaluation report unavailable: {exc}")
    st.caption("Mode: historical CSV snapshot" if is_snapshot else
               "Mode: PostgreSQL (updated by independent Kafka model consumer)")


st.title("🏭 Machine Pulse")
st.caption(
    "Real-Time Industrial Equipment Monitoring | "
    "Apache Kafka + PostgreSQL + Streamlit"
)

st.markdown(
    "Sensor telemetry, rolling vibration analytics, "
    "and simulated equipment alerts."
)

selected_minutes = st.selectbox(
    "Display time range",
    options=[5, 15, 30, 60, 180, 1440],
    index=4,
    format_func=lambda x: f"Last {x} minutes"
    if x < 1440 else "Last 24 hours",
)

st.caption(
    "Local database mode refreshes every 3 seconds. "
    "Hosted snapshot mode shows the latest exported data (not live). "
    "Timestamps are shown in UTC."
)


@st.fragment(run_every=REFRESH_INTERVAL)
def live_dashboard():
    refresh_start = time.perf_counter()

    try:
        query_start = time.perf_counter()
        events, metrics, total_events = load_data()
        query_seconds = time.perf_counter() - query_start

        print(
            f"[Dashboard] Database queries: {query_seconds:.3f}s",
            flush=True,
        )
    except Exception as exc:
        st.error(
            "Unable to load monitoring data. Check PostgreSQL connection "
            "or the exported CSV snapshot files."
        )
        st.exception(exc)
        return

    events = prepare_timestamps(events)
    metrics = prepare_timestamps(metrics)

    if events.empty:
        st.info(
            "No telemetry data yet. "
            "Start the Kafka producer and consumers."
        )
        return

    now = pd.Timestamp.now(tz="UTC")
    latest_event_time = events["event_time"].max()
    is_snapshot = snapshot_mode()
    # Keep truly live windows relative to wall-clock time. When there is no
    # telemetry in the requested window, show the latest recorded time window
    # instead of hiding all charts (common when reviewing a finished demo).
    requested_window = timedelta(minutes=int(selected_minutes))
    stale_window = (now - latest_event_time) > requested_window
    historical_view = is_snapshot or stale_window
    reference_time = latest_event_time if historical_view else now
    cutoff = reference_time - requested_window

    recent_events = events[
        events["event_time"] >= cutoff
    ].copy()

    recent_metrics = metrics[
        metrics["event_time"] >= cutoff
    ].copy()

    data_age = max(
        0,
        (now - latest_event_time).total_seconds(),
    )

    if is_snapshot:
        st.info(
            "Historical snapshot demo — NOT a live Kafka connection. "
            f"Last recorded sensor event (UTC): {latest_event_time}. "
            "Local mode uses Kafka, PostgreSQL, and automatic refresh."
        )
    elif stale_window:
        st.warning(
            f"No new telemetry for {data_age:.0f} seconds. "
            f"The selected {int(selected_minutes)}-minute period has no live events, "
            f"so charts are anchored to the last recorded event ({latest_event_time}). "
            "This is historical data, not a live feed."
        )
    elif data_age > 15:
        st.warning(
            f"No new telemetry for {data_age:.0f} seconds. "
            "Showing stored historical data."
        )

    # Summary KPIs
    active_cutoff = reference_time - timedelta(minutes=5)
    minute_cutoff = reference_time - timedelta(minutes=1)

    active_machines = events.loc[
        events["event_time"] >= active_cutoff,
        "machine_id",
    ].nunique()

    minute_events = events[
        events["event_time"] >= minute_cutoff
    ]

    avg_temp = minute_events["temperature_c"].mean()

    warning_count = int(
        (recent_metrics["alert_status"] == "WARNING").sum()
    )

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Events",
        f"{total_events:,}",
    )
    col2.metric(
        "Machines (last 5 recorded min)" if historical_view else "Active Machines (5 min)",
        int(active_machines),
    )
    col3.metric(
        "Avg Temp (last recorded min)" if historical_view else "Avg Temperature (1 min)",
        f"{avg_temp:.1f} °C"
        if pd.notna(avg_temp) else "N/A",
    )
    col4.metric(
        "Warnings (Selected Range)",
        warning_count,
    )

    st.divider()

    if recent_events.empty:
        st.info(
            "No events in the selected time range. "
            "Choose a longer period or restart the producer."
        )
        return

    # Chart 1: Temperature trends
    st.subheader("Temperature Trends")

    temp_plot_data = break_long_gaps(
        recent_events,
        "temperature_c",
    )

    fig_temp = px.line(
        temp_plot_data,
        x="event_time",
        y="temperature_c",
        color="machine_id",
        markers=True,
        labels={
            "event_time": "Time (UTC)",
            "temperature_c": "Temperature (°C)",
            "machine_id": "Machine",
        },
    )

    fig_temp.update_traces(connectgaps=False)
    fig_temp.update_layout(
        height=380,
        legend_title_text="Machine",
    )
    st.plotly_chart(
        fig_temp,
        width="stretch",
    )

    # Chart 2: Vibration distribution
    st.subheader("Vibration Distribution")

    fig_vibration = px.box(
        recent_events,
        x="machine_id",
        y="vibration_mm_s",
        color="machine_type",
        points="outliers",
        labels={
            "machine_id": "Machine",
            "vibration_mm_s": "Vibration (mm/s)",
            "machine_type": "Machine Type",
        },
    )

    fig_vibration.update_layout(height=380)
    st.plotly_chart(
        fig_vibration,
        width="stretch",
    )

    # Chart 3: Derived rolling vibration
    st.subheader("Rolling Vibration & Alerts")

    if not recent_metrics.empty:
        rolling_plot_data = break_long_gaps(
            recent_metrics,
            "rolling_avg_vibration",
        )

        fig_rolling = px.line(
            rolling_plot_data,
            x="event_time",
            y="rolling_avg_vibration",
            color="machine_id",
            labels={
                "event_time": "Time (UTC)",
                "rolling_avg_vibration":
                    "30-second Rolling Avg (mm/s)",
                "machine_id": "Machine",
            },
        )

        fig_rolling.update_traces(connectgaps=False)
        fig_rolling.add_hline(
            y=WARNING_THRESHOLD,
            line_dash="dash",
            line_color="red",
            annotation_text="Warning threshold",
        )

        warnings = recent_metrics[
            recent_metrics["alert_status"] == "WARNING"
        ]

        if not warnings.empty:
            fig_rolling.add_trace(
                go.Scatter(
                    x=warnings["event_time"],
                    y=warnings["rolling_avg_vibration"],
                    mode="markers",
                    name="WARNING",
                    marker=dict(
                        color="red",
                        size=11,
                        symbol="x",
                    ),
                )
            )

        fig_rolling.update_layout(height=410)
        st.plotly_chart(
            fig_rolling,
            width="stretch",
        )

    else:
        st.info("No derived metrics in this time range.")

    # Alerts detail
    st.subheader("Detected Equipment Alerts")

    if not recent_metrics.empty:
        warning_rows = recent_metrics[
            recent_metrics["alert_status"] == "WARNING"
        ].sort_values(
            "event_time",
            ascending=False,
        )

        if warning_rows.empty:
            st.success(
                "No warning events in the selected time range."
            )
        else:
            st.dataframe(
                warning_rows[
                    [
                        "event_time",
                        "machine_id",
                        "vibration_mm_s",
                        "rolling_avg_vibration",
                        "alert_status",
                    ]
                ],
                width="stretch",
                hide_index=True,
            )

    try:
        prediction_data = load_predictions(is_snapshot)
    except (psycopg.Error, ValueError, OSError, KeyError) as exc:
        st.warning(f"Optional sequential-model data could not be loaded: {exc}")
        prediction_data = pd.DataFrame()
    show_predictive_maintenance(prediction_data, cutoff, is_snapshot)

    st.caption(
        f"Latest recorded event (UTC): {latest_event_time} | "
        f"Page refresh (UTC): {now} | "
        f"Mode: {'snapshot' if is_snapshot else 'PostgreSQL historical view' if historical_view else 'live PostgreSQL'}"
    )

    total_seconds = time.perf_counter() - refresh_start

    st.caption(
        f"Server refresh time: {total_seconds:.2f}s | "
        f"Data loading: {query_seconds:.2f}s"
    )

    print(
        f"[Dashboard] Total server execution: {total_seconds:.3f}s",
        flush=True,
    )


live_dashboard()
