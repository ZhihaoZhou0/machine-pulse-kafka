
import os
import time
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
    # Anchor historical filters to the last stored event, not today's clock.
    # Otherwise charts would become empty days after deployment.
    reference_time = latest_event_time if is_snapshot else now
    cutoff = reference_time - timedelta(minutes=int(selected_minutes))

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
        "Machines (last 5 recorded min)" if is_snapshot else "Active Machines (5 min)",
        int(active_machines),
    )
    col3.metric(
        "Avg Temp (last recorded min)" if is_snapshot else "Avg Temperature (1 min)",
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

    st.caption(
        f"Latest recorded event (UTC): {latest_event_time} | "
        f"Page refresh (UTC): {now} | "
        f"Mode: {'snapshot' if is_snapshot else 'live PostgreSQL'}"
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
