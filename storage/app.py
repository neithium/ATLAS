import streamlit as st
import clickhouse_connect
import psycopg
import pandas as pd
import plotly.express as px
import os
import time

st.set_page_config(page_title="ATLAS Observability Dashboard", layout="wide")

# Connection specs mapping local envs 
# (these match the supervisord container context where everything is local)
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
POSTGRES_HOST   = os.environ.get("POSTGRES_HOST", "127.0.0.1")
POSTGRES_DB     = os.environ.get("POSTGRES_DB", "atlas_metadata")
POSTGRES_USER   = os.environ.get("POSTGRES_USER", "atlas")
POSTGRES_PASS   = os.environ.get("POSTGRES_PASSWORD", "atlas_secure_pwd")

@st.cache_resource(ttl=60)
def init_ch_client():
    """Initialize ClickHouse connection with a short TTL."""
    try:
        return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123, username='default', password='', database='atlas')
    except Exception as e:
        st.error(f"ClickHouse connection failed: {e}")
        return None

@st.cache_resource(ttl=60)
def init_pg_client():
    """Initialize PostgreSQL connection with a short TTL."""
    try:
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASS
        )
        return conn
    except Exception as e:
        st.error(f"PostgreSQL connection failed: {e}")
        return None

# Attempt connections
ch_client = init_ch_client()
pg_conn = init_pg_client()

# =============================================================================
# Helper queries for caching
# =============================================================================

@st.cache_data(ttl=60)
def get_customers():
    """Fetch distinct application customers."""
    if ch_client:
        try:
            # Query distinct application_customer_id from ClickHouse
            df = ch_client.query_df("SELECT DISTINCT application_customer_id FROM telemetry_refined WHERE application_customer_id != ''")
            if not df.empty:
                return df['application_customer_id'].tolist()
        except:
            pass
    # If connection fails or query fails (table empty), return mocks 
    return ["APPCUST0001", "APPCUST0002", "APPCUST0003"]

@st.cache_data(ttl=60)
def get_report_types():
    """Fetch distinct report types from ClickHouse."""
    if ch_client:
        try:
            df = ch_client.query_df("SELECT DISTINCT report_type FROM telemetry_refined WHERE report_type != ''")
            if not df.empty:
                return df['report_type'].tolist()
        except:
            pass
    return ["power_metrics", "thermal_metrics"]

@st.cache_data(ttl=60)
def get_kpis(acid):
    """Calculate the total device count & latest averages."""
    kpi_data = {
        "devices": 0,
        "avg_power": 0.0,
        "avg_thermal": 0.0
    }
    if ch_client:
        try:
            # Fetch KPIs using ClickHouse for ultra-fast aggregation over backend
            # Note: For strict demo purposes filtering by acid
            dev_df = ch_client.query_df(f"SELECT uniqExact(device_id) as count FROM telemetry_refined WHERE application_customer_id = '{acid}'")
            kpi_data["devices"] = dev_df['count'].iloc[0] if not dev_df.empty else 0

            power_df = ch_client.query_df(f"SELECT avg(MetricValue) as avg_val FROM telemetry_refined WHERE application_customer_id = '{acid}' AND report_type = 'power_metrics'")
            kpi_data["avg_power"] = power_df['avg_val'].iloc[0] if not power_df.empty and not pd.isna(power_df['avg_val'].iloc[0]) else 0.0

            thermal_df = ch_client.query_df(f"SELECT avg(MetricValue) as avg_val FROM telemetry_refined WHERE application_customer_id = '{acid}' AND report_type = 'thermal_metrics'")
            kpi_data["avg_thermal"] = thermal_df['avg_val'].iloc[0] if not thermal_df.empty and not pd.isna(thermal_df['avg_val'].iloc[0]) else 0.0
            return kpi_data
        except:
            pass

    # Mock Data Fallback
    kpi_data = {
        "devices": 10560,
        "avg_power": 242.5,
        "avg_thermal": 55.4
    }
    return kpi_data

@st.cache_data(ttl=60)
def get_time_series_data(acid, types, start_dt, end_dt):
    """Retrieve raw historical metrics for plots."""
    if ch_client and len(types) > 0:
        try:
            # Format types payload for IN clause
            types_str = "','".join(types)
            sql = f"""
                SELECT metric_time, device_id, report_type, MetricValue 
                FROM telemetry_refined 
                WHERE application_customer_id = '{acid}' 
                  AND report_type IN ('{types_str}')
                  AND metric_time >= '{start_dt}'
                  AND metric_time <= '{end_dt}'
                ORDER BY metric_time DESC 
                LIMIT 5000
            """
            df = ch_client.query_df(sql)
            if not df.empty:
                return df
        except Exception as e:
            st.warning(f"Failed to fetch time series: {str(e)}")

    # Mock Data Fallback
    times = pd.date_range(end=pd.Timestamp.now(), periods=100, freq='5T')
    data = []
    for rtype in (types if len(types) > 0 else ['power_metrics']):
        base = 240 if rtype == 'power_metrics' else 60
        for _ in range(100):
            data.append({"metric_time": times[_], "device_id": "SRV_MOCK", "report_type": rtype, "MetricValue": base + (time.time() % 10)})
    return pd.DataFrame(data)

@st.cache_data(ttl=60)
def get_raw_records():
    """Retrieve raw tabular data for debugging block."""
    if ch_client:
        try:
            df = ch_client.query_df("SELECT metric_time, application_customer_id, report_type, device_id, MetricValue FROM telemetry_refined ORDER BY metric_time DESC LIMIT 100")
            if not df.empty:
                return df
        except:
            pass
    # Mock Data
    return pd.DataFrame([
        {"metric_time": pd.Timestamp.now(), "application_customer_id": "MOCK", "report_type": "MOCK", "device_id": "MOCK", "MetricValue": 0.0}
    ])

@st.cache_data(ttl=60)
def get_pipeline_runs():
    """Retrieve data pipeline run metadata from PostgreSQL."""
    if pg_conn:
        try:
            query = "SELECT pipeline_name, status, records_processed, records_deduplicated, started_at, completed_at FROM pipeline_runs ORDER BY started_at DESC LIMIT 5"
            return pd.read_sql(query, pg_conn)
        except:
            pass
    # Mock Data Fallback
    return pd.DataFrame([
        {"pipeline_name": "delta_loader_mock", "status": "running", "records_processed": 0, "records_deduplicated": 0, "started_at": pd.Timestamp.now(), "completed_at": None}
    ])

# =============================================================================
# Dashboard UI Layout
# =============================================================================

st.title("🛰️ ATLAS Real-Time Observability")
st.markdown("Monitoring telemetry ingestion, deduplication, and refined querying over ClickHouse & Postgres.")

# ---------- SIDEBAR ----------
st.sidebar.header("Filter Telemetry")

available_customers = get_customers()
selected_customer = st.sidebar.selectbox("Application Customer ID", available_customers)

available_reports = get_report_types()
selected_reports = st.sidebar.multiselect("Report Types", available_reports, default=available_reports[:1])

# Date / Time Range Slider
import datetime
today = datetime.date.today()
date_range = st.sidebar.slider("Time Range", 
                               min_value=today - datetime.timedelta(days=7), 
                               max_value=today + datetime.timedelta(days=1), 
                               value=(today - datetime.timedelta(days=1), today))

st.sidebar.markdown("---")
if not ch_client:
    st.sidebar.error("🔴 ClickHouse Offline (Displaying Mock Data)")
else:
    st.sidebar.success("🟢 ClickHouse Connected")

if not pg_conn:
    st.sidebar.error("🔴 PostgreSQL Offline")
else:
    st.sidebar.success("🟢 PostgreSQL Connected")


# ---------- MAIN BODY ----------
kpis = get_kpis(selected_customer)

st.subheader("Key Performance Indicators")
col1, col2, col3 = st.columns(3)
col1.metric("Total Devices Active", f"{kpis['devices']:,}")
col2.metric("Latest Avg Power Metric", f"{kpis['avg_power']:.2f} W")
col3.metric("Latest Avg Thermal Metric", f"{kpis['avg_thermal']:.2f} °C")

st.markdown("---")

st.subheader(f"Time Series: {selected_customer}")
ts_df = get_time_series_data(selected_customer, selected_reports, date_range[0], date_range[1])

if not ts_df.empty:
    fig = px.line(
        ts_df, 
        x="metric_time", 
        y="MetricValue", 
        color="device_id", 
        facet_row="report_type", 
        title="High-Cardinality Metric Flows",
        template="plotly_dark",
        height=500
    )
    # Hide verbose legend depending on scale
    fig.update_layout(showlegend=(len(ts_df['device_id'].unique()) < 20))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No time series data available for these filters.")


st.markdown("---")

st.subheader("Live Raw Data Pipeline (Max 100)")
raw_df = get_raw_records()
st.dataframe(raw_df, use_container_width=True)

st.markdown("---")

st.subheader("Data Pipeline Runs (PostgreSQL Metadata)")
pipeline_df = get_pipeline_runs()
st.dataframe(pipeline_df, use_container_width=True)
