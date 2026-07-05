import streamlit as st
import clickhouse_connect
import psycopg
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta
from deltalake import DeltaTable
import pytz
from streamlit_option_menu import option_menu

import ml_app 

st.set_page_config(page_title="ATLAS Observability Dashboard", layout="wide", initial_sidebar_state="expanded")

# Timezone setup - IST (Indian Standard Time)
IST = pytz.timezone('Asia/Kolkata')

# =============================================================================
# Connection Configuration
# =============================================================================
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
POSTGRES_HOST   = os.environ.get("POSTGRES_HOST", "127.0.0.1")
POSTGRES_DB     = os.environ.get("POSTGRES_DB", "atlas_metadata")
POSTGRES_USER   = os.environ.get("POSTGRES_USER", "atlas")
POSTGRES_PASS   = os.environ.get("POSTGRES_PASSWORD", "atlas_secure_pwd")

@st.cache_resource(ttl=60)
def init_ch_client():
    try:
        return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123, username='default', password='', database='atlas')
    except Exception as e:
        return None

@st.cache_resource(ttl=60)
def init_pg_client():
    try:
        return psycopg.connect(host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS)
    except Exception as e:
        return None

@st.cache_resource(ttl=60)
def init_delta_client():
    """Check if Delta Lake metrics table is accessible."""
    try:
        dt = DeltaTable("/refined/system_metrics")
        return dt
    except FileNotFoundError:
        # Table doesn't exist yet - pipeline hasn't processed first batch
        return None
    except Exception as e:
        # Connection error or other issue
        return None

def check_delta_status():
    """Check Delta Lake directory and table status for diagnostics."""
    import os
    status = {"refined_exists": False, "system_metrics_exists": False, "error": None}
    
    try:
        if os.path.exists("/refined"):
            status["refined_exists"] = True
            if os.path.exists("/refined/system_metrics"):
                status["system_metrics_exists"] = True
    except Exception as e:
        status["error"] = str(e)
    
    return status

@st.cache_data(ttl=15)
def query_delta_metrics(limit=500):
    """Read RECENT metrics from Delta Lake (not entire table). Last N batches only."""
    try:
        dt = DeltaTable("/refined/system_metrics")
        # Convert to pandas for Streamlit
        df = dt.to_pandas()
        
        if df.empty:
            return df
        
        # Convert timestamp and sort ONCE, also convert to IST
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            df['timestamp'] = df['timestamp'].dt.tz_convert(IST)
            df = df.sort_values('timestamp', ascending=False).head(limit)
        
        return df
    except FileNotFoundError:
        # Metrics table doesn't exist yet
        return pd.DataFrame()
    except Exception as e:
        return pd.DataFrame()

ch_client = init_ch_client()
pg_conn = init_pg_client()
delta_client = init_delta_client()

# =============================================================================
# Dynamic Data Fetchers
# =============================================================================

@st.cache_data(ttl=60)
def query_pg(sql_query):
    if not pg_conn or pg_conn.closed: return pd.DataFrame()
    try:
        with pg_conn.cursor() as cur:
            cur.execute(sql_query)
            if cur.description:
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                df = pd.DataFrame(rows, columns=cols)
                # Convert UUID columns to strings to avoid Arrow serialization issues
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str)
                return df
        return pd.DataFrame()
    except Exception as e:
        pg_conn.rollback()
        return pd.DataFrame()

def get_pg_tables():
    df = query_pg("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    return df['table_name'].tolist() if not df.empty else []

@st.cache_data(ttl=60)
def get_ch_tables():
    if ch_client:
        try:
            df = ch_client.query_df("SHOW TABLES FROM atlas")
            return df['name'].tolist() if not df.empty else []
        except: pass
    return []

@st.cache_data(ttl=60)
def get_ch_table_schema(table_name):
    if ch_client:
        try:
            return ch_client.query_df(f"DESCRIBE TABLE atlas.{table_name}")
        except: pass
    return pd.DataFrame()

@st.cache_data(ttl=60)
def get_ch_data(table_name, limit=50):
    if ch_client:
        try:
            return ch_client.query_df(f"SELECT * FROM atlas.{table_name} LIMIT {limit}")
        except: pass
    return pd.DataFrame()

@st.cache_data(ttl=10) # 10-second TTL for "Real-Time" feel
def get_ch_chart_data(table, x_col, y_col, color_col, limit=1000):
    """Fetches specific columns for plotting, ordered by time."""
    if ch_client:
        try:
            select_cols = f"{x_col}, {y_col}"
            if color_col != "None": select_cols += f", {color_col}"
            
            query = f"SELECT {select_cols} FROM atlas.{table} ORDER BY {x_col} DESC LIMIT {limit}"
            return ch_client.query_df(query)
        except Exception as e:
            st.error(f"Chart Data Error: {e}")
    return pd.DataFrame()

# =============================================================================
# UI Layout: Global Sidebar
# =============================================================================

with st.sidebar:
    st.markdown("#  ATLAS DASHBOARD")
    st.markdown("##### Observability, Telemetry and AI, All Packed in One !!")
    st.markdown("---")
    st.markdown("###  System Status")
    
    # Render connection status blocks
    if ch_client: 
        st.success(" ClickHouse Online")
    else: 
        st.error(" ClickHouse Offline")
    
    if pg_conn and not pg_conn.closed: 
        st.success(" PostgreSQL Online")
    else: 
        st.error(" PostgreSQL Offline")
    
    if delta_client: 
        st.success(" Delta Lake Online")
    else: 
        st.error(" Delta Lake Offline")

     
    st.markdown("---")
    st.markdown("###  Navigation")
    
    # The new, beautiful routing menu
    selected_page = option_menu(
        menu_title=None,
        options=[
            "ClickHouse Explorer", 
            "PostgreSQL Explorer", 
            "Live Charts Builder", 
            "Delta Lake Statistics", 
            "ATLAS AI"
        ],
        icons=[
            "database-fill",     
            "server",            
            "graph-up-arrow",    
            "water",             
            "robot"              
        ], 
        menu_icon="cast", 
        default_index=0,
        styles={
            "container": {
                "padding": "0!important", 
                "background-color": "transparent"
            },
            "icon": {
                "color": "#9CA3AF", # Subtle slate-gray icons
                "font-size": "18px"
            }, 
            "nav-link": {
                "font-family": "system-ui, -apple-system, sans-serif", # Forces modern OS fonts
                "font-size": "15px", 
                "text-align": "left", 
                "margin": "4px 0px",
                "padding": "12px 15px",     # Better touch-target breathing room
                "border-radius": "8px",     # Sleek rounded edges
                "color": "#D1D5DB",         # Soft muted text (anti-HTML blue)
                "--hover-color": "rgba(255, 255, 255, 0.05)"
            },
            "nav-link-selected": {
                "background-color": "rgba(255, 255, 255, 0.1)", 
                "font-weight": "600",
                "color": "#FFFFFF",         # Bright white only when active
                "border": "1px solid rgba(255, 255, 255, 0.05)" # Ultra-subtle depth border
            },
        }
    )

# =============================================================================
# UI Layout: Main Content Area
# =============================================================================

# Header for the main area
st.title(f"{selected_page}")
st.markdown("---")

# -----------------------------------------------------------------------------
# ClickHouse Explorer
# -----------------------------------------------------------------------------
if selected_page == "ClickHouse Explorer":
    ch_tables = get_ch_tables()
    if not ch_tables:
        st.info("No tables found in ClickHouse.")
    else:
        selected_ch_table = st.selectbox("Select ClickHouse Table:", ch_tables, key="ch_table")
        col_schema, col_preview = st.columns([1, 2])
        with col_schema:
            st.markdown(f"**Schema for `{selected_ch_table}`**")
            st.dataframe(get_ch_table_schema(selected_ch_table), use_container_width=True, height=600)
        with col_preview:
            st.markdown(f"**Live Data Preview (Top 50 Rows)**")
            st.dataframe(get_ch_data(selected_ch_table), use_container_width=True, height=600)

# -----------------------------------------------------------------------------
# PostgreSQL Explorer
# -----------------------------------------------------------------------------
elif selected_page == "PostgreSQL Explorer":
    pg_tables = get_pg_tables()
    if not pg_tables:
        st.info("No tables found in PostgreSQL.")
    else:
        selected_pg_table = st.selectbox("Select PostgreSQL Table:", pg_tables, key="pg_table")
        col_schema_pg, col_preview_pg = st.columns([1, 2])
        with col_schema_pg:
            st.markdown(f"**Schema for `{selected_pg_table}`**")
            pg_schema_df = query_pg(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{selected_pg_table}'")
            st.dataframe(pg_schema_df, use_container_width=True, height=600)
        with col_preview_pg:
            st.markdown(f"**Live Data Preview (Top 50 Rows)**")
            st.dataframe(query_pg(f"SELECT * FROM {selected_pg_table} LIMIT 50"), use_container_width=True, height=600)

# -----------------------------------------------------------------------------
# Live Charts Builder
# -----------------------------------------------------------------------------
elif selected_page == "Live Charts Builder":
    st.subheader("Dynamic Time-Series Visualizer")
    ch_tables = get_ch_tables()
    
    if not ch_tables:
        st.warning("No ClickHouse tables available for charting.")
    else:
        # 1. Select the table to plot
        chart_table = st.selectbox("Source Table:", ch_tables, key="chart_table")
        schema_df = get_ch_table_schema(chart_table)
        
        if not schema_df.empty:
            # 2. Introspect columns to figure out what can be plotted
            time_cols = schema_df[schema_df['type'].str.contains('Date|DateTime', case=False)]['name'].tolist()
            num_cols = schema_df[schema_df['type'].str.contains('Int|Float', case=False)]['name'].tolist()
            cat_cols = ["None"] + schema_df[schema_df['type'].str.contains('String', case=False)]['name'].tolist()

            if not time_cols or not num_cols:
                st.info(f"Table '{chart_table}' must have at least one DateTime column and one Numeric column to build a time-series chart.")
            else:
                # 3. Chart Controls
                ctrl_col1, ctrl_col2, ctrl_col3, ctrl_col4 = st.columns(4)
                with ctrl_col1: x_axis = st.selectbox("X-Axis (Time):", time_cols)
                with ctrl_col2: y_axis = st.selectbox("Y-Axis (Metric):", num_cols)
                with ctrl_col3: color_by = st.selectbox("Group By (Color):", cat_cols)
                with ctrl_col4: row_limit = st.slider("Lookback Window (Rows):", 100, 10000, 1000, step=100)
            
                if st.button(" Refresh Data Now"):
                    get_ch_chart_data.clear()

                # 4. Fetch and Plot
                with st.spinner("Fetching data from ClickHouse..."):
                    chart_df = get_ch_chart_data(chart_table, x_axis, y_axis, color_by, row_limit)
                    
                    if not chart_df.empty:
                        chart_df = chart_df.sort_values(by=x_axis)
                        fig = px.line(
                            chart_df, 
                            x=x_axis, 
                            y=y_axis, 
                            color=None if color_by == "None" else color_by,
                            template="plotly_dark",
                            title=f"{y_axis} over time (Last {row_limit} records)"
                        )
                        fig.update_layout(height=600) # Made the chart taller!
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No data returned for the selected parameters.")

# -----------------------------------------------------------------------------
# Delta Lake Statistics
# -----------------------------------------------------------------------------
elif selected_page == "Delta Lake Statistics":
    st.subheader("Delta Lake Streaming Metrics Dashboard")
    st.markdown("Autonomous metrics from the Delta lake layer proving sub-second latency independent of upstream APIs.")
    
    if not delta_client:
        status = check_delta_status()
        
        if not status["refined_exists"]:
            st.error(" `/refined` volume not mounted to container")
            st.info("**Action**: Ensure volume mount in docker-compose.yml: `- delta-refined:/refined`")
        elif not status["system_metrics_exists"]:
            st.warning(" Metrics table initializing...")
            st.info("**Status**: Streaming pipeline is creating the metrics table. This appears on first batch processing (~10-30 seconds after startup).")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric(" Pipeline", "Starting", delta="Monitor logs")
            with col2:
                st.metric(" Table Status", "Pending", delta="Waiting for first batch")
            
            st.divider()
            st.markdown("**Troubleshooting**:")
            st.markdown("-  Streaming pipeline should be running: `docker ps | grep atlas-lakehouse`")
            st.markdown("-  Check pipeline logs: `docker compose logs atlas-lakehouse --tail 20`")
            st.markdown("-  Volume should be writable (not `:ro` in docker-compose.yml)")
        else:
            st.error(f" DeltaTable connection error: {status['error']}")
            st.info("Try refreshing the page or check container logs")
    else:
        with st.spinner(" Loading metrics from Delta Lake..."):
            metrics_df = query_delta_metrics(limit=500)
        
        if metrics_df.empty:
            st.info(" No metrics available yet. Metrics will appear here once the streaming pipeline processes batches.")
        else:
            try:
                with st.spinner(" Computing SLA statistics..."):
                    total_time_ms = metrics_df['total_time'] * 1000
                    p50_latency = total_time_ms.quantile(0.50)
                    p95_latency = total_time_ms.quantile(0.95)
                    p99_latency = total_time_ms.quantile(0.99)
                    
                    total_rows = metrics_df['row_count'].sum()
                    total_time_sum = metrics_df['total_time'].sum()
                    throughput = total_rows / total_time_sum if total_time_sum > 0 else 0
                
                st.markdown("###  Key Performance Indicators (Last 500 Batches)")
                col_p50, col_p95, col_p99, col_tput = st.columns(4)
                with col_p50: st.metric(label="P50 Latency", value=f"{p50_latency:.1f}ms")
                with col_p95: st.metric(label="P95 Latency", value=f"{p95_latency:.1f}ms")
                with col_p99: st.metric(label="P99 Latency", value=f"{p99_latency:.1f}ms")
                with col_tput: st.metric(label="Throughput", value=f"{throughput:,.0f}rows/sec")
                
                st.markdown("---")
                st.markdown("###  Latency Trends (Recent Batches)")
                
                col_trend, col_merge = st.columns(2)
                if 'timestamp' not in metrics_df.columns:
                    st.warning("⚠ Timestamp column not found in metrics data.")
                else:
                    with col_trend:
                        trend_df = metrics_df.sort_values('timestamp')
                        fig_trend = px.line(
                            trend_df, x='timestamp', y='total_time',
                            title="Total Batch Latency Over Time",
                            labels={'total_time': 'Latency (seconds)', 'timestamp': 'Time'},
                            template="plotly_dark", render_mode="webgl"
                        )
                        fig_trend.add_hline(y=0.5, line_dash="dash", line_color="yellow", annotation_text="SLA Target (500ms)")
                        fig_trend.add_hline(y=1.5, line_dash="dash", line_color="red", annotation_text="Alert Threshold (1.5s)")
                        st.plotly_chart(fig_trend, use_container_width=True)
                    
                    with col_merge:
                        merge_df = metrics_df.sort_values('timestamp')
                        fig_merge = px.line(
                            merge_df, x='timestamp', y='merge_time',
                            title="MERGE Operation Time Trend",
                            labels={'merge_time': 'MERGE Duration (seconds)', 'timestamp': 'Time'},
                            template="plotly_dark", render_mode="webgl"
                        )
                        fig_merge.add_hline(y=0.1, line_dash="dash", line_color="green", annotation_text="Typical (<100ms)")
                        st.plotly_chart(fig_merge, use_container_width=True)
                
                st.markdown("---")
                
                # Performance Breakdown
                st.markdown("###  Performance Analysis")
                tab_breakdown, tab_outliers, tab_raw = st.tabs(["Latency Breakdown", "Outliers & Anomalies", "Raw Metrics"])
                
                with tab_breakdown:
                    metrics_df['merge_pct'] = (metrics_df['merge_time'] / metrics_df['total_time'] * 100).round(2)
                    metrics_df['prep_pct'] = ((metrics_df['total_time'] - metrics_df['merge_time']) / metrics_df['total_time'] * 100).round(2)
                    
                    breakdown_stats = pd.DataFrame({
                        'Component': ['MERGE Operation', 'Prep & Partitioning'],
                        'Avg Time (ms)': [metrics_df['merge_time'].mean() * 1000, (metrics_df['total_time'] - metrics_df['merge_time']).mean() * 1000],
                        'P95 Time (ms)': [metrics_df['merge_time'].quantile(0.95) * 1000, (metrics_df['total_time'] - metrics_df['merge_time']).quantile(0.95) * 1000],
                        'Pct of Total': [metrics_df['merge_pct'].mean(), metrics_df['prep_pct'].mean()]
                    })
                    st.dataframe(breakdown_stats, use_container_width=True)
                
                with tab_outliers:
                    p99_threshold = metrics_df['total_time'].quantile(0.99)
                    slow_batches = metrics_df[metrics_df['total_time'] > p99_threshold].sort_values('total_time', ascending=False)
                    st.subheader("Slow Batches (P99+)")
                    if not slow_batches.empty:
                        display_cols = [c for c in ['batch_id', 'timestamp', 'total_time', 'merge_time', 'row_count'] if c in slow_batches.columns]
                        slow_batches_display = slow_batches[display_cols].copy()
                        if 'total_time' in slow_batches_display.columns: slow_batches_display['total_time'] = slow_batches_display['total_time'].apply(lambda x: f"{x*1000:.2f}ms")
                        if 'merge_time' in slow_batches_display.columns: slow_batches_display['merge_time'] = slow_batches_display['merge_time'].apply(lambda x: f"{x*1000:.2f}ms")
                        st.dataframe(slow_batches_display, use_container_width=True)
                    else:
                        st.success(" No outliers detected! All batches performing within SLA.")
                
                with tab_raw:
                    st.subheader("Raw Metrics Data")
                    display_df = metrics_df.sort_values('timestamp', ascending=False).head(50).copy()
                    st.dataframe(display_df, use_container_width=True)
                    
            except Exception as e:
                st.error(f" Error rendering dashboard: {type(e).__name__}")
                st.info("Try refreshing the page or check container logs for details.")

# -----------------------------------------------------------------------------
# ATLAS AI
# -----------------------------------------------------------------------------
elif selected_page == "ATLAS AI":
    ml_app.render_ml_dashboard(ch_client)