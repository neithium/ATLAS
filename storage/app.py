import streamlit as st
import clickhouse_connect
import psycopg
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="ATLAS Observability Dashboard", layout="wide")

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

ch_client = init_ch_client()
pg_conn = init_pg_client()

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
                return pd.DataFrame(cur.fetchall(), columns=cols)
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
# UI Layout
# =============================================================================

st.title(" ATLAS Observability Dashboard")
st.markdown(" Observability and Real-Time Telemetry Visualization Packed in One")

col1, col2 = st.columns(2)
with col1:
    if ch_client: st.success("  ClickHouse Online (Time-Series Engine)")
    else: st.error("  ClickHouse Offline")
with col2:
    if pg_conn and not pg_conn.closed: st.success("  PostgreSQL Online (Relational Metadata)")
    else: st.error("  PostgreSQL Offline")

st.markdown("---")

# Added the new "Live Charts" tab
tab_ch, tab_pg, tab_charts = st.tabs(["ClickHouse Explorer", "PostgreSQL Explorer", " Live Charts Builder"])

# -----------------------------------------------------------------------------
# ClickHouse Tab
# -----------------------------------------------------------------------------
with tab_ch:
    ch_tables = get_ch_tables()
    if not ch_tables:
        st.info("No tables found in ClickHouse.")
    else:
        selected_ch_table = st.selectbox("Select ClickHouse Table:", ch_tables, key="ch_table")
        col_schema, col_preview = st.columns([1, 2])
        with col_schema:
            st.markdown(f"**Schema for `{selected_ch_table}`**")
            st.dataframe(get_ch_table_schema(selected_ch_table), use_container_width=True, height=400)
        with col_preview:
            st.markdown(f"**Live Data Preview (Top 50 Rows)**")
            st.dataframe(get_ch_data(selected_ch_table), use_container_width=True, height=400)

# -----------------------------------------------------------------------------
# PostgreSQL Tab
# -----------------------------------------------------------------------------
with tab_pg:
    pg_tables = get_pg_tables()
    if not pg_tables:
        st.info("No tables found in PostgreSQL.")
    else:
        selected_pg_table = st.selectbox("Select PostgreSQL Table:", pg_tables, key="pg_table")
        col_schema_pg, col_preview_pg = st.columns([1, 2])
        with col_schema_pg:
            st.markdown(f"**Schema for `{selected_pg_table}`**")
            pg_schema_df = query_pg(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{selected_pg_table}'")
            st.dataframe(pg_schema_df, use_container_width=True, height=400)
        with col_preview_pg:
            st.markdown(f"**Live Data Preview (Top 50 Rows)**")
            st.dataframe(query_pg(f"SELECT * FROM {selected_pg_table} LIMIT 50"), use_container_width=True, height=400)

# -----------------------------------------------------------------------------
# Live Charts Builder Tab (NEW)
# -----------------------------------------------------------------------------
with tab_charts:
    st.subheader("Dynamic Time-Series Visualizer")
    
    if not ch_tables:
        st.warning("No ClickHouse tables available for charting.")
    else:
        # 1. Select the table to plot
        chart_table = st.selectbox("Source Table:", ch_tables, key="chart_table")
        schema_df = get_ch_table_schema(chart_table)
        
        if not schema_df.empty:
            # 2. Introspect columns to figure out what can be plotted
            # Find Date/DateTime columns for X-Axis
            time_cols = schema_df[schema_df['type'].str.contains('Date|DateTime', case=False)]['name'].tolist()
            # Find Numeric columns for Y-Axis
            num_cols = schema_df[schema_df['type'].str.contains('Int|Float', case=False)]['name'].tolist()
            # Find String columns for Grouping/Categorizing
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
                
                # Manual Refresh Button for "Real-Time" fetching
                if st.button("  Refresh Data Now"):
                    get_ch_chart_data.clear() # Clears the cache to force a fresh DB pull

                # 4. Fetch and Plot
                with st.spinner("Fetching data from ClickHouse..."):
                    chart_df = get_ch_chart_data(chart_table, x_axis, y_axis, color_by, row_limit)
                    
                    if not chart_df.empty:
                        # Sort chronologically for Plotly line charts
                        chart_df = chart_df.sort_values(by=x_axis)
                        
                        fig = px.line(
                            chart_df, 
                            x=x_axis, 
                            y=y_axis, 
                            color=None if color_by == "None" else color_by,
                            template="plotly_dark",
                            title=f"{y_axis} over time (Last {row_limit} records)"
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("No data returned for the selected parameters.")