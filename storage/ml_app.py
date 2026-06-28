import streamlit as st
import pandas as pd
import plotly.express as px

def fetch_recent_predictions(ch_client, limit=50):
    """Fetch the most recent ML predictions regardless of status."""
    query = f"""
        SELECT *
        FROM atlas.telemetry_ml_predictions
        ORDER BY metric_time DESC
        LIMIT {limit}
    """
    try:
        return ch_client.query_df(query)
    except Exception as e:
        st.error(f"Database error while fetching predictions: {e}")
        return pd.DataFrame()

def fetch_recent_anomalies(ch_client, limit=50):
    """Fetch only the most recent anomalies (-1 prediction)."""
    query = f"""
        SELECT *
        FROM atlas.telemetry_ml_predictions
        WHERE prediction = -1
        ORDER BY metric_time DESC
        LIMIT {limit}
    """
    try:
        return ch_client.query_df(query)
    except Exception as e:
        st.error(f"Database error while fetching anomalies: {e}")
        return pd.DataFrame()

def render_ml_dashboard(ch_client):
    """Main rendering function to be called from app.py."""
    st.subheader("Machine Learning Anomaly Detection")
    st.markdown("Real-time telemetry inference results, health scoring, and automated root cause analysis.")

    if not ch_client:
        st.error("ClickHouse client connection is not available.")
        return

    with st.spinner("Retrieving ML inference data..."):
        df_recent = fetch_recent_predictions(ch_client, limit=50)
        df_anomalies = fetch_recent_anomalies(ch_client, limit=50)

    if df_recent.empty:
        st.info("No ML predictions found in the database. Awaiting data from the ML loader.")
        return

    # -------------------------------------------------------------------------
    # KPI Metrics
    # -------------------------------------------------------------------------
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Recent Predictions Scanned", value=len(df_recent))
    with col2:
        anomaly_count = len(df_anomalies)
        # Visually indicate if anomalies are present
        st.metric(label="Recent Anomalies Detected", value=anomaly_count)
    with col3:
        avg_health = df_recent['health_score'].mean() if 'health_score' in df_recent.columns else 0
        st.metric(label="Average Fleet Health Score", value=f"{avg_health:.1f} / 100")

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Dashboard Tabs
    # -------------------------------------------------------------------------
    tab_overview, tab_anomalies, tab_ai_analysis = st.tabs([
        "Latest Inferences", 
        "Detected Anomalies", 
        "AI Diagnostic Engine"
    ])

    with tab_overview:
        st.markdown("**Recent Fleet Health Timeline**")
        
        if 'metric_time' in df_recent.columns and 'health_score' in df_recent.columns and 'prediction' in df_recent.columns:
            # Map numeric predictions to strings for better chart legends and colors
            plot_df = df_recent.copy()
            plot_df['Status'] = plot_df['prediction'].map({1: 'Normal', -1: 'Anomaly'})
            
            fig = px.scatter(
                plot_df, 
                x="metric_time", 
                y="health_score", 
                color="Status",
                color_discrete_map={"Normal": "#00CC96", "Anomaly": "#EF553B"},
                hover_data=["device_id", "server_name", "anomaly_score"],
                title="Health Score Distribution (Last 50 Records)",
                template="plotly_dark"
            )
            fig.update_traces(marker=dict(size=10, line=dict(width=1, color='DarkSlateGrey')))
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Raw Data (Last 50 Records)**")
        st.dataframe(df_recent, use_container_width=True)

    with tab_anomalies:
        st.markdown("**Isolated Anomaly Records**")
        if df_anomalies.empty:
            st.success("System Normal: No recent anomalies detected in the queried window.")
        else:
            st.dataframe(df_anomalies, use_container_width=True)

    with tab_ai_analysis:
        st.markdown("**Automated Root Cause Analysis**")
        st.write("Trigger the LLM engine to analyze the recent anomalous telemetry patterns, correlate cross-metric degradation, and generate mitigation strategies.")
        
        if st.button("Run AI Root Cause Analysis", type="primary"):
            if df_anomalies.empty:
                st.warning("Analysis aborted: No anomalies available in the current context window.")
            else:
                st.info("LLM Integration Module Pending. The data payload below will be transmitted to the inference endpoint.")
                
                # Setup the JSON payload that you will eventually send to the LLM
                with st.expander("View Data Payload (JSON)"):
                    payload = df_anomalies.drop(columns=['insertion_time'], errors='ignore').to_dict(orient="records")
                    st.json(payload)