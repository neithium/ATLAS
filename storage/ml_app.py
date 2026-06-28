import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import json
import os
import re

PROMPT_FILE_PATH = "/app/prompts/rca_system_prompt.txt"

def normalize_ollama_endpoint(raw_endpoint):
    """Normalize the Ollama endpoint so requests receives a valid URL."""
    endpoint = (raw_endpoint or "").strip().strip('"').strip("'")
    markdown_match = re.match(r"^\[(?P<label>[^\]]+)\]\((?P<url>[^)]+)\)$", endpoint)
    if markdown_match:
        endpoint = markdown_match.group("url").strip().strip('"').strip("'")
    if endpoint and "://" not in endpoint:
        endpoint = f"http://{endpoint}"
    return endpoint.rstrip("/") or "http://host.docker.internal:11434"

# Grab the Ollama target location from your environment configuration
OLLAMA_ENDPOINT = normalize_ollama_endpoint(os.environ.get("OLLAMA_HOST", "http://host.docker.internal:11434"))

def load_system_prompt(path):
    """Safely ingest the externalized system prompt rule-set."""
    try:
        with open(path, "r") as f:
            return f.read()
    except FileNotFoundError:
        st.error(f"Configuration Error: System prompt file missing at {path}")
        return None

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
    # KPI Metrics (Cleaned up - NO LLM CODE HERE!)
    # -------------------------------------------------------------------------
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Recent Predictions Scanned", value=len(df_recent))
    with col2:
        anomaly_count = len(df_anomalies)
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
        
        # --- ALL AI LOGIC HAPPENS HERE, SAFELY BEHIND THE BUTTON ---
        if st.button("Run AI Root Cause Analysis", type="primary"):
            if df_anomalies.empty:
                st.warning("Analysis aborted: No anomalies available in the current context window.")
            else:
                system_rules = load_system_prompt(PROMPT_FILE_PATH)
                
                if system_rules:
                    with st.spinner("🧠 Phi-4-Mini is extracting root causes..."):
                        
                        # 1. Isolate the anomaly rows
                        payload_df = df_anomalies.drop(columns=['insertion_time'], errors='ignore').copy()

                        # 2. Force convert the Timestamp objects to strings
                        if 'metric_time' in payload_df.columns:
                            payload_df['metric_time'] = payload_df['metric_time'].astype(str)

                        # 3. Convert to JSON payload
                        payload = payload_df.to_dict(orient="records")
                        
                        # 4. Initialize response dict 
                        ai_response = {} 
                        
                        try:
                            response = requests.post(f"{OLLAMA_ENDPOINT}/api/generate", json={
                                "model": "phi4-mini", 
                                "prompt": f"Analyze this telemetry dataset for anomalies:\n{json.dumps(payload)}",
                                "system": system_rules,
                                "format": "json", 
                                "stream": False
                            }, timeout=45)
                            
                            if response.status_code == 200:
                                try:
                                    raw_text = response.json().get('response', '{}')
                                    ai_response = json.loads(raw_text)
                                    
                                    # --- RENDERING THE AI OUTPUT ---
                                    st.success(ai_response.get('incident_summary', 'Analysis Complete'))
                                    
                                    st.markdown("### 🔍 Diagnostics")
                                    st.info(ai_response.get('root_cause_hypothesis', ai_response.get('root_cause_analysis', 'No structural metrics extracted.')))
                                    
                                    col_a, col_b = st.columns(2)
                                    with col_a:
                                        st.markdown("### ⚠️ Component Risk")
                                        for subsystem in ai_response.get('affected_subsystems', []):
                                            st.markdown(f"- **{subsystem}**")
                                    with col_b:
                                        st.markdown("### 🛠️ Mitigation Steps")
                                        for runbook_step in ai_response.get('recommended_remediation', []):
                                            st.markdown(f"- {runbook_step}")
                                            
                                        # HERE ARE YOUR BASH COMMANDS! Safe and sound.
                                        if 'diagnostic_commands' in ai_response:
                                            st.markdown("### 💻 Runbook Commands")
                                            for cmd in ai_response.get('diagnostic_commands', []):
                                                st.code(cmd, language="bash")
                                                
                                except json.JSONDecodeError:
                                    st.error("🚨 Phi-4-Mini returned an improperly formatted JSON response.")
                                    with st.expander("View Raw Output"):
                                        st.write(response.json().get('response', 'No response body'))
                                        
                            else:
                                st.error(f"Inference failure response code: {response.status_code}")
                                with st.expander("View Error Details"):
                                    st.write(response.text)
                                
                        except requests.exceptions.RequestException as exc:
                            st.error("🚨 Connection Blocked: Unable to hit the Ollama server via host.docker.internal.")
                            st.caption(f"Resolved Ollama endpoint: {OLLAMA_ENDPOINT}")
                            st.caption(f"Request error: {type(exc).__name__}")