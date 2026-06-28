import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import json
import os
import re
from pathlib import Path

PROMPT_FILE_PATH = "/app/prompts/rca_system_prompt.txt"
ASSET_DIR = Path(__file__).resolve().parent / "assets" / "img"


def load_avatar_asset(filename):
    """Load a local avatar image if it is packaged with the app."""
    avatar_path = ASSET_DIR / filename
    try:
        if avatar_path.exists():
            return avatar_path.read_bytes()
    except OSError:
        pass
    return None


ASSISTANT_AVATAR = load_avatar_asset("ai-chatbot.webp")
USER_AVATAR = load_avatar_asset("user.png")

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
    # KPI Metrics  
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
    tab_overview, tab_anomalies, tab_ai_analysis, tab_terminal = st.tabs([
        "Latest Inferences", 
        "Detected Anomalies", 
        "AI Diagnostic Engine"," ATLAS Copilot"
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
                    with st.spinner(" Phi-4-Mini is extracting root causes..."):
                        
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
                            }, timeout=120)
                            
                            if response.status_code == 200:
                                try:
                                    raw_text = response.json().get('response', '{}')
                                    ai_response = json.loads(raw_text)
                                    
                                    # --- RENDERING THE AI OUTPUT ---
                                    st.success(ai_response.get('incident_summary', 'Analysis Complete'))
                                    
                                    st.markdown("###   Diagnostics")
                                    st.info(ai_response.get('root_cause_hypothesis', ai_response.get('root_cause_analysis', 'No structural metrics extracted.')))
                                    
                                    col_a, col_b = st.columns(2)
                                    with col_a:
                                        st.markdown("###   Component Risk")
                                        for subsystem in ai_response.get('affected_subsystems', []):
                                            st.markdown(f"- **{subsystem}**")
                                    with col_b:
                                        st.markdown("###   Mitigation Steps")
                                        for runbook_step in ai_response.get('recommended_remediation', []):
                                            st.markdown(f"- {runbook_step}")
                                            
                                      
                                        if 'diagnostic_commands' in ai_response:
                                            st.markdown("### Suggested Commands")
                                            for cmd in ai_response.get('diagnostic_commands', []):
                                                st.code(cmd, language="bash")
                                                
                                except json.JSONDecodeError:
                                    st.error("  Phi-4-Mini returned an improperly formatted JSON response.")
                                    with st.expander("View Raw Output"):
                                        st.write(response.json().get('response', 'No response body'))
                                        
                            else:
                                st.error(f"Inference failure response code: {response.status_code}")
                                with st.expander("View Error Details"):
                                    st.write(response.text)
                                
                        except requests.exceptions.RequestException as exc:
                            st.error("  Connection Blocked: Unable to hit the Ollama server via host.docker.internal.")
                            st.caption(f"Resolved Ollama endpoint: {OLLAMA_ENDPOINT}")
                            st.caption(f"Request error: {type(exc).__name__}")
                        except requests.exceptions.Timeout:
                            st.error("  AI Analysis Timeout: The LLM took too long to process the telemetry data. Try analyzing fewer rows.")
                        except requests.exceptions.ConnectionError:
                            st.error("  Connection Blocked: Unable to reach the Ollama server.")
                        except requests.exceptions.RequestException as exc:
                            st.error(f" Request error: {type(exc).__name__}")
    # -------------------------------------------------------------------------
    # TAB 4: SRE Copilot Terminal
    # -------------------------------------------------------------------------
    with tab_terminal:
        st.markdown("###### Chat with ATLAS Copilot")
        st.caption("Engage directly with ATLAS  Copilot. Ask for command explanations, custom scripts, or deeper analysis.")
            
        
        # 1. Initialize chat history in Streamlit's session state
        if "sre_messages" not in st.session_state:
            st.session_state.sre_messages = [
                {
                    "role": "system", 
                    "content": "You are ATLAS, an elite AI Site Reliability Engineering copilot. Speak directly to the user. NEVER simulate a conversation between a 'User' and 'Assistant'. NEVER prefix your responses. Be concise, technical, and robotic."
                },
                {
                    "role": "user",
                    "content": "Initialize interactive copilot session."
                },
                {
                    "role": "assistant",
                    "content": "ATLAS Copilot initialized. Telemetry streams connected. Awaiting your command."
                }
            ]
            
        # THE FIX 1: Create a fixed-height container so the terminal scrolls internally!
        terminal_container = st.container(height=500)
        
        # 2. Display existing chat history INSIDE the container
        with terminal_container:
            for msg in st.session_state.sre_messages:
                if msg["role"] != "system":
                    avatar_icon = ASSISTANT_AVATAR if msg["role"] == "assistant" else USER_AVATAR
                    with st.chat_message(msg["role"], avatar=avatar_icon):
                        st.markdown(msg["content"])
                    
        # 3. Chat Input Field (Stays pinned to the bottom of the tab)
        if user_prompt := st.chat_input("Enter command or query..."):
            
            # Add user message to state
            st.session_state.sre_messages.append({"role": "user", "content": user_prompt})
            
            # Render the new messages INSIDE the fixed container so it doesn't push the layout
            with terminal_container:
                with st.chat_message("user", avatar=USER_AVATAR):
                    st.markdown(user_prompt)
                    
                # 4. Generate the streaming response
                with st.chat_message("assistant", avatar=ASSISTANT_AVATAR):
                    message_placeholder = st.empty()
                    full_response = ""
                    
                    try:
                        response = requests.post(f"{OLLAMA_ENDPOINT}/api/chat", json={
                            "model": "phi4-mini",
                            "messages": st.session_state.sre_messages,
                            "stream": True 
                        }, stream=True)
                        
                        if response.status_code == 200:
                            for line in response.iter_lines():
                                if line:
                                    json_data = json.loads(line)
                                    if "message" in json_data and "content" in json_data["message"]:
                                        chunk = json_data["message"]["content"]
                                        full_response += chunk
                                        message_placeholder.markdown(full_response + " █")
                                        
                            message_placeholder.markdown(full_response)
                            st.session_state.sre_messages.append({"role": "assistant", "content": full_response})
                            
                        else:
                            message_placeholder.error(f"Terminated. Exit code: {response.status_code}")
                            
                    except requests.exceptions.RequestException as e:
                        message_placeholder.error("🚨 Connection to local LLM socket severed.")