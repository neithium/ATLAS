import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import json
import os
import re
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import uuid


PROMPT_FILE_PATH = "/app/prompts/rca_system_prompt.txt"
ASSET_DIR = Path(__file__).resolve().parent / "assets" / "img"
 
# --- PostgreSQL Connection Setup ---
PG_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_USER = os.getenv("POSTGRES_USER", "atlas")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "atlas_secure_pwd")
PG_DB = os.getenv("POSTGRES_DB", "atlas_metadata")

def get_pg_connection():
    """Establish a connection to the local PostgreSQL metadata store."""
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB
    )

def create_chat_session(session_id, title):
    """Initialize a new chat thread in the database."""
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chat_sessions (session_id, title) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (session_id, title)
            )
def delete_chat_session(session_id):
    """Delete a chat thread and all its associated messages."""
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM chat_sessions WHERE session_id = %s", (session_id,))
            
def save_chat_message(session_id, role, content):
    """Save a single message to the current thread."""
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chat_messages (session_id, role, content) VALUES (%s, %s, %s)",
                (session_id, role, content)
            )

def load_chat_sessions():
    """Retrieve all past chat threads for the sidebar."""
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT session_id, title FROM chat_sessions ORDER BY created_at DESC")
            return cur.fetchall()

def load_chat_messages(session_id):
    """Retrieve all messages for a specific thread."""
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT role, content FROM chat_messages WHERE session_id = %s ORDER BY created_at ASC",
                (session_id,)
            )
            return cur.fetchall()

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
    
def fetch_total_predictions_count(ch_client):
    """Get the total number of inferences in the entire table."""
    query = "SELECT count() AS total FROM atlas.telemetry_ml_predictions"
    try:
        df = ch_client.query_df(query)
        return int(df.iloc[0]['total'])
    except Exception:
        return 0

def fetch_total_anomalies_count(ch_client):
    """Get the total number of anomalies in the entire table."""
    query = "SELECT count() AS total FROM atlas.telemetry_ml_predictions WHERE prediction = -1"
    try:
        df = ch_client.query_df(query)
        return int(df.iloc[0]['total'])
    except Exception:
        return 0

def fetch_global_health_score(ch_client):
    """Calculate the true average health score across the entire fleet."""
    query = "SELECT avg(health_score) AS avg_health FROM atlas.telemetry_ml_predictions"
    try:
        df = ch_client.query_df(query)
        # Handle cases where the table is empty and returns NaN
        val = df.iloc[0]['avg_health']
        return float(val) if pd.notna(val) else 0.0
    except Exception:
        return 0.0
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
    # KPI Metrics (Global Fleet View)
    # -------------------------------------------------------------------------
    total_predictions = fetch_total_predictions_count(ch_client)
    total_anomalies = fetch_total_anomalies_count(ch_client)
    global_avg_health = fetch_global_health_score(ch_client)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Total Predictions Scanned", value=f"{total_predictions:,}")
    with col2:
        st.metric(label="Total Anomalies Detected", value=f"{total_anomalies:,}")
    with col3:
        st.metric(label="Global Fleet Health Score", value=f"{global_avg_health:.1f} / 100")

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
        st.markdown("**Fleet Health Analysis**")
        
        if 'health_score' in df_recent.columns and 'prediction' in df_recent.columns:
            plot_df = df_recent.copy()
            plot_df['Status'] = plot_df['prediction'].map({1: 'Normal', -1: 'Anomaly'})
            
            # 1. Let the user choose the X-axis!
            available_axes = {
                "Server Name (Fleet View)": "server_name",
                "ML Anomaly Score": "anomaly_score",
                "CPU Utilization": "cpu_utilization",
                "Memory Utilization": "memory_utilization",
                "Network Throughput": "network_throughput"
            }
            
            selected_view = st.selectbox(
                "Analyze Health Score against:", 
                options=list(available_axes.keys()),
                index=0
            )
            
            x_col = available_axes[selected_view]
            
            # 2. Render the dynamic chart
            fig = px.scatter(
                plot_df, 
                x=x_col, 
                y="health_score", 
                color="Status",
                color_discrete_map={"Normal": "#00CC96", "Anomaly": "#EF553B"},
                hover_data=["device_id", "server_name", "metric_time"],
                title=f"Health Score vs. {selected_view.split('(')[0].strip()}",
                template="plotly_dark",
                size_max=12 # Keeps dots uniform
            )
            
            # Add a bit of jitter if they choose a categorical column like server_name to prevent overlap
            if plot_df[x_col].dtype == 'object':
                fig.update_traces(marker=dict(size=10, line=dict(width=1, color='DarkSlateGrey')))
            else:
                fig.update_traces(marker=dict(size=8, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
                
            st.plotly_chart(fig, use_container_width=True)

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
    # TAB 4: SRE Copilot Terminal (With Local Scoped History)
    # -------------------------------------------------------------------------
    with tab_terminal:
        st.markdown("###### Chat with ATLAS Copilot")
        st.caption("Engage directly with ATLAS Copilot. Ask for command explanations, custom scripts, or deeper analysis.")
        
        # 1. Initialize core session state
        if "session_id" not in st.session_state:
            st.session_state.session_id = str(uuid.uuid4())
            st.session_state.is_new_session = True
            st.session_state.sre_messages = [
                {
                    "role": "system", 
                    "content": "You are ATLAS, an elite AI Site Reliability Engineering copilot. Speak directly to the user. NEVER simulate a conversation between a 'User' and 'Assistant'. NEVER prefix your responses. Be concise, technical, and robotic."
                }
            ]

        # --- THE FIX: Create a local grid layout (1/4 for history, 3/4 for chat) ---
        history_col, chat_col = st.columns([1, 3], gap="small")

        # 2. Local "Sidebar" (Chat History) scoped only to this tab
        with history_col:
            if st.button(" New Chat", use_container_width=True, type="primary"):
                st.session_state.session_id = str(uuid.uuid4())
                st.session_state.is_new_session = True
                st.session_state.sre_messages = [
                    {"role": "system", "content": "You are ATLAS, an elite AI Site Reliability Engineering copilot."}
                ]
                st.rerun()
                
            st.markdown("---")
            st.markdown("**Recent Threads**")
            
             
           # Wrap the history in a fixed-height container so it scrolls independently
            with st.container(height=500, border=False):
                past_sessions = load_chat_sessions()
                
                for session in past_sessions:
                    # Create a mini-grid for each row: 5 parts for the name, 1 part for the trash can
                    btn_col, del_col = st.columns([5, 1])
                    
                    with btn_col:
                        if st.button(f" {session['title']}", key=f"load_{session['session_id']}", use_container_width=True):
                            st.session_state.session_id = str(session['session_id'])
                            st.session_state.is_new_session = False
                            
                            # Load historical messages into state
                            db_messages = load_chat_messages(session['session_id'])
                            st.session_state.sre_messages = [{"role": "system", "content": "You are ATLAS..."}]
                            for msg in db_messages:
                                st.session_state.sre_messages.append({"role": msg['role'], "content": msg['content']})
                            st.rerun()
                            
                    with del_col:
                        # Trash can button
                        if st.button("🗙", key=f"del_{session['session_id']}", help="Delete this chat"):
                            delete_chat_session(str(session['session_id']))
                            
                            # If the user just deleted the chat they are currently looking at, reset the screen
                            if st.session_state.session_id == str(session['session_id']):
                                st.session_state.session_id = str(uuid.uuid4())
                                st.session_state.is_new_session = True
                                st.session_state.sre_messages = [
                                    {"role": "system", "content": "You are ATLAS, an elite AI Site Reliability Engineering copilot."}
                                ]
                            st.rerun()
            
        # 3. Main Chat UI
        with chat_col:
            # Fixed height for the message area
            terminal_container = st.container(height=650)
            
            with terminal_container:
                for msg in st.session_state.sre_messages:
                    if msg["role"] != "system":
                        avatar_icon = ASSISTANT_AVATAR if msg["role"] == "assistant" else USER_AVATAR
                        with st.chat_message(msg["role"], avatar=avatar_icon):
                            st.markdown(msg["content"])
                        
            # 4. Handle New Input (Because it's inside chat_col, it pins to the bottom of the column!)
            if user_prompt := st.chat_input("Enter command or query..."):
                
                # If this is the very first message of a new thread, save the session to the DB
                if st.session_state.get("is_new_session", False):
                    # Create a short title from the first prompt (max 30 chars)
                    thread_title = user_prompt[:30] + "..." if len(user_prompt) > 30 else user_prompt
                    create_chat_session(st.session_state.session_id, thread_title)
                    st.session_state.is_new_session = False
                
                # Save user message
                st.session_state.sre_messages.append({"role": "user", "content": user_prompt})
                save_chat_message(st.session_state.session_id, "user", user_prompt)
                
                with terminal_container:
                    with st.chat_message("user", avatar=USER_AVATAR):
                        st.markdown(user_prompt)
                        
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
                                
                                # Save AI message to state and DB
                                st.session_state.sre_messages.append({"role": "assistant", "content": full_response})
                                save_chat_message(st.session_state.session_id, "assistant", full_response)
                                
                            else:
                                message_placeholder.error(f"Terminated. Exit code: {response.status_code}")
                                
                        except requests.exceptions.RequestException:
                            message_placeholder.error("  Connection to local LLM socket severed.")