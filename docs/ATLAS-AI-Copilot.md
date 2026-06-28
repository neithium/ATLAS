 
#   ATLAS AI: Intelligent SRE Copilot & Root Cause Analysis

The ATLAS AI module is a localized, LLM-powered diagnostic layer integrated directly into the ATLAS Observability Dashboard. It utilizes a local instance of the `phi-4-mini` model via Ollama to provide zero-latency, privacy-preserving infrastructure analysis and interactive SRE assistance.

##   Core Features

### 1. Automated Root Cause Analysis (RCA) Engine
* **Context-Aware Diagnostics:** Automatically extracts the top 10 most severe anomalies from the ClickHouse `telemetry_ml_predictions` table.
* **Smart Prompting:** Strips noisy metadata (e.g., location, server generation) to optimize context window limits before feeding the JSON payload to the LLM.
* **Structured Mitigation:** Generates JSON-formatted responses containing an incident summary, root cause hypothesis, affected subsystems, and actionable bash commands for remediation.

### 2. ATLAS SRE Copilot (Gemini-Style Interface)
* **Persistent Chat History:** Seamlessly resumes previous troubleshooting sessions. All chat threads and messages are saved natively to the ATLAS PostgreSQL metadata database.
* **Dynamic Scoping:** UI is compartmentalized into a 1:3 ratio grid, allowing independent scrolling of thread histories without disrupting the main chat window.
* **Real-Time Streaming:** Responses are streamed chunk-by-chunk for a highly responsive, ChatGPT-like user experience.
* **Auto-Pruning:** `ON DELETE CASCADE` architecture ensures that deleting a chat session immediately purges all associated messages to keep the database lightweight.

---

##   Architecture & Tech Stack

* **LLM Backend:** [Ollama](https://ollama.com/) (running on the host machine).
* **Model:** `phi4-mini` (optimized for fast, technical reasoning).
* **Application Framework:** Streamlit (`app.py`, `ml_app.py`).
* **Telemetry Source:** ClickHouse (`clickhouse_connect`).
* **State/Memory Store:** PostgreSQL (`psycopg2-binary`).
* **Container Networking:** Connects via `host.docker.internal:11434` to securely bridge the Docker network to the host's Ollama socket.

---

##   Prerequisites & Setup

### 1. Host Machine (Ollama)
You must have Ollama installed and running on your host machine.
```bash
# Pull the required model
ollama run phi4-mini

```

*Note: Ensure your `OLLAMA_HOST` environment variable allows cross-origin requests if necessary, though Docker desktop typically handles `host.docker.internal` automatically.*

### 2. Container Dependencies

The AI layer requires `requests` for the Ollama REST API and `psycopg2-binary` for the persistent chat memory.

If you are running the stack for the first time, ensure your `Dockerfile` includes:

```dockerfile
RUN pip install --break-system-packages psycopg2-binary requests streamlit-option-menu

```

### 3. Database Schema

The chat history relies on two PostgreSQL tables. These are automatically deployed via `postgres-init.sql` on startup:

```sql
CREATE TABLE chat_sessions (
    session_id UUID PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE chat_messages (
    id SERIAL PRIMARY KEY,
    session_id UUID REFERENCES chat_sessions(session_id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
 

---

##   Usage

### Triggering an RCA

1. Navigate to the **ATLAS AI** tab in the global navigation menu.
2. Select the **AI Diagnostic Engine** sub-tab.
3. Click **Run AI Root Cause Analysis**. The system will aggregate the latest isolation-forest anomalies and generate a structured runbook.

### Interacting with the Copilot

1. Navigate to the **ATLAS Copilot** sub-tab.
2. Type a command (e.g., *"Write a bash script to clear zombie processes taking up port 80"*) in the input field.
3. A new thread will automatically generate a title based on your prompt and save to the left-hand history panel.
4. Click the **🗑️** icon next to any historical thread to permanently delete it.

---

##  Troubleshooting

**Error: "Connection Blocked: Unable to hit the Ollama server..."**

* **Cause 1:** The LLM timed out generating an RCA. (The `requests` timeout is strictly capped to prevent container hanging).
* **Cause 2:** Ollama is not running on your host machine, or port `11434` is blocked.
* **Fix:** Ensure Ollama is running (`ollama serve`). If analyzing massive batches of anomalies, consider reducing the payload DataFrame `head(N)` size in `ml_app.py`.

**Error: ModuleNotFoundError: No module named 'psycopg2'**

* **Cause:** The PostgreSQL connector wasn't installed in the Streamlit container.
* **Fix (Hot):** Run `docker exec -it atlas-analytics pip install psycopg2-binary --break-system-packages` and refresh the browser.

```

```