
 
#  ATLAS Observability Dashboard

The ATLAS Observability Dashboard is a high-performance, real-time front-end interface built with Streamlit. It serves as the primary "Command Center" for Site Reliability Engineers (SREs), offering deep introspection into the platform's multi-tier storage architecture and real-time visualization of high-frequency server telemetry.

---

##  Features Listing

1. **Global Sidebar & Live Health Probes:** Persistent system navigation with real-time connectivity status for all underlying datastores.
2. **ClickHouse Explorer:** Zero-configuration introspection of the OLAP telemetry layer, featuring automated schema parsing and live data previews.
3. **PostgreSQL Explorer:** Direct window into the relational metadata layer, enabling quick audits of system configurations and state.
4. **Dynamic Live Charts Builder:** An interactive, schema-aware time-series visualizer that allows engineers to build custom Plotly charts on the fly without writing SQL.
5. **Delta Lake Micro-SLA Dashboard:** Autonomous streaming metrics tracking the latency, throughput, and anomalies of the underlying Delta Lake merge pipeline.
6. **ATLAS AI & SRE Copilot:** A localized, LLM-powered diagnostic layer featuring targeted Root Cause Analysis (RCA) and a persistent, ChatGPT-style troubleshooting interface.

---

##  Feature Breakdown & Mechanics

### 1. Global Sidebar & System Health
**What it does:** Provides a unified, persistent navigation menu and instantly alerts operators to infrastructure outages.
**How it works:** * **Custom UI:** Replaces standard Streamlit radio buttons with `streamlit-option-menu`, utilizing custom CSS injections to force native OS fonts (`system-ui`), rounded touch-targets, and interactive hover states.
* **Health Probes:** On every page load, the app attempts lightweight connections to ClickHouse, PostgreSQL, and the Delta Lake directory. The results dynamically render as `🟢 Online` or `🔴 Offline` status badges directly in the sidebar.

### 2. ClickHouse & PostgreSQL Explorers
**What it does:** Allows engineers to peek inside the raw tables of both the OLAP (ClickHouse) and OLTP (Postgres) databases without needing a separate database client like DBeaver or DataGrip.
**How it works:**
* **Introspection:** Uses `information_schema` (Postgres) and `SHOW TABLES / DESCRIBE TABLE` (ClickHouse) to dynamically fetch available tables.
* **Caching:** Wraps queries in `@st.cache_data(ttl=60)` to prevent the dashboard from hammering the databases if multiple users are clicking around, while ensuring schema changes are reflected within a minute.
* **Serialization Safety:** Automatically detects and casts complex types (like Postgres UUIDs) to strings to prevent Apache Arrow serialization crashes during Streamlit dataframe rendering.

### 3. Dynamic Live Charts Builder
**What it does:** Transforms raw ClickHouse telemetry into interactive, dark-mode time-series charts. 
**How it works:**
* **Schema-Aware Filtering:** When a user selects a table, the app parses the schema and categorizes the columns. It explicitly restricts the **X-Axis** dropdown to `Date|DateTime` columns and the **Y-Axis** to `Int|Float` columns, preventing users from attempting to plot incompatible data types.
* **Real-Time Rendering:** Fetches data ordered by time descending, applying user-defined lookback windows (e.g., Last 1,000 rows). 
* **Cache Busting:** Features a manual `🔄 Refresh Data Now` button that executes `get_ch_chart_data.clear()`, bypassing the Streamlit cache to force an immediate pull of live anomalies.
* **Visualization:** Renders using `plotly.express` with the `plotly_dark` template for seamless integration with the dashboard's aesthetic.

### 4. Delta Lake SLA & Diagnostics Dashboard
**What it does:** Proves the sub-second latency of the streaming pipeline independently of upstream APIs, tracking exactly how long it takes the PySpark engine to deduplicate and merge incoming telemetry.
**How it works:**
* **Direct File System Reads:** Bypasses external databases to read directly from the `_delta_log` and Parquet files in the `/refined/system_metrics` volume using the `deltalake` Python library.
* **Pre-Computed Quantiles:** Reads the last 500 processing batches into Pandas and computes the **P50, P95, and P99** latencies in memory, displaying them in top-level metric cards.
* **Component Breakdown:** Uses Plotly Pie Charts to visually split the total pipeline time between `Prep & Partitioning` and the actual `MERGE Operation`.
* **Outlier Detection:** Automatically isolates batches where the `total_time` exceeds the P99 threshold, filtering them into a dedicated "Outliers & Anomalies" tab so engineers can immediately see which specific Spark batches struggled.

### 5. ATLAS AI: Targeted Device Forensics & SRE Copilot
**What it does:** Allows SREs to run automated anomaly diagnostics on specific hardware hardware and interact with an elite AI assistant for remediation steps, complete with chat history.
**How it works:**
* **Chronological Forensics:** A dropdown isolates specific failing devices (`device_id`). The UI fetches the chronological history of the last 50 ticks to visually plot state degradation (e.g., `health_score` crashing) over time on a dynamic trendline.
* **Automated RCA Generation:** Strips noisy metadata from the telemetry timeline to fit LLM context windows. It sends this chronological JSON payload via REST API to a local Ollama instance (`phi-4-mini`). The LLM strictly returns a JSON-formatted Root Cause Analysis containing an incident summary, affected subsystems, and actionable bash remediation commands.
* **Persistent Copilot Memory:** The Gemini/ChatGPT-style chat interface utilizes a dual-table PostgreSQL schema (`chat_sessions`, `chat_messages`) with `ON DELETE CASCADE`. Deleting a chat thread from the UI instantly purges all associated messages from the database.
* **Dynamic Scoping:** Employs a 1:3 screen ratio container to render a scrollable historical chat sidebar that operates independently of the active conversation window.
* **Streaming Responses:** Yields AI text chunk-by-chunk using `requests` streaming and Streamlit's `st.write_stream()`, ensuring a zero-latency, highly responsive user experience.

---

##  Technology Stack

* **Framework:** [Streamlit](https://streamlit.io/) (Python)
* **Charting:** [Plotly Express](https://plotly.com/python/plotly-express/) & Plotly Graph Objects
* **Database Drivers:** * `clickhouse-connect` (Native HTTP ClickHouse driver)
  * `psycopg2-binary` (PostgreSQL driver for metadata and Chat Memory)
  * `deltalake` (Native Rust-based Delta Lake reader)
* **AI Integration:** `requests` (REST API to host machine Ollama socket)
* **Data Manipulation:** Pandas & NumPy
* **UI Components:** `streamlit-option-menu` with Bootstrap Icons

---

##  Execution & Setup

The dashboard is fully containerized within the `atlas-analytics` Docker service.

### Environment Variables
The dashboard relies on the following environment variables (passed via `docker-compose.yml`):
* `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`
* `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

### Container Networking (AI)
To securely bridge the Docker network to the host's Ollama socket for AI inference, the container relies on `host.docker.internal:11434`.

### Running the App Locally (Outside Docker)
If you wish to run the UI locally for development, ensure your local machine has access to the database ports, then execute:

```bash
pip install -r requirements.txt
streamlit run app.py

```
 