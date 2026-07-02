# ATLAS Project: Local LLM Selection Analysis 

## 1. Executive Summary
This document outlines the architectural decision-making process for selecting the local Small Language Model (SLM) powering the **ATLAS Machine Learning Anomaly Diagnostic Engine**. 

The objective was to deploy a local AI model capable of ingesting raw telemetry anomaly payloads (CPU, Memory, Network metrics via ClickHouse) and generating a Root Cause Analysis (RCA) strictly formatted as JSON for the Streamlit dashboard.

**Selected Model:** `microsoft/Phi-4-mini-instruct` (via Ollama)

---

## 2. System Constraints & Requirements

To ensure the dashboard remains accessible to all developers on the team regardless of their hardware, the selected model had to meet strict operational constraints:

* **Compute Environment:** Must run efficiently on standard CPU-only laptops (no dedicated GPU assumption).
* **Memory Footprint:** Must leave enough RAM available to simultaneously run Docker containers (ClickHouse, PostgreSQL, Streamlit, Spark).
* **Format Rigidity:** Must strictly adhere to system prompt instructions and output 100% valid JSON. Extraneous conversational text (e.g., "Here is your analysis:") will break the UI parser.
* **Inference Speed:** Must deliver diagnostic results in under 15 seconds to maintain a responsive user experience.
* **Data Parsing:** Must exhibit high logic and reasoning capabilities to correlate multi-variable time-series metrics.

---

## 3. Evaluated Models (The Contenders)

We evaluated several state-of-the-art SLMs available on the Ollama ecosystem. 

### 3.1. Meta Llama 3.2 (3B)
* **Profile:** An excellent general-purpose SLM. 
* **Drawback:** While a strong all-rounder, benchmark data indicated lower performance in strict structural output and code/data parsing compared to Microsoft's architectures. 

### 3.2. Google Gemma 2 (2B)
* **Profile:** A highly optimized model recommended during early architectural reviews due to its low memory footprint.
* **Drawback:** Previous generations (1.1) struggled with strict JSON schema adherence. While Gemma 2 is vastly improved, it still exhibited a higher probability of hallucinating markdown formatting outside the requested JSON block.

### 3.3. Microsoft Phi-3 Family (Small / Medium)
* **Profile:** Industry leaders in logic and reasoning for their size class.
* **Drawback:** The Phi-3-Small (7B parameters, 2.05kg weight) and Phi-3.5-MoE (9.26kg weight) models proved too heavy for CPU-only inference, causing severe latency and memory exhaustion alongside the database containers.

---

## 4. Benchmark Analysis Matrix

*Data sourced from Hugging Face SLM Leaderboards (Focusing on structural and instruction-following metrics).*

| Model | Size Class | Memory Weight | Instruction Following | Data Parsing | CPU Viability |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Phi-4-Mini-Instruct** | **~3.8B** | **0.83 kg** | **73.78%** | **32.58%** | **Excellent** |
| Llama-3.2-3B-Instruct | 3B | 1.93 kg | 73.93% | 24.39% | Good |
| Phi-3-Small-8k-Instruct | 7B | 2.05 kg | 64.97% | 38.96% | Poor |
| Qwen-1.5-1.8B-Chat | 1.8B | 1.13 kg | 20.19% | 8.93% | Excellent |

---

## 5. Final Decision Rationale: Why Phi-4-Mini-Instruct?

`microsoft/Phi-4-mini-instruct` was selected as the optimal engine for the ATLAS dashboard for the following technical reasons:

1.  **Unmatched Footprint-to-Performance Ratio:** At a weight of only **0.83 kg**, it has the smallest memory footprint of the top contenders. It runs lightning-fast on CPU without starving the Docker containers of system resources.
2.  **Instruction Adherence (JSON Safety):** With a 73.78% instruction-following score, it possesses the rigidity required to output pure JSON payloads. It suppresses conversational filler, ensuring `json.loads()` operations in the Streamlit app never throw decode errors.
3.  **Exclusion of Reasoning Variants:** We explicitly rejected `phi4:mini-reasoning`. While reasoning models score higher on complex logic, they inject thousands of tokens of internal dialogue inside `<think>` tags before their final output. This completely breaks real-time UI parsers and creates unacceptable latency (3-5 minutes per request) on CPU hardware.
4.  **Advanced Logic Architecture:** It retains the dense logic pathways of the Phi-4 generation, allowing it to accurately deduce that a high CPU temperature coupled with dropping fan speeds indicates a hardware failure rather than a software loop.

## 6. Implementation Specifications

**Ollama Execution Command:**
```bash
ollama run phi4:mini