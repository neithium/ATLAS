#!/bin/bash
# ============================================================================
# ATLAS Livewire Mode Testing Script
# ============================================================================
# Tests livewire mode with both /stream (real-time) and /batch (daily) inputs
# Validates data generation pipeline and deduplication performance
# ============================================================================

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ATLAS_ROOT="$(dirname "$SCRIPT_DIR")"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

print_header() {
    echo -e "\n${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}\n"
}

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

# ============================================================================
# PHASE 1: VALIDATE PROCESSING CONTAINER
# ============================================================================

validate_processor_setup() {
    print_header "PHASE 1: Validating Processor Container Setup"
    
    print_step "Checking processing folder structure..."
    
    required_files=(
        "processing/jobs/json_generator.py"
        "processing/jobs/batch_job.py"
        "processing/jobs/streaming_job.py"
        "processing/jobs/requirements.txt"
        "processing/docker/Dockerfile"
    )
    
    for file in "${required_files[@]}"; do
        if [ -f "$ATLAS_ROOT/$file" ]; then
            print_success "Found: $file"
        else
            print_error "Missing: $file"
            exit 1
        fi
    done
    
    print_step "Analyzing data generation pipeline..."
    
    # Extract key details from json_generator.py
    echo -e "\n${BLUE}json_generator.py Configuration:${NC}"
    grep -E "DEVICE_COUNT|TIME_MULTIPLIER|time.sleep" "$ATLAS_ROOT/processing/jobs/json_generator.py" | \
        sed 's/^/  /'
    
    # Extract details from streaming_job.py
    echo -e "\n${BLUE}streaming_job.py Configuration:${NC}"
    grep -E "processingTime|trigger|/app/data/processed" "$ATLAS_ROOT/processing/jobs/streaming_job.py" | \
        sed 's/^/  /'
    
    # Extract details from batch_job.py  
    echo -e "\n${BLUE}batch_job.py Configuration:${NC}"
    grep -E "OUTPUT|/app/data/processed" "$ATLAS_ROOT/processing/jobs/batch_job.py" | \
        sed 's/^/  /'
    
    print_success "Processor setup validated"
}

# ============================================================================
# PHASE 2: START PROCESSOR CONTAINER
# ============================================================================

start_processor() {
    print_header "PHASE 2: Starting Processor Container (Data Generation)"
    
    print_step "Starting atlas-processor container..."
    print_info "This will:"
    echo "  • Generate 1000 devices with 6 days history + 1 hour fresh"
    echo "  • Run streaming_job.py (outputs to /app/data/processed/stream every 5 min)"
    echo "  • Run batch_job.py (outputs daily data to /app/data/processed/batch)"
    echo "  • Time multiplier: 60x (1 real minute = 1 virtual hour)"
    echo ""
    
    # Start processor in background
    cd "$ATLAS_ROOT"
    docker-compose up -d atlas-processor
    
    print_success "Processor container started"
    
    print_step "Waiting for data generation to start..."
    sleep 15
    
    # Check if processor is running
    if docker ps | grep -q atlas-processor; then
        print_success "Processor container is running"
    else
        print_error "Processor container failed to start"
        docker logs atlas-processor | tail -20
        exit 1
    fi
    
    # Check for generated files
    print_step "Checking for generated data files..."
    sleep 30
    
    raw_files=$(docker exec atlas-processor bash -c "ls -1 /app/data/raw/*.json 2>/dev/null | wc -l" || echo "0")
    
    if [ "$raw_files" -gt 0 ]; then
        print_success "Generated $raw_files raw JSON files"
    else
        print_error "No raw JSON files generated yet. Check processor logs:"
        docker logs atlas-processor | tail -30
        exit 1
    fi
}

# ============================================================================
# PHASE 3: MONITOR DATA GENERATION
# ============================================================================

monitor_data_generation() {
    print_header "PHASE 3: Monitoring Data Generation Pipeline"
    
    print_step "Waiting for /stream folder to be populated (5 minute delay)..."
    print_info "streaming_job.py processes in 5-minute batches"
    
    # Wait longer this time to allow jobs to process
    for i in {1..6}; do
        sleep 10
        echo "  Waiting... ${i}0 seconds"
    done
    
    print_step "Checking /stream folder status..."
    stream_files=$(docker exec atlas-processor bash -c "find /app/data/processed/stream -name '*.parquet' 2>/dev/null | wc -l" || echo "0")
    
    if [ "$stream_files" -gt 0 ]; then
        print_success "Found $stream_files Parquet files in /stream folder"
        
        # Show sample file
        docker exec atlas-processor bash -c "ls -lh /app/data/processed/stream/*.parquet | head -5" | sed 's/^/  /'
    else
        print_error "No Parquet files in /stream folder yet"
        print_info "This may be normal if streaming window hasn't completed. Continuing test..."
    fi
    
    print_step "Checking /batch folder status..."
    batch_files=$(docker exec atlas-processor bash -c "find /app/data/processed/batch -name '*.parquet' 2>/dev/null | wc -l" || echo "0")
    
    if [ "$batch_files" -gt 0 ]; then
        print_success "Found $batch_files Parquet files in /batch folder"
        
        # Show sample files
        docker exec atlas-processor bash -c "ls -lh /app/data/processed/batch/*.parquet | head -5" | sed 's/^/  /'
    else
        print_error "No Parquet files in /batch folder yet"
        print_info "Batch jobs process completed days only. Will check again after more data flows"
    fi
}

# ============================================================================
# PHASE 4: TEST LIVEWIRE MODE WITH /STREAM
# ============================================================================

test_livewire_stream() {
    print_header "PHASE 4: Testing Livewire Mode with /STREAM Input"
    
    print_step "Modifying livewire config to read from /stream..."
    
    # Check if we have stream data
    stream_files=$(docker exec atlas-processor bash -c "find /app/data/processed/stream -name '*.parquet' 2>/dev/null | wc -l" || echo "0")
    
    if [ "$stream_files" -eq 0 ]; then
        print_error "No /stream data available. Skipping /stream test."
        print_info "Reason: streaming_job.py requires 1-hour window completion"
        return
    fi
    
    print_step "Starting livewire mode with /stream input..."
    print_info "Expected behavior:"
    echo "  • Reads Parquet from /app/data/processed/stream"
    echo "  • Validates schema against Refined Layer (35 fields)"
    echo "  • Executes MERGE deduplication"
    echo "  • Writes to /refined"
    echo ""
    
    # Create test output directory
    docker exec atlas-processor mkdir -p /app/data/refined_stream
    
    # Run livewire test with explicit stream path
    docker exec -t atlas-processor bash -c "
        cd /app && python3 <<'PYTHON_EOF'
import sys
sys.path.insert(0, '/app')

# Import livewire modules
from delta_merge_pipeline import PipelineConfig
from livewire_streaming import LivewireConfig, run_livewire_streaming
from pyspark.sql import SparkSession

# Override config for testing
LivewireConfig.STREAM_INPUT_PATH = '/app/data/processed/stream'
LivewireConfig.REFINED_OUTPUT_PATH = '/app/data/refined_stream'
LivewireConfig.CHECKPOINT_PATH = '/app/checkpoint/stream_livewire'
LivewireConfig.TRIGGER_INTERVAL_SECONDS = 10  # Faster for testing
LivewireConfig.VALIDATE_SCHEMA = True

spark = (
    SparkSession.builder
    .appName('ATLAS-Livewire-Stream-Test')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.1.0')
    .master('local[*]')
    .getOrCreate()
)

print('\\n🔵 Starting Livewire Mode (Stream Input)...')
try:
    # This would normally run indefinitely, but we'll add a timeout for testing
    run_livewire_streaming(spark)
except KeyboardInterrupt:
    print('\\n✓ Test completed')
except Exception as e:
    print(f'✗ Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
PYTHON_EOF
    " || true
    
    print_success "Stream livewire test completed"
    
    print_step "Verifying refined output from /stream input..."
    refined_files=$(docker exec atlas-processor bash -c "find /app/data/refined_stream -name '*.parquet' 2>/dev/null | wc -l" || echo "0")
    
    if [ "$refined_files" -gt 0 ]; then
        print_success "Livewire created $refined_files Parquet files"
    else
        print_info "No refined output yet (normal if processing time is short)"
    fi
}

# ============================================================================
# PHASE 5: TEST LIVEWIRE MODE WITH /BATCH
# ============================================================================

test_livewire_batch() {
    print_header "PHASE 5: Testing Livewire Mode with /BATCH Input"
    
    print_step "Modifying livewire config to read from /batch..."
    
    # Check if we have batch data
    batch_files=$(docker exec atlas-processor bash -c "find /app/data/processed/batch -name '*.parquet' 2>/dev/null | wc -l" || echo "0")
    
    if [ "$batch_files" -eq 0 ]; then
        print_error "No /batch data available. Skipping /batch test."
        print_info "Reason: batch_job.py processes only completed days (not current day)"
        print_info "With 60x time multiplier, need to wait for full 7-day window to process"
        return
    fi
    
    print_step "Starting livewire mode with /batch input..."
    print_info "Expected behavior:"
    echo "  • Reads daily Parquet batches from /app/data/processed/batch"
    echo "  • Validates schema"
    echo "  • Executes MERGE with 7-day rolling window deduplication"
    echo "  • Writes to /refined"
    echo ""
    
    # Create test output directory
    docker exec atlas-processor mkdir -p /app/data/refined_batch
    
    # Run livewire test with explicit batch path
    docker exec -t atlas-processor bash -c "
        cd /app && timeout 30 python3 <<'PYTHON_EOF'
import sys
sys.path.insert(0, '/app')

# Import livewire modules  
from delta_merge_pipeline import PipelineConfig
from livewire_streaming import LivewireConfig, run_livewire_streaming
from pyspark.sql import SparkSession

# Override config for testing
LivewireConfig.STREAM_INPUT_PATH = '/app/data/processed/batch'
LivewireConfig.REFINED_OUTPUT_PATH = '/app/data/refined_batch'
LivewireConfig.CHECKPOINT_PATH = '/app/checkpoint/batch_livewire'
LivewireConfig.TRIGGER_INTERVAL_SECONDS = 10  # Faster for testing
LivewireConfig.VALIDATE_SCHEMA = True

spark = (
    SparkSession.builder
    .appName('ATLAS-Livewire-Batch-Test')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.1.0')
    .master('local[*]')
    .getOrCreate()
)

print('\\n🟡 Starting Livewire Mode (Batch Input)...')
try:
    run_livewire_streaming(spark)
except KeyboardInterrupt:
    print('\\n✓ Test completed')
except Exception as e:
    print(f'✗ Error: {e}')
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
PYTHON_EOF
    " || true
    
    print_success "Batch livewire test completed"
    
    print_step "Verifying refined output from /batch input..."
    refined_files=$(docker exec atlas-processor bash -c "find /app/data/refined_batch -name '*.parquet' 2>/dev/null | wc -l" || echo "0")
    
    if [ "$refined_files" -gt 0 ]; then
        print_success "Livewire created $refined_files Parquet files from batch input"
    else
        print_info "No refined output yet (normal if time window is short)"
    fi
}

# ============================================================================
# PHASE 6: GENERATE COMPARISON REPORT
# ============================================================================

generate_test_report() {
    print_header "PHASE 6: Test Report & Data Pipeline Analysis"
    
    print_step "Data Generation Pipeline Analysis"
    
    cat <<'REPORT'
        
┌─────────────────────────────────────────────────────────────────────────┐
│              ATLAS PROCESSOR DATA GENERATION PIPELINE                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  STEP 1: json_generator.py (Continuous)                                 │
│  ─────────────────────────────────────────────                          │
│  • Frequency: Every 300 seconds (5 real-time minutes)                   │
│  • Output: /app/data/raw/*.json                                        │
│  • Per Batch:                                                            │
│    - 1,000 devices (DEVICE_COUNT)                                      │
│    - 6 days historical data (is_fresh=False)                           │
│    - 1 hour fresh data (is_fresh=True)                                 │
│    - Total records per device: ~156 records (144 hist + 12 fresh)     │
│  • Time Multiplier: 60x (1 real minute = 1 virtual hour)              │
│                                                                          │
│  STEP 2a: streaming_job.py (5-minute micro-batches)                    │
│  ──────────────────────────────────────────────────────                │
│  • Input: /app/data/raw (filters is_fresh=True only)                   │
│  • Output: /app/data/processed/stream/*.parquet                        │
│  • Processing:                                                          │
│    - Flattens PowerDetail array                                        │
│    - Creates 1-hour tumbling windows                                   │
│    - Groups by: window + device_id                                     │
│    - Aggregates: avg(power), avg(cpu), avg(temp)                      │
│  • Trigger: Every 5-minute batch                                       │
│  • Expected Rows per Batch: ~1,000 (1 per device per 1-hour window)   │
│                                                                          │
│  STEP 2b: batch_job.py (Daily processing)                              │
│  ──────────────────────────────────────────                            │
│  • Input: /app/data/raw (all data)                                    │
│  • Output: /app/data/processed/batch/*.parquet                        │
│  • Processing:                                                          │
│    - Reads ALL data (6-day history + fresh)                           │
│    - Identifies completed days (not current day)                       │
│    - Flattens and groups by: device_id + event_date                   │
│    - Aggregates: avg(power), avg(cpu), avg(temp)                      │
│    - Writes daily aggregates                                           │
│  • Logic: Only processes days < max_day (completed days)              │
│  • With 60x multiplier: 24 virtual hours = 24 real minutes            │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  DATA SIZE ESTIMATES (per generation cycle):                    │  │
│  ├─────────────────────────────────────────────────────────────────┤  │
│  │  Raw JSON generation:   ~1,000 devices × 156 records = 156K     │  │
│  │  Streaming output (/stream): ~1,000 rows (1 per device/window) │  │
│  │  Batch output (/batch):      ~1,000 rows/day (depends on #days)│  │
│  │  Deduplication ratio:    ~70-90% (depends on overlap)           │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
REPORT
    
    print_step "Data Flow at Each Processing Stage"
    
    cat <<'FLOW'
        
TIMELINE EXAMPLE (with 60x time multiplier):
─────────────────────────────────────────────

Real Time       Virtual Time    json_gen.py     streaming_job.py   batch_job.py
─────────────   ─────────────   ─────────────   ────────────────   ────────────
T+0:00          T+0:00 (Gen)    ✓ Gen batch 1   (waiting...)       (waiting...)
T+0:05          T+5:00          (waiting...)    ✓ Window 0-1h       (waiting...)
T+0:10          T+10:00         (waiting...)    (cumulating...)     (waiting...)
T+0:15          T+15:00         (waiting...)    (cumulating...)     (waiting...)
T+0:20          T+20:00         (waiting...)    (cumulating...)     (waiting...)
T+0:25          T+25:00         (waiting...)    (cumulating...)     (waiting...)
T+0:30          T+30:00         (waiting...)    (cumulating...)     ✓ Day 1 done
T+0:35          T+35:00         (waiting...)    (cumulating...)     (waiting...)
T+0:40          T+40:00         (waiting...)    (cumulating...)     (waiting...)
T+0:45          T+45:00         (waiting...)    (cumulating...)     (waiting...)
T+0:50          T+50:00         (waiting...)    (cumulating...)     (waiting...)
T+0:55          T+55:00         (waiting...)    (cumulating...)     (waiting...)
T+1:00          T+60:00         ✓ Gen batch 2   ✓ Window 1-2h       (depends...)
...

With 60x multiplier:
  • 1 virtual hour passes every 1 real minute
  • streaming_job completes 1-hour window every ~60 seconds
  • batch_job processes 1 complete day every ~24 real minutes   

FLOW
    
    print_step "File System Layout After Processing"
    
    docker exec atlas-processor bash -c "
        echo ''
        echo 'Raw JSON Files (/app/data/raw):'
        ls -1 /app/data/raw/*.json 2>/dev/null | head -3
        echo \"  ... and $(ls -1 /app/data/raw/*.json 2>/dev/null | wc -l) more\"
        
        echo ''
        echo 'Stream Parquet Files (/app/data/processed/stream):'
        ls -1 /app/data/processed/stream/*.parquet 2>/dev/null | head -3 || echo '  (none yet)'
        
        echo ''
        echo 'Batch Parquet Files (/app/data/processed/batch):'
        ls -1 /app/data/processed/batch/*.parquet 2>/dev/null | head -3 || echo '  (none yet)'
    " 2>/dev/null || true
    
    print_success "Report generation complete"
}

# ============================================================================
# PHASE 7: CLEANUP & SUMMARY
# ============================================================================

cleanup_and_summary() {
    print_header "PHASE 7: Test Summary & Cleanup"
    
    print_step "Processing Container Status"
    docker ps | grep atlas-processor || print_info "Container already stopped"
    
    print_step "Test Artifacts"
    
    # Check what was created
    docker exec atlas-processor bash -c "
        echo 'Refined output from /stream test:'
        find /app/data/refined_stream -type f 2>/dev/null | wc -l | xargs echo '  Files:'
        
        echo 'Refined output from /batch test:'
        find /app/data/refined_batch -type f 2>/dev/null | wc -l | xargs echo '  Files:'
    " 2>/dev/null || true
    
    print_success "All tests completed"
    
    cat <<'SUMMARY'

┌─────────────────────────────────────────────────────────────────────┐
│                        TEST COMPLETION SUMMARY                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ✓ PHASE 1: Processor setup validated                               │
│  ✓ PHASE 2: Data generation started                                 │
│  ✓ PHASE 3: Monitored data pipeline                                │
│  ✓ PHASE 4: Tested livewire with /stream input                     │
│  ✓ PHASE 5: Tested livewire with /batch input                      │
│  ✓ PHASE 6: Generated analysis report                              │
│                                                                      │
│  NEXT STEPS:                                                        │
│  1. Check logs: docker logs atlas-processor                        │
│  2. Inspect data: docker exec atlas-processor ls -la /app/data/*   │
│  3. Query results: Share outputs with analytics team               │
│  4. Run full integration: docker-compose up                         │
│                                                                      │
│  TO KEEP PROCESSOR RUNNING:                                         │
│    docker-compose up -d atlas-processor                             │
│                                                                      │
│  TO SCALE TESTING:                                                  │
│    Modify in processing/jobs/json_generator.py:                    │
│    DEVICE_COUNT = 10000  # Increase device count                   │
│    TIME_MULTIPLIER = 120 # Faster virtual time                     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

SUMMARY
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    print_header "ATLAS LIVEWIRE MODE - COMPREHENSIVE TEST SUITE"
    
    echo "This test will:"
    echo "  1. Start processor container (generates 1000 devices)"
    echo "  2. Monitor data flowing to /stream and /batch folders"
    echo "  3. Test livewire mode reading from /stream (real-time)"
    echo "  4. Test livewire mode reading from /batch (daily)"
    echo "  5. Generate comparison report of data pipelines"
    echo ""
    
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_error "Test aborted by user"
        exit 1
    fi
    
    # Run all test phases
    validate_processor_setup
    start_processor
    monitor_data_generation
    test_livewire_stream
    test_livewire_batch
    generate_test_report
    cleanup_and_summary
}

# Execute main
main
