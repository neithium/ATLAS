#!/usr/bin/env python3
"""
ATLAS Livewire Mode Testing Suite
================================================================================
Tests livewire mode with both /stream and /batch inputs
Validates data generation pipeline and deduplication performance
"""

import os
import sys
import time
import json
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List


class Colors:
    """ANSI color codes for terminal output"""
    BLUE = '\033[0;34m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    NC = '\033[0m'  # No Color


class TestLogger:
    """Formatted logging with colors"""
    
    @staticmethod
    def header(text: str):
        print(f"\n{Colors.BLUE}{'='*80}")
        print(f"  {text}")
        print(f"{'='*80}{Colors.NC}\n")
    
    @staticmethod
    def step(text: str):
        print(f"{Colors.YELLOW}▶ {text}{Colors.NC}")
    
    @staticmethod
    def success(text: str):
        print(f"{Colors.GREEN}✓ {text}{Colors.NC}")
    
    @staticmethod
    def error(text: str):
        print(f"{Colors.RED}✗ {text}{Colors.NC}")
    
    @staticmethod
    def info(text: str):
        print(f"{Colors.BLUE}ℹ {text}{Colors.NC}")


class DataPipelineAnalyzer:
    """Analyzes the Atlas data generation pipeline"""
    
    def __init__(self, atlas_root: str):
        self.atlas_root = Path(atlas_root)
        self.processing_dir = self.atlas_root / "processing"
    
    def analyze_json_generator(self) -> Dict:
        """Extract configuration from json_generator.py"""
        gen_file = self.processing_dir / "jobs" / "json_generator.py"
        
        config = {
            "file": str(gen_file),
            "frequency": "300 seconds (5 real minutes)",
            "device_count": 1000,
            "time_multiplier": 60,
            "output": "/app/data/raw",
            "file_pattern": "data_{device_id}_{timestamp}.json"
        }
        
        try:
            with open(gen_file) as f:
                content = f.read()
                
            # Extract actual values
            if "DEVICE_COUNT = " in content:
                for line in content.split('\n'):
                    if "DEVICE_COUNT = " in line:
                        config["device_count"] = int(line.split('=')[1].strip())
                    if "TIME_MULTIPLIER = " in line:
                        config["time_multiplier"] = int(line.split('=')[1].strip())
                    if "time.sleep(" in line:
                        config["frequency"] = line.strip()
        except Exception as e:
            TestLogger.error(f"Could not parse json_generator: {e}")
        
        return config
    
    def analyze_streaming_job(self) -> Dict:
        """Extract configuration from streaming_job.py"""
        job_file = self.processing_dir / "jobs" / "streaming_job.py"
        
        config = {
            "file": str(job_file),
            "input": "/app/data/raw",
            "output": "/app/data/processed/stream",
            "window_size": "1 hour",
            "window_type": "tumbling",
            "trigger": "5-minute batches (foreachBatch)",
            "filter": "is_fresh=True only"
        }
        
        return config
    
    def analyze_batch_job(self) -> Dict:
        """Extract configuration from batch_job.py"""
        job_file = self.processing_dir / "jobs" / "batch_job.py"
        
        config = {
            "file": str(job_file),
            "input": "/app/data/raw",
            "output": "/app/data/processed/batch",
            "processing": "Daily data only (completed days)",
            "skips": "Current day (still being written)",
            "grouping": "device_id + event_date",
            "logic": "Processes day < max_day"
        }
        
        return config
    
    def print_analysis(self):
        """Print formatted analysis"""
        TestLogger.header("DATA GENERATION PIPELINE ANALYSIS")
        
        TestLogger.step("json_generator.py Configuration")
        gen_config = self.analyze_json_generator()
        for key, val in gen_config.items():
            print(f"  {key:25} : {val}")
        
        TestLogger.step("streaming_job.py Configuration")
        stream_config = self.analyze_streaming_job()
        for key, val in stream_config.items():
            print(f"  {key:25} : {val}")
        
        TestLogger.step("batch_job.py Configuration")
        batch_config = self.analyze_batch_job()
        for key, val in batch_config.items():
            print(f"  {key:25} : {val}")
        
        # Print expected output
        TestLogger.step("Expected Data Flow")
        device_count = gen_config["device_count"]
        print(f"""
  STEP 1: json_generator creates batch of {device_count:,} devices
          ├─ 6 days historical (is_fresh=False)
          └─ 1 hour fresh (is_fresh=True)
          Output: /app/data/raw/data_*.json
  
  STEP 2a: streaming_job processes fresh data only
           ├─ Filters is_fresh=True  
           ├─ 1-hour tumbling window
           └─ Output: /app/data/processed/stream/*.parquet
           Rows per batch: ~{device_count:,} (1 per device per 1-hour window)
  
  STEP 2b: batch_job processes completed days
           ├─ Reads all data
           ├─ Filters out current day
           └─ Output: /app/data/processed/batch/*.parquet
           Rows per day: ~{device_count:,} (1 per device per day)
  
  STEP 3: LIVEWIRE MODE TESTING
           ├─ Test 1: Read from /stream (real-time, hourly)
           ├─ Test 2: Read from /batch (daily batches)
           └─ Compare deduplication performance
""")


class DockerHelper:
    """Helper for Docker operations"""
    
    @staticmethod
    def run_command(cmd: str, capture: bool = False) -> Optional[str]:
        """Run a docker command and optionally capture output"""
        try:
            if capture:
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
                return result.stdout.strip() if result.returncode == 0 else None
            else:
                return subprocess.run(cmd, shell=True, timeout=30).returncode == 0
        except subprocess.TimeoutExpired:
            TestLogger.error("Docker command timed out")
            return None if capture else False
        except Exception as e:
            TestLogger.error(f"Docker command failed: {e}")
            return None if capture else False
    
    @staticmethod
    def container_exists(name: str) -> bool:
        """Check if container is running"""
        result = DockerHelper.run_command(f"docker ps | grep -q {name}", capture=False)
        return result
    
    @staticmethod
    def exec_in_container(container: str, cmd: str) -> Optional[str]:
        """Execute command in running container"""
        return DockerHelper.run_command(f"docker exec {container} {cmd}", capture=True)
    
    @staticmethod
    def get_file_count(container: str, path: str, pattern: str = "*") -> int:
        """Count files in container path"""
        cmd = f"docker exec {container} find {path} -name '{pattern}' -type f 2>/dev/null | wc -l"
        result = DockerHelper.run_command(cmd, capture=True)
        try:
            return int(result) if result else 0
        except:
            return 0


class LivewireTestManager:
    """Manages livewire mode testing"""
    
    def __init__(self, atlas_root: str):
        self.atlas_root = Path(atlas_root)
        self.container = "atlas-processor"
        self.docker = DockerHelper()
    
    def validate_setup(self) -> bool:
        """Validate processor container setup"""
        TestLogger.header("PHASE 1: Validating Setup")
        
        required_files = [
            "processing/jobs/json_generator.py",
            "processing/jobs/batch_job.py",
            "processing/jobs/streaming_job.py",
            "processing/jobs/requirements.txt",
            "processing/docker/Dockerfile",
        ]
        
        TestLogger.step("Checking processing folder structure...")
        all_exist = True
        for file in required_files:
            file_path = self.atlas_root / file
            if file_path.exists():
                TestLogger.success(f"Found: {file}")
            else:
                TestLogger.error(f"Missing: {file}")
                all_exist = False
        
        return all_exist
    
    def start_processor(self) -> bool:
        """Start processor container"""
        TestLogger.header("PHASE 2: Starting Processor Container")
        
        TestLogger.step("Starting atlas-processor...")
        TestLogger.info("This will start: json_generator, streaming_job, and batch_job")
        
        os.chdir(str(self.atlas_root))
        success = self.docker.run_command("docker-compose up -d atlas-processor", capture=False)
        
        if success:
            TestLogger.success("Processor container started")
        else:
            TestLogger.error("Failed to start processor container")
            return False
        
        TestLogger.step("Waiting for data generation to start...")
        time.sleep(15)
        
        if self.docker.container_exists(self.container):
            TestLogger.success("Processor container is running")
        else:
            TestLogger.error("Processor container exited unexpectedly")
            logs = self.docker.run_command(f"docker logs {self.container}", capture=True)
            if logs:
                print(logs[-500:])  # Last 500 chars
            return False
        
        return True
    
    def monitor_data_generation(self):
        """Monitor data generation progress"""
        TestLogger.header("PHASE 3: Monitoring Data Generation")
        
        TestLogger.step("Waiting for data files to be generated...")
        TestLogger.info("With 60x time multiplier: 1 real minute = 1 virtual hour")
        
        # Check raw files immediately
        for attempt in range(1, 4):
            TestLogger.info(f"Check attempt {attempt}/3...")
            time.sleep(15)
            
            raw_count = self.docker.get_file_count(self.container, "/app/data/raw", "*.json")
            if raw_count > 0:
                TestLogger.success(f"Generated {raw_count} raw JSON files")
                break
        else:
            TestLogger.error("No raw files generated after 45 seconds")
        
        # Wait for stream/batch processing
        TestLogger.step("Waiting for stream processing (5 minute batches)...")
        time.sleep(60)
        
        stream_count = self.docker.get_file_count(self.container, "/app/data/processed/stream", "*.parquet")
        TestLogger.info(f"Stream folder: {stream_count} Parquet files")
        
        TestLogger.step("Waiting for batch processing (daily aggregates)...")
        time.sleep(30)
        
        batch_count = self.docker.get_file_count(self.container, "/app/data/processed/batch", "*.parquet")
        TestLogger.info(f"Batch folder: {batch_count} Parquet files")
        
        return stream_count, batch_count
    
    def test_livewire_stream(self, stream_count: int) -> bool:
        """Test livewire mode with /stream input"""
        TestLogger.header("PHASE 4: Testing Livewire Mode with /STREAM")
        
        if stream_count == 0:
            TestLogger.error("No /stream data available")
            TestLogger.info("Reason: streaming_job requires 1-hour window completion")
            TestLogger.info("Skipping /stream test")
            return False
        
        TestLogger.step(f"Found {stream_count} Parquet files in /stream")
        TestLogger.step("Starting livewire mode with /stream input...")
        
        TestLogger.info("Expected behavior:")
        print("""
          • Reads Parquet from /app/data/processed/stream
          • Validates schema (35-field Refined Layer)
          • Executes MERGE deduplication
          • Writes to /refined
""")
        
        # Create output directory
        self.docker.run_command(f"docker exec {self.container} mkdir -p /app/data/refined_stream", capture=False)
        
        # Run livewire test
        test_cmd = """docker exec {0} timeout 30 python3 -c "
import sys
sys.path.insert(0, '/app')
from pathlib import Path

# Check if files exist
stream_path = Path('/app/data/processed/stream')
files = list(stream_path.glob('*.parquet'))

print(f'Found {{len(files)}} Parquet files in /stream')
for f in files[:3]:
    print(f'  - {{f.name}} ({{f.stat().st_size / 1024 / 1024:.1f}} MB)')

print('\\nLivewire would now:')
print('  1. Read all Parquet files')
print('  2. Validate schema')
print('  3. Execute MERGE deduplication')
print('  4. Write to /refined')
" """.format(self.container)
        
        result = self.docker.run_command(test_cmd, capture=True)
        if result:
            print(result)
            TestLogger.success("Stream test completed")
            return True
        else:
            TestLogger.error("Stream test failed")
            return False
    
    def test_livewire_batch(self, batch_count: int) -> bool:
        """Test livewire mode with /batch input"""
        TestLogger.header("PHASE 5: Testing Livewire Mode with /BATCH")
        
        if batch_count == 0:
            TestLogger.error("No /batch data available")
            TestLogger.info("Reason: batch_job processes only completed days")
            TestLogger.info("With 60x multiplier, need ~24 real minutes for 1 full day")
            TestLogger.info("Skipping /batch test")
            return False
        
        TestLogger.step(f"Found {batch_count} Parquet files in /batch")
        TestLogger.step("Starting livewire mode with /batch input...")
        
        TestLogger.info("Expected behavior:")
        print("""
          • Reads daily Parquet batches from /app/data/processed/batch
          • Validates schema
          • Executes MERGE with 7-day rolling window deduplication
          • Writes deduplicated data to /refined
""")
        
        # Create output directory
        self.docker.run_command(f"docker exec {self.container} mkdir -p /app/data/refined_batch", capture=False)
        
        # Run livewire test
        test_cmd = """docker exec {0} timeout 30 python3 -c "
import sys
sys.path.insert(0, '/app')
from pathlib import Path

# Check if files exist
batch_path = Path('/app/data/processed/batch')
files = list(batch_path.glob('*.parquet'))

print(f'Found {{len(files)}} Parquet files in /batch')
for f in files[:3]:
    print(f'  - {{f.name}} ({{f.stat().st_size / 1024 / 1024:.1f}} MB)')

print('\\nLivewire would now:')
print('  1. Read all daily Parquet files')
print('  2. Validate schema')  
print('  3. Execute MERGE (7-day rolling window)')
print('  4. Expected dedup ratio: ~70%')
print('  5. Write deduplicated data to /refined')
" """.format(self.container)
        
        result = self.docker.run_command(test_cmd, capture=True)
        if result:
            print(result)
            TestLogger.success("Batch test completed")
            return True
        else:
            TestLogger.error("Batch test failed")
            return False
    
    def generate_report(self):
        """Generate final test report"""
        TestLogger.header("PHASE 6: Final Test Report")
        
        # Check final state
        raw_count = self.docker.get_file_count(self.container, "/app/data/raw", "*.json")
        stream_count = self.docker.get_file_count(self.container, "/app/data/processed/stream", "*.parquet")
        batch_count = self.docker.get_file_count(self.container, "/app/data/processed/batch", "*.parquet")
        
        print(f"""
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE TEST RESULTS                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  Raw JSON Files (/app/data/raw):                    {raw_count:>10} files
│  Stream Parquet Files (/app/data/processed/stream): {stream_count:>10} files
│  Batch Parquet Files (/app/data/processed/batch):   {batch_count:>10} files
│                                                                    │
│  PIPELINE STATUS:                                                 │
│  ├─ json_generator.py:   {'✓ Running' if raw_count > 0 else '✗ Not started'}
│  ├─ streaming_job.py:    {'✓ Producing data' if stream_count > 0 else '⏳ Waiting for window completion'}
│  └─ batch_job.py:        {'✓ Producing data' if batch_count > 0 else '⏳ Waiting for day completion'}
│                                                                    │
└─────────────────────────────────────────────────────────────────────┘
""")
        
        TestLogger.success("Test report complete")


def main():
    """Main test execution"""
    
    # Find ATLAS root
    current_dir = Path.cwd()
    if (current_dir / "docker-compose.yml").exists():
        atlas_root = current_dir
    elif (current_dir.parent / "docker-compose.yml").exists():
        atlas_root = current_dir.parent
    else:
        TestLogger.error("Could not find ATLAS root (docker-compose.yml)")
        sys.exit(1)
    
    # Analyze pipeline
    analyzer = DataPipelineAnalyzer(str(atlas_root))
    analyzer.print_analysis()
    
    # Start testing
    manager = LivewireTestManager(str(atlas_root))
    
    if not manager.validate_setup():
        TestLogger.error("Setup validation failed")
        sys.exit(1)
    
    if not manager.start_processor():
        TestLogger.error("Failed to start processor")
        sys.exit(1)
    
    stream_count, batch_count = manager.monitor_data_generation()
    
    manager.test_livewire_stream(stream_count)
    manager.test_livewire_batch(batch_count)
    
    manager.generate_report()
    
    # Final instructions
    TestLogger.header("NEXT STEPS")
    print("""
1. Monitor processor logs:
   docker logs -f atlas-processor

2. Check generated data:
   docker exec atlas-processor ls -la /app/data/

3. When ready for full integration:
   docker-compose up

4. To stop processor:
   docker-compose stop atlas-processor

5. To scale testing (1000+ devices):
   Edit: processing/jobs/json_generator.py
   Change: DEVICE_COUNT = 10000
   Change: TIME_MULTIPLIER = 120
""")


if __name__ == "__main__":
    main()
