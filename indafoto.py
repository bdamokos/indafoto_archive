try:
    import os
    import niquests as requests
    import sqlite3
    import time
    import logging
    from tqdm import tqdm
    from bs4 import BeautifulSoup
    from urllib.parse import unquote, urlparse
    import re
    from datetime import datetime
    import threading
    import queue
    import argparse
    import hashlib
    import shutil
    from pathlib import Path
    import portalocker
    import json
    import sys
    import subprocess
    import signal
    import atexit
    from io import BytesIO
    import psutil
    import tracemalloc
    import ssl
    import random
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please install the required modules using the requirements.txt file:\n")
    print("pip install -r requirements.txt")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('indafoto_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_for_updates(filename=None):
    """Check if there's a newer version of the script available on GitHub.
    
    Args:
        filename: Optional filename to check. If None, checks the current script.
    """
    try:
        # Get local file info
        if filename is None:
            filename = __file__
        
        # Get absolute path and basename consistently
        local_path = os.path.abspath(filename)
        base_filename = os.path.basename(local_path)
        
        logger.info(f"Checking for updates for {base_filename}")
        
        # Get local file info
        local_mtime = datetime.fromtimestamp(os.path.getmtime(local_path))
        
        with open(local_path, 'r', encoding='utf-8') as f:
            local_content = f.read()
        
        # Get the latest version info from GitHub API first
        api_url = "https://api.github.com/repos/bdamokos/indafoto_archive/commits"
        api_params = {
            "path": base_filename,  # Use consistent basename
            "page": 1,
            "per_page": 1
        }
        api_response = requests.get(api_url, params=api_params, timeout=5)
        
        if api_response.status_code == 200:
            commit_info = api_response.json()[0]
            github_date = datetime.strptime(commit_info['commit']['committer']['date'], "%Y-%m-%dT%H:%M:%SZ")
            
            # Get the file content from GitHub using consistent basename
            github_url = f"https://raw.githubusercontent.com/bdamokos/indafoto_archive/main/{base_filename}"
            response = requests.get(github_url, timeout=5)
            
            if response.status_code == 200:
                github_content = response.text
                
                # Normalize line endings for comparison
                local_content = local_content.replace('\r\n', '\n')
                github_content = github_content.replace('\r\n', '\n')
                
                if local_content != github_content:
                    print("\n" + "="*80)
                    
                    # Compare dates to determine if local is newer or older
                    if local_mtime > github_date:
                        print(f"NOTE: Your version of {base_filename} is newer than the one on GitHub!")
                        print(f"Your version date:   {local_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"GitHub version date: {github_date.strftime('%Y-%m-%d %H:%M:%S')}")
                        print("\nThis might mean:")
                        print("1. You have local modifications")
                        print("2. Your changes haven't been pushed to GitHub yet")
                        print("3. The file timestamps are incorrect")
                    else:
                        print(f"UPDATE AVAILABLE for {base_filename}!")
                        print(f"Your version date:   {local_mtime.strftime('%Y-%m-%d %H:%M:%S')}")
                        print(f"GitHub version date: {github_date.strftime('%Y-%m-%d %H:%M:%S')}")
                        print("\nTo see what changed:")
                        print(f"https://github.com/bdamokos/indafoto_archive/commits/main/{base_filename}")
                        print("\nTo update:")
                        print("1. If you used git clone:")
                        print("   Run: git pull")
                        print("\n2. If you downloaded the ZIP:")
                        print("   Download the latest version from:")
                        print("   https://github.com/bdamokos/indafoto_archive/archive/refs/heads/main.zip")
                    
                    print("="*80 + "\n")
                    
                    # Give user a chance to read the message
                    time.sleep(5)
    except Exception as e:
        logger.warning(f"Failed to check for updates for {base_filename}: {e}")

def monitor_system_resources(interval=5, report_functions=False):
    """Start a background thread that monitors system resources.
    
    Args:
        interval: How often to log stats (in seconds)
        report_functions: Whether to attempt to identify CPU-heavy functions
    """
    import psutil
    import threading
    import time
    import tracemalloc
    from datetime import datetime
    import platform
    
    # Start memory tracking if reporting functions
    if report_functions:
        tracemalloc.start()
    
    # Get process object
    process = psutil.Process()
    stop_monitoring = {"value": False}
    
    def monitor_thread():
        logger.info("Starting system resource monitoring")
        logger.info(f"System: {platform.system()} {platform.version()}")
        logger.info(f"CPU: {platform.processor()}")
        logger.info(f"Cores: {psutil.cpu_count(logical=False)} physical, {psutil.cpu_count()} logical")
        
        # Track consecutive high CPU measurements
        high_cpu_count = 0
        cpu_threshold = 80.0  # Consider CPU high if above this percentage
        
        cpu_history = []
        thread_history = []
        memory_history = []
        disk_io_history = []
        
        # Counter for full stats output
        stat_counter = 0
        full_stats_interval = 10  # Show full stats every X intervals
        
        while not stop_monitoring["value"]:
            try:
                stat_counter += 1
                show_full_stats = (stat_counter % full_stats_interval == 0)
                
                # Get the timestamp
                current_time = datetime.now().strftime("%H:%M:%S")
                
                # Memory usage
                mem_info = process.memory_info()
                memory_mb = mem_info.rss / (1024 * 1024)
                
                # CPU usage (percentage across all cores)
                process_cpu_percent = process.cpu_percent(interval=0.5)
                system_cpu_percent = psutil.cpu_percent(interval=0)
                per_core_cpu = psutil.cpu_percent(interval=0, percpu=True)
                
                # Thread count and details
                threads = process.threads()
                thread_count = len(threads)
                # Sort threads by CPU time
                threads_sorted = sorted(threads, key=lambda t: t.user_time + t.system_time, reverse=True)
                
                # Disk I/O
                try:
                    io_counters = process.io_counters()
                    io_read_mb = io_counters.read_bytes / (1024 * 1024)
                    io_write_mb = io_counters.write_bytes / (1024 * 1024)
                    
                    # Calculate IO rates if we have history
                    if disk_io_history:
                        last_io = disk_io_history[-1]
                        time_diff = time.time() - last_io[0]
                        if time_diff > 0:
                            io_read_rate = (io_read_mb - last_io[1]) / time_diff
                            io_write_rate = (io_write_mb - last_io[2]) / time_diff
                        else:
                            io_read_rate = io_write_rate = 0
                    else:
                        io_read_rate = io_write_rate = 0
                    
                    disk_io_history.append((time.time(), io_read_mb, io_write_mb))
                except (psutil.Error, AttributeError) as e:
                    # Log the specific error that occurred with disk I/O
                    logger.debug(f"Failed to get process disk I/O stats: {str(e)}")
                    io_read_mb = io_write_mb = io_read_rate = io_write_rate = 0
                
                # Network I/O - Get process-specific network I/O if available
                try:
                    # Attempt to get process-specific network counters (not supported on all platforms)
                    net_io = process.net_connections()
                    # If we got here, we have connections but not bytes, so use system counters
                    net_io = psutil.net_io_counters()
                    net_sent_mb = net_io.bytes_sent / (1024 * 1024)
                    net_recv_mb = net_io.bytes_recv / (1024 * 1024)
                    # Add a note that this is system-wide
                    net_io_note = " (system-wide)"
                except (psutil.Error, AttributeError) as e:
                    logger.debug(f"Failed to get network I/O stats: {str(e)}")
                    net_sent_mb = net_recv_mb = 0
                    net_io_note = ""
                
                # Record stats
                timestamp = time.time()
                cpu_history.append((timestamp, process_cpu_percent))
                thread_history.append((timestamp, thread_count))
                memory_history.append((timestamp, memory_mb))
                
                # Only keep recent history
                max_history = 60  # Last 5 minutes at 5-second intervals
                if len(cpu_history) > max_history:
                    cpu_history.pop(0)
                    thread_history.pop(0)
                    memory_history.pop(0)
                
                if len(disk_io_history) > max_history:
                    disk_io_history.pop(0)
                
                # Calculate averages
                avg_cpu = sum(item[1] for item in cpu_history) / len(cpu_history)
                avg_threads = sum(item[1] for item in thread_history) / len(thread_history)
                avg_memory = sum(item[1] for item in memory_history) / len(memory_history)
                
                # Standard stats line for regular updates
                logger.info(
                    f"[{current_time}] CPU: {process_cpu_percent:.1f}% ({system_cpu_percent:.1f}% sys), "
                    f"Threads: {thread_count}, Mem: {memory_mb:.1f}MB, "
                    f"IO: {io_read_rate:.2f}MB/s read, {io_write_rate:.2f}MB/s write"
                )
                
                # Full stats block (every X intervals)
                if show_full_stats:
                    # Create a nicely formatted block for detailed stats
                    stats_block = [
                        "┌─────────────────────────────────────────────────────────────────┐",
                        f"│ SYSTEM STATISTICS REPORT                      {current_time}    │",
                        "├─────────────────────────────────────────────────────────────────┤",
                        f"│ CPU USAGE                                                       │",
                        f"│   Process:       {process_cpu_percent:5.1f}% (avg: {avg_cpu:5.1f}%)                        │",
                        f"│   System:        {system_cpu_percent:5.1f}%                                      │",
                        "│   Per Core:      " + " ".join(f"{cpu:3.0f}%" for cpu in per_core_cpu[:8]) + "   │",
                    ]
                    
                    if len(per_core_cpu) > 8:
                        stats_block.append("│              " + " ".join(f"{cpu:3.0f}%" for cpu in per_core_cpu[8:16]) + "   │")
                    
                    stats_block.extend([
                        "├─────────────────────────────────────────────────────────────────┤",
                        f"│ MEMORY                                                          │",
                        f"│   RSS:           {memory_mb:8.1f} MB                                   │",
                        f"│   Virtual:       {mem_info.vms / (1024*1024):8.1f} MB                                   │",
                        f"│   Percent:       {psutil.virtual_memory().percent:5.1f}%                                      │",
                        "├─────────────────────────────────────────────────────────────────┤",
                        f"│ THREADS (Total: {thread_count:3d}, avg: {avg_threads:5.1f})                              │"
                    ])
                    
                    # Add top 5 threads by CPU usage
                    for i, t in enumerate(threads_sorted[:5]):
                        cpu_time = t.user_time + t.system_time
                        # Handle platforms where thread status isn't available
                        try:
                            thread_status = t.status
                        except AttributeError:
                            thread_status = "unknown"
                        stats_block.append(f"│   #{i+1}: ID {t.id:6d} - CPU time: {cpu_time:7.2f}s, Status: {thread_status:10s} │")
                    
                    stats_block.extend([
                        "├─────────────────────────────────────────────────────────────────┤",
                        f"│ DISK I/O (Process-specific)                                      │",
                        f"│   Read:          {io_read_mb:8.1f} MB (Rate: {io_read_rate:6.2f} MB/s)             │",
                        f"│   Write:         {io_write_mb:8.1f} MB (Rate: {io_write_rate:6.2f} MB/s)             │",
                        "├─────────────────────────────────────────────────────────────────┤",
                        f"│ NETWORK I/O{net_io_note}                                          │",
                        f"│   Sent:          {net_sent_mb:8.1f} MB                                   │",
                        f"│   Received:      {net_recv_mb:8.1f} MB                                   │",
                        "└─────────────────────────────────────────────────────────────────┘"
                    ])
                    
                    # Log the entire block
                    for line in stats_block:
                        logger.info(line)
                
                # Check for high CPU
                if process_cpu_percent > cpu_threshold:
                    high_cpu_count += 1
                    if high_cpu_count >= 3 and report_functions:  # Three consecutive high readings
                        # Take a memory snapshot to find possible hotspots
                        snapshot = tracemalloc.take_snapshot()
                        top_stats = snapshot.statistics('lineno')
                        
                        logger.warning(f"High CPU detected ({process_cpu_percent:.1f}%). Top memory consumers:")
                        for i, stat in enumerate(top_stats[:5]):  # Show top 5
                            logger.warning(f"  #{i+1}: {stat}")
                        
                        high_cpu_count = 0  # Reset counter
                else:
                    high_cpu_count = 0
                
                # Sleep for the interval
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in resource monitor: {e}")
                time.sleep(interval)  # Still sleep on error
    
    # Start monitor thread
    monitor_t = threading.Thread(target=monitor_thread)
    monitor_t.daemon = True
    monitor_t.start()
    
    # Return function to stop monitoring
    return lambda: setattr(stop_monitoring, "value", True)

RESTART_INTERVAL = 3600*24 # 24 hours in seconds
last_restart_time = time.time()
restart_timer = None
should_restart = False

restart_lock = threading.Lock()
restart_in_progress = False

def restart_script(error_restart=False):
    """Restart the script while preserving the current state.
    
    Args:
        error_restart: If True, restart regardless of auto-restart setting (for error handling)
                       unless --no-error-restart is specified
    """
    global should_restart, restart_timer, args, restart_in_progress
    
    # Use the lock to ensure only one thread can attempt restart
    with restart_lock:
        # Check if restart is already in progress
        if restart_in_progress:
            logger.info("Restart already in progress, skipping additional restart attempt")
            return
            
        # Skip restart if auto-restart is disabled and this is not an error restart
        # OR if this is an error restart but no-error-restart is enabled
        if (not error_restart and not args.auto_restart) or (error_restart and args.no_error_restart):
            restart_type = "error-based" if error_restart else "time-based"
            logger.info(f"{restart_type.capitalize()} restart is disabled, continuing without restart")
            return
            
        # Set the flag to prevent other threads from initiating restart
        restart_in_progress = True
        
    should_restart = True
    restart_type = "error-based" if error_restart else "time-based"
    logger.info(f"Initiating {restart_type} script restart...")
    
    # Cancel the restart timer if it exists
    if restart_timer:
        restart_timer.cancel()
    
    # Get the current page number from the database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    try:
        # First try to find the page with the latest completion date
        cursor.execute("""
            SELECT page_number 
            FROM completed_pages
            ORDER BY completion_date DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            next_page = result[0] + 1
            logger.info(f"Restarting from page {next_page} (based on latest completion date)")
        else:
            # Fall back to the highest page number if no completion date is found
            cursor.execute("""
                SELECT MAX(page_number) 
                FROM completed_pages
            """)
            last_page = cursor.fetchone()[0]
            next_page = (last_page + 1) if last_page is not None else 0
            logger.info(f"Restarting from page {next_page} (based on highest page number)")
    except Exception as e:
        logger.error(f"Error getting last page: {e}")
        next_page = 0
    finally:
        conn.close()
    
    # Prepare the command with the same arguments
    cmd = [str(Path(sys.executable))] + sys.argv
    # Update or add the start-offset argument
    start_offset_found = False
    for i, arg in enumerate(cmd):
        if arg == '--start-offset':
            cmd[i + 1] = str(next_page)
            start_offset_found = True
            break
    if not start_offset_found:
        cmd.extend(['--start-offset', str(next_page)])
    
    # Clean up all threads and processes
    try:
        # Stop all thread pools if they exist
        global metadata_pool, download_pool, validation_pool
        if 'metadata_pool' in globals() and metadata_pool is not None:
            metadata_pool.shutdown()
        if 'download_pool' in globals() and download_pool is not None:
            download_pool.shutdown()
        if 'validation_pool' in globals() and validation_pool is not None:
            validation_pool.shutdown()
        
        # Close all database connections
        try:
            if 'conn' in locals():
                conn.close()
        except:
            pass
        
        # Kill all child processes
        current_process = psutil.Process()
        children = current_process.children(recursive=True)
        for child in children:
            try:
                child.terminate()
            except:
                pass
        
        # Give them some time to terminate gracefully
        _, alive = psutil.wait_procs(children, timeout=3)
        
        # Force kill if still alive
        for p in alive:
            try:
                p.kill()
            except:
                pass
                
        logger.info("Successfully cleaned up all threads and processes")
        
        # Run redownload_missing.py to clean up any corrupted downloads
        logger.info("Running redownload_missing.py to fix any missing or corrupted downloads...")
        redownload_cmd = [str(Path(sys.executable)), "redownload_missing.py"]
        # Use subprocess.run to wait for the redownload process to complete
        try:
            subprocess.run(redownload_cmd, check=True)
            logger.info("Successfully completed redownload process")
        except subprocess.SubprocessError as e:
            logger.error(f"Error during redownload process: {e}")
        
        # Start the new process with platform-appropriate creation flags
        creation_flags = 0
        if sys.platform == 'win32':
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP
        
        subprocess.Popen(cmd, creationflags=creation_flags if sys.platform == 'win32' else 0)
        logger.info("New process started successfully")
        
        # Exit the current process
        os._exit(0)  # Use os._exit() to ensure immediate termination
        
    except Exception as cleanup_error:
        logger.error(f"Error during cleanup: {cleanup_error}")
        # Still try to start new process and exit
        try:
            # Try to run redownload_missing.py even if there were cleanup errors
            try:
                logger.info("Attempting to run redownload_missing.py despite cleanup errors...")
                subprocess.run([str(Path(sys.executable)), "redownload_missing.py"], check=False)
            except Exception as re_error:
                logger.error(f"Failed to run redownload_missing.py: {re_error}")
                
            subprocess.Popen(cmd, creationflags=creation_flags if sys.platform == 'win32' else 0)
            logger.info("New process started successfully despite cleanup errors")
        finally:
            # Reset restart flag in case exit fails
            restart_in_progress = False
            os._exit(0)  # Ensure we exit even if new process start fails

def check_restart_timer():
    """Check if it's time to restart the script."""
    global last_restart_time, restart_timer
    current_time = time.time()
    if current_time - last_restart_time >= RESTART_INTERVAL:
        logger.info("Restart interval reached, initiating restart...")
        restart_script()
    else:
        # Schedule next check
        restart_timer = threading.Timer(60, check_restart_timer)
        restart_timer.start()

def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    if restart_timer:
        restart_timer.cancel()
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Register cleanup function
atexit.register(lambda: restart_timer.cancel() if restart_timer else None)

# Configurations
# If you want to save images with different characteristics, you can change the search URL template and the total number of pages.
SEARCH_URL_TEMPLATE = "https://indafoto.hu/search/list?profile=main&sphinx=1&search=advanced&textsearch=fulltext&textuser=&textmap=&textcompilation=&photo=&datefrom=&dateto=&licence%5B1%5D=I1%3BI2%3BII1%3BII2&licence%5B2%5D=I1%3BI2%3BI3&page_offset={}"
BASE_DIR = "indafoto_archive"
DB_FILE = "indafoto.db"
TOTAL_PAGES = 14267
BASE_RATE_LIMIT = 1  # Base seconds between requests
BASE_TIMEOUT = 60    # Base timeout in seconds
FILES_PER_DIR = 1000  # Maximum number of files per directory
ARCHIVE_SAMPLE_RATE = 0.005  # 0.5% sample rate for Internet Archive submissions
DEFAULT_WORKERS = 8  # Default number of parallel download workers
BLOCK_SIZE = 2097152  # 2MB block size for maximum performance with 0.5-3MB images

# Adaptive rate limiting and worker scaling configuration
MIN_WORKERS = 1  # Minimum number of workers
INITIAL_WAIT_TIME = 300  # 5 minutes in seconds
MAX_WAIT_TIME = 3600  # 1 hour in seconds
FAILURE_THRESHOLD = 5  # Number of consecutive failures before reducing workers
SUCCESS_THRESHOLD = 3  # Number of consecutive successes before increasing workers
WORKER_SCALE_STEP = 2  # How many workers to add when scaling up

# Failure tracking
consecutive_failures = 0
consecutive_successes = 0
current_wait_time = 0
current_workers = DEFAULT_WORKERS  # Will be updated by command line argument
MAX_WORKERS = DEFAULT_WORKERS  # Will be updated by command line argument

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none'
}

# Cookies required for authentication and consent
COOKIES = {
    'PHPSESSID': '8tbh4kro71s41a17r2figg0j36',
    'ident': '67d53b898a86171abd8b4592',
    'INX_CHECKER2': '1',
    'euconsent-v2': 'CQOT54AQOT54AAKA7AHUBhFsAP_gAEPgAA6gK7NX_G__bWlr8X73aftkeY1P99h77sQxBgbJE-4FzLvW_JwXx2E5NAz6tqIKmRIAu3TBIQNlHJDURVCgaogVrSDMaEyUoTNKJ6BkiFMRI2dYCFxvm4tjeQCY5vr991dx2B-t7dr83dzyy4hHn3a5_2S1WJCdAYetDfv9bROb-9IOd_x8v4v4_F7pE2-eS1l_tWvp7D9-Yts_9X299_bbff5Pn__ul_-_X_vf_n37v94wV2AJMNCogjLIkJCJQMIIEAKgrCAigQBAAAkDRAQAmDApyBgAusJEAIAUAAwQAgABBgACAAASABCIAKACgQAAQCBQABgAQDAQAMDAAGACwEAgABAdAxTAggECwASMyKBTAlAASCAlsqEEgCBBXCEIs8AgAREwUAAAIABSAAIDwWBxJICViQQBcQTQAAEAAAQQAFCKTswBBAGbLUXgyfRlaYFg-YJmlMAyAIgjIyTYhN-gAAAA.YAAAAAAAAAAA',
    'usprivacy': '1---'
}

# Ensure base directory exists
os.makedirs(BASE_DIR, exist_ok=True)

# Add at the top with other global variables
banned_authors_set = set()
current_page = 0  # Track current page number

def create_session(max_retries=3, pool_connections=4):
    """Create a standardized session optimized for HTTP/1.1 with TLS optimizations"""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(COOKIES)
    
    # Set session-level timeout
    session.timeout = 30
    
    # Configure retry strategy
    retry_strategy = requests.adapters.Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    
    # Create adapter with connection pooling
    adapter = requests.adapters.HTTPAdapter(
        pool_maxsize=pool_connections,
        pool_block=True,
        max_retries=retry_strategy
    )
    
    # Mount the adapter for both HTTP and HTTPS
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    # Configure TLS settings
    if hasattr(adapter, 'init_poolmanager'):
        adapter.init_poolmanager(
            connections=pool_connections,
            maxsize=pool_connections,
            block=True,
            ssl_version=ssl.PROTOCOL_TLS,
            # Enable session reuse
            ssl_context=ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=None,
                capath=None,
                cadata=None
            )
        )
    
    return session

def init_db():
    """Initialize the database with required tables."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Dynamically calculate SQLite memory parameters based on available system RAM
    try:
        # Get available system memory in bytes
        available_memory = psutil.virtual_memory().available
        total_memory = psutil.virtual_memory().total
        
        # Calculate cache size (in KB) - use up to 15% of available memory, max 200MB
        cache_kb = min(int(available_memory * 0.15 / 1024), 200 * 1024)
        
        # Calculate mmap size (in bytes) - use up to 10% of available memory, max 500MB
        mmap_bytes = min(int(available_memory * 0.10), 500 * 1024 * 1024)
        
        logger.info(f"System memory: {total_memory/1024/1024/1024:.2f}GB total, {available_memory/1024/1024/1024:.2f}GB available")
        logger.info(f"SQLite memory allocation: {cache_kb/1024:.2f}MB cache, {mmap_bytes/1024/1024:.2f}MB mmap")
    except Exception as e:
        # Fallback to conservative values if memory detection fails
        logger.warning(f"Could not determine system memory: {e}")
        cache_kb = 20000  # 20MB
        mmap_bytes = 20 * 1024 * 1024  # 20MB
    
    # Set performance optimizing PRAGMAs with dynamic memory values
    cursor.execute("PRAGMA journal_mode=WAL")  # Use Write-Ahead Logging for better concurrency
    cursor.execute("PRAGMA synchronous=NORMAL")  # Reduce fsync calls while maintaining safety
    cursor.execute(f"PRAGMA cache_size=-{cache_kb}")  # Dynamic cache size (-ve means kilobytes)
    cursor.execute("PRAGMA temp_store=MEMORY")  # Store temp tables and indices in memory
    cursor.execute(f"PRAGMA mmap_size={mmap_bytes}")  # Dynamic memory-mapped I/O
    cursor.execute("PRAGMA page_size=4096")  # Optimal page size for most systems
    cursor.execute("PRAGMA busy_timeout=60000")  # Wait up to 60 seconds for locks
    
    # Create tables if they don't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid TEXT UNIQUE,
        url TEXT,
        local_path TEXT,
        sha256_hash TEXT,
        title TEXT,
        description TEXT,
        author TEXT,
        author_url TEXT,
        license TEXT,
        camera_make TEXT,
        camera_model TEXT,
        focal_length TEXT,
        aperture TEXT,
        shutter_speed TEXT,
        taken_date TEXT,
        upload_date TEXT,
        page_url TEXT
    )
    """)
    
    # Create index on url for faster lookups
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_images_url ON images(url)
    """)
    
    # Add description column if it doesn't exist (for backwards compatibility)
    try:
        cursor.execute("ALTER TABLE images ADD COLUMN description TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Add camera_model column if it doesn't exist (for backwards compatibility)
    try:
        cursor.execute("ALTER TABLE images ADD COLUMN camera_model TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Add sha256_hash column if it doesn't exist (for backwards compatibility)
    try:
        cursor.execute("ALTER TABLE images ADD COLUMN sha256_hash TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Add upload_date column if it doesn't exist (for backwards compatibility)
    try:
        cursor.execute("ALTER TABLE images ADD COLUMN upload_date TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Create banned_authors table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS banned_authors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        author TEXT UNIQUE,
        reason TEXT,
        banned_date TEXT,
        banned_by TEXT
    )
    """)
    
    # Load banned authors into memory
    cursor.execute("SELECT author FROM banned_authors")
    global banned_authors_set
    banned_authors_set = {row[0] for row in cursor.fetchall()}
    
    # Create collections table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS collections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection_id TEXT UNIQUE,
        title TEXT,
        url TEXT,
        is_public BOOLEAN
    )
    """)
    
    # Create albums table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS albums (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        album_id TEXT UNIQUE,
        title TEXT,
        url TEXT,
        is_public BOOLEAN
    )
    """)
    
    # Create image_collections junction table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS image_collections (
        image_id INTEGER,
        collection_id INTEGER,
        FOREIGN KEY (image_id) REFERENCES images (id),
        FOREIGN KEY (collection_id) REFERENCES collections (id),
        PRIMARY KEY (image_id, collection_id)
    )
    """)

    # Create image_albums junction table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS image_albums (
        image_id INTEGER,
        album_id INTEGER,
        FOREIGN KEY (image_id) REFERENCES images (id),
        FOREIGN KEY (album_id) REFERENCES albums (id),
        PRIMARY KEY (image_id, album_id)
    )
    """)
    
    # Create tags table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        count INTEGER  -- number of images with this tag on indafoto
    )
    """)
    
    # Create image_tags junction table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS image_tags (
        image_id INTEGER,
        tag_id INTEGER,
        FOREIGN KEY (image_id) REFERENCES images (id),
        FOREIGN KEY (tag_id) REFERENCES tags (id),
        PRIMARY KEY (image_id, tag_id)
    )
    """)
    
    # Create archive_submissions table if not exists
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS archive_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        submission_date TEXT,
        type TEXT,  -- 'author_page', 'image_page', 'gallery_page'
        status TEXT,
        archive_url TEXT,
        error TEXT,  -- Store error messages
        retry_count INTEGER DEFAULT 0,
        last_attempt TEXT
    )
    """)
    
    # Add retry_count column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE archive_submissions ADD COLUMN retry_count INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add last_attempt column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE archive_submissions ADD COLUMN last_attempt TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Create failed_pages table to track pages that need retry
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS failed_pages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        page_number INTEGER UNIQUE,
        url TEXT,
        error TEXT,
        attempts INTEGER DEFAULT 1,
        last_attempt TEXT,
        status TEXT DEFAULT 'pending'  -- 'pending', 'retried', 'success', 'failed'
    )
    """)
    
    # Create completed_pages table to track successfully processed pages
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS completed_pages (
        page_number INTEGER PRIMARY KEY,
        completion_date TEXT,
        image_count INTEGER,
        total_size_bytes INTEGER
    )
    """)
    
    # Create author_stats table to track author statistics
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS author_stats (
        author TEXT PRIMARY KEY,
        total_images INTEGER DEFAULT 0,
        last_updated TEXT
    )
    """)
    
    # Add error column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE archive_submissions ADD COLUMN error TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    conn.commit()
    return conn

def get_image_directory(author):
    """Create and return a directory path for saving images."""
    # Create author directory with sanitized name
    author_dir = os.path.join(BASE_DIR, re.sub(r'[<>:"/\\|?*]', '_', author))
    os.makedirs(author_dir, exist_ok=True)
    
    # Count existing files in author directory and subdirectories
    total_files = 0
    for _, _, files in os.walk(author_dir):
        total_files += len(files)
    
    # Create new subdirectory based on total files
    subdir_num = total_files // FILES_PER_DIR
    subdir = os.path.join(author_dir, str(subdir_num))
    os.makedirs(subdir, exist_ok=True)
    
    return subdir

def get_image_links(search_page_url, attempt=1, session=None):
    """Extract image links from a search result page."""
    try:
        logger.info(f"Attempting to fetch URL: {search_page_url} (attempt {attempt})")
        timeout = BASE_TIMEOUT * attempt  # Progressive timeout
        
        # Special handling for 408 errors - always create a fresh session
        if attempt > 1:
            logger.info("Creating fresh session for retry attempt")
            if session:
                try:
                    session.close()  # Close old session if it exists
                except:
                    pass
            # Create completely new session with fresh connection pool
            session = create_session(max_retries=attempt)
            # Configure the adapter to not retry on 408 specifically
            for protocol in ['http://', 'https://']:
                adapter = session.adapters[protocol]
                # Get the current retry configuration
                retries = adapter.max_retries
                # Create a new Retry instance with 408 removed from status_forcelist
                if 408 in retries.status_forcelist:
                    status_forcelist = set(retries.status_forcelist)
                    status_forcelist.remove(408)
                    adapter.max_retries = requests.adapters.Retry(
                        total=retries.total,
                        connect=retries.connect,
                        read=retries.read,
                        redirect=retries.redirect,
                        status=retries.status,
                        other=retries.other,
                        backoff_factor=retries.backoff_factor,
                        status_forcelist=list(status_forcelist),
                        allowed_methods=retries.allowed_methods
                    )
        elif not session:
            session = create_session(max_retries=attempt)
            
        response = session.get(
            search_page_url,
            timeout=timeout,
            allow_redirects=True
        )
        
        # Log response details
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response URL after redirects: {response.url}")
        
        # Special handling for server errors (5xx) and client timeout errors (408, 429)
        if response.status_code in [408, 429, 500, 502, 503, 504, 507, 508, 509]:
            error_msg = f"Error ({response.status_code}) for {search_page_url}"
            logger.error(error_msg)
            
            # Close the response and session for 408 errors
            if response.status_code == 408:
                response.close()
                try:
                    session.close()
                except:
                    pass
                    
                # Determine backoff time based on attempt number
                backoff_time = min(20 * attempt, 300)  # Max 5 minutes
                logger.info(f"Request Timeout (408), backing off for {backoff_time} seconds")
                time.sleep(backoff_time)
                
                # If this is not the final attempt, retry with a fresh session
                if attempt < 3:  # Max 3 attempts
                    logger.info(f"Retrying {search_page_url} with fresh session (attempt {attempt + 1})")
                    return get_image_links(search_page_url, attempt=attempt + 1, session=None)
            elif response.status_code == 429:  # Too Many Requests
                backoff_time = min(60 * attempt, 600)  # Max 10 minutes
                logger.info(f"Rate limited (429), backing off for {backoff_time} seconds")
                time.sleep(backoff_time)
                
                # If this is not the final attempt, retry recursively with increased attempt count
                if attempt < 3:
                    logger.info(f"Retrying {search_page_url} (attempt {attempt + 1})")
                    return get_image_links(search_page_url, attempt=attempt + 1, session=session)
            
            # If all retries failed, raise the exception to be handled by the caller
            raise requests.exceptions.HTTPError(f"Server Error ({response.status_code})")
        
        response.raise_for_status()
        
        # Clear any stored error state on success
        if hasattr(session, '_last_error'):
            delattr(session, '_last_error')
            
        soup = BeautifulSoup(response.text, "html.parser")
        image_data = []
        
        # Find all Tumblr share links which contain the actual image URLs
        tumblr_links = soup.find_all('a', href=lambda x: x and 'tumblr.com/share/photo' in x)
        logger.info(f"Found {len(tumblr_links)} Tumblr share links")
        
        # Extract current page number from URL
        current_page = 0
        page_match = re.search(r'page_offset=(\d+)', search_page_url)
        if page_match:
            current_page = int(page_match.group(1))
        next_page = current_page + 1
        
        for link in tumblr_links:
            href = link.get('href', '')
            # Extract both the source (image URL) and clickthru (photo page URL) parameters
            if 'source=' in href and 'clickthru=' in href:
                try:
                    # Extract image URL
                    source_param = href.split('source=')[1].split('&')[0]
                    image_url = unquote(unquote(source_param))
                    
                    # Extract photo page URL
                    clickthru_param = href.split('clickthru=')[1].split('&')[0]
                    photo_page_url = unquote(unquote(clickthru_param))
                    
                    # Extract caption
                    caption_param = href.split('caption=')[1].split('&')[0] if 'caption=' in href else ''
                    caption = unquote(unquote(caption_param))
                    
                    # Extract page number from photo page URL
                    page_match = re.search(r'page_offset=(\d+)', photo_page_url)
                    page_number = int(page_match.group(1)) if page_match else current_page
                    
                    if image_url.startswith('https://'):
                        image_data.append({
                            'image_url': image_url,
                            'page_url': photo_page_url,
                            'caption': caption,
                            'next_page': page_number == next_page  # True only if this image is from the next sequential page
                        })
                        logger.debug(f"Found image: {image_url} from page {page_number} (next_page={page_number == next_page})")
                except Exception as e:
                    logger.error(f"Failed to parse Tumblr share URL: {e}")
                    continue

        next_page_count = sum(1 for img in image_data if img.get('next_page', False))
        logger.info(f"Found {len(image_data)} images with metadata on page {search_page_url} ({next_page_count} from next page {next_page})")
        return image_data
    except requests.exceptions.HTTPError as e:
        if any(str(code) in str(e) for code in [408, 429, 500, 502, 503, 504, 507, 508, 509]):
            # Re-raise these specific errors to be handled by the caller
            raise
        logger.error(f"HTTP error for {search_page_url}: {e}")
        return []
    except requests.exceptions.Timeout as e:
        # Handle timeout errors with retry logic
        logger.error(f"Timeout error for {search_page_url}: {e}")
        if attempt < 3:  # Max 3 attempts
            backoff_time = 30 * attempt  # Progressive backoff
            logger.info(f"Backing off for {backoff_time} seconds before retrying")
            time.sleep(backoff_time)
            # Create fresh session for timeout retries
            return get_image_links(search_page_url, attempt=attempt + 1, session=None)
        return []
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {search_page_url}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error while processing {search_page_url}: {e}")
        return []
    finally:
        # Always try to close the response if it exists
        if 'response' in locals():
            try:
                response.close()
            except:
                pass

def parse_hungarian_date(date_str):
    """Parse a date string in Hungarian format with optional time."""
    if not date_str:
        return None
        
    # Hungarian month mappings
    hu_months = {
        'jan.': '01', 'febr.': '02', 'márc.': '03', 'ápr.': '04',
        'máj.': '05', 'jún.': '06', 'júl.': '07', 'aug.': '08',
        'szept.': '09', 'okt.': '10', 'nov.': '11', 'dec.': '12'
    }
    
    try:
        # Remove any leading/trailing whitespace and "Készült:" prefix
        date_str = date_str.replace('Készült:', '').strip()
        
        # Split into date and time parts
        parts = date_str.split(' ')
        
        # Handle date part
        if len(parts) >= 3:  # At least year, month, day
            year = parts[0].rstrip('.')
            month = parts[1].lower()
            day = parts[2].rstrip('.')
            
            # Convert month name to number
            month_num = hu_months.get(month)
            if not month_num:
                return None
                
            # Format date part
            formatted_date = f"{year}-{month_num}-{day.zfill(2)}"
            
            # If time is present (format: HH:MM)
            if len(parts) >= 4:
                time_str = parts[3]
                if ':' in time_str:
                    return f"{formatted_date} {time_str}"
            
            return formatted_date
            
    except Exception as e:
        logger.warning(f"Failed to parse Hungarian date '{date_str}': {e}")
    return None

def handle_timeout_error(url, attempt, error_type="request"):
    """Centralized timeout error handling."""
    backoff_time = min(30 * attempt, 300)  # Max 5 minutes
    logger.info(f"{error_type} timeout for {url}, backing off for {backoff_time} seconds (attempt {attempt})")
    time.sleep(backoff_time)
    return attempt + 1

def extract_metadata(photo_page_url, attempt=1, session=None):
    """Extract metadata from a photo page with improved error handling."""
    try:
        # Verify session health only if:
        # 1. We have a session
        # 2. This is a retry attempt (attempt > 1)
        # 3. The previous attempt failed with a connection error
        if session and attempt > 1 and isinstance(getattr(session, '_last_error', None), 
                                                (requests.exceptions.ConnectionError, 
                                                 requests.exceptions.ChunkedEncodingError)):
            # Session had connection issues, create a fresh one
            session = create_session()
        
        timeout = BASE_TIMEOUT * attempt  # Progressive timeout
        if session:
            response = session.get(photo_page_url, timeout=timeout)
        else:
            temp_session = create_session(max_retries=attempt)
            response = temp_session.get(photo_page_url, timeout=timeout)

        response.raise_for_status()
        
        # Clear any stored error state on success
        if hasattr(session, '_last_error'):
            delattr(session, '_last_error')
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extract title from h1 tag
        title = soup.find("h1", class_="image_title")
        if title:
            title = title.text.strip()
        else:
            title_meta = soup.find("meta", property="og:title")
            title = title_meta["content"] if title_meta else "Unknown"

        # Extract description from desc div
        description = None
        desc_div = soup.find("div", class_="desc")
        if desc_div:
            description = desc_div.text.strip()

        # Extract author from user_name class
        author_container = soup.find("h2", class_="user_name")
        author = "Unknown"
        author_url = None
        if author_container:
            author_link = author_container.find("a")
            if author_link:
                author = author_link.text.strip()
                author_url = author_link.get('href')

        # Extract license information with version from cc_container
        license_info = "Unknown"
        cc_container = soup.find("div", class_="cc_container")
        if cc_container:
            license_link = cc_container.find("a")
            if license_link:
                license_href = license_link.get('href', '')
                # Updated regex to properly capture country code from the URL path
                version_match = re.search(r'/licenses/([^/]+)/(\d+\.\d+)(?:/([a-zA-Z]{2}))?/?$', license_href)
                if version_match:
                    license_type, version, country = version_match.groups()
                    # Format as "CC TYPE VERSION COUNTRY" (e.g. "CC BY 2.5 HU")
                    license_info = f"CC {license_type.upper()} {version}"
                    if country:
                        license_info += f" {country.upper()}"

        # Extract collections and albums
        collections = []
        albums = []
        
        # Extract collections ("Gyűjteményekben")
        collections_section = soup.find("section", class_="compilations")
        if collections_section:
            collections_container = collections_section.find("ul", class_="collections_container")
            if collections_container:
                for li in collections_container.find_all("li", class_=lambda x: x and "collection_" in x):
                    collection_id = None
                    for class_name in li.get('class', []):
                        if class_name.startswith('collection_'):
                            collection_id = class_name.split('_')[1]
                            break
                    
                    collection_link = li.find("a")
                    if collection_link and collection_id:
                        # Try different ways to get the title
                        collection_title = None
                        # First try the album_title span
                        title_span = collection_link.find("span", class_="album_title")
                        if title_span:
                            collection_title = title_span.get_text(strip=True)
                        # If that fails, try getting the link text directly
                        if not collection_title:
                            collection_title = collection_link.get_text(strip=True)
                        
                        # If still no title, try looking for the text directly in the li element
                        if not collection_title:
                            collection_title = li.get_text(strip=True)
                        
                        if collection_title:
                            collections.append({
                                'id': collection_id,
                                'title': collection_title,
                                'url': collection_link.get('href'),
                                'is_public': 'public' in li.get('class', [])
                            })

        # Extract albums ("Albumokban")
        albums_section = soup.find("section", class_="collections")
        if albums_section and "Albumokban" in albums_section.get_text():
            albums_container = albums_section.find("ul", class_="collections_container")
            if albums_container:
                for li in albums_container.find_all("li", class_=lambda x: x and "collection_" in x):
                    album_id = None
                    for class_name in li.get('class', []):
                        if class_name.startswith('collection_'):
                            album_id = class_name.split('_')[1]
                            break
                    
                    album_link = li.find("a")
                    if album_link and album_id:
                        # Try different ways to get the title
                        album_title = None
                        # First try the album_title span
                        title_span = album_link.find("span", class_="album_title")
                        if title_span:
                            album_title = title_span.get_text(strip=True)
                        # If that fails, try getting the link text directly
                        if not album_title:
                            album_title = album_link.get_text(strip=True)
                        
                        # If still no title, try looking for the text directly in the li element
                        if not album_title:
                            album_title = li.get_text(strip=True)
                        
                        if album_title:
                            albums.append({
                                'id': album_id,
                                'title': album_title,
                                'url': album_link.get('href'),
                                'is_public': 'public' in li.get('class', [])
                            })

        # Extract tags - try multiple possible locations
        tags = []
        
        # First try to find the tags box
        tags_box = soup.find("div", class_="tags box")
        if tags_box:
            # Look for box_data div inside the tags box
            tags_container = tags_box.find("div", class_="box_data")
            if tags_container:
                for tag_li in tags_container.find_all("li", class_="tag"):
                    tag_link = tag_li.find("a", class_="global-tag")
                    if tag_link:
                        tag_name = tag_link.get_text(strip=True)
                        # Extract count from title attribute (e.g., "Az összes kuba címkéjű kép (2714 db)")
                        count_match = re.search(r'\((\d+)\s*db\)', tag_link.get('title', ''))
                        count = int(count_match.group(1)) if count_match else 0
                        tags.append({
                            'name': tag_name,
                            'count': count
                        })
        
        # If no tags found, try looking for tags in other locations
        if not tags:
            # Try finding all global-tag links anywhere in the page
            tag_links = soup.find_all("a", class_="global-tag")
            for tag_link in tag_links:
                tag_name = tag_link.get_text(strip=True)
                # Extract count from title attribute
                count_match = re.search(r'\((\d+)\s*db\)', tag_link.get('title', ''))
                count = int(count_match.group(1)) if count_match else 0
                tags.append({
                    'name': tag_name,
                    'count': count
                })
        
        # Remove duplicates while preserving order
        seen = set()
        tags = [tag for tag in tags if not (tag['name'] in seen or seen.add(tag['name']))]

        # Extract EXIF data from the table
        exif_data = {}
        exif_table = soup.find('table')
        if exif_table:
            for row in exif_table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) == 2:
                    key = cols[0].text.strip().rstrip(':')
                    value = cols[1].text.strip()
                    exif_data[key] = value

        # Extract camera make and model from the manufacturer link
        camera_make = None
        camera_model = None
        if 'Gyártó' in exif_data:
            manufacturer_link = soup.find('a', href=lambda x: x and '/fenykepezogep/' in x)
            if manufacturer_link:
                href = manufacturer_link.get('href', '')
                full_text = manufacturer_link.get_text(strip=True)
                # Extract make and model from href (e.g., /fenykepezogep/apple/iphone_se_(2nd_generation))
                parts = href.split('/')
                if len(parts) >= 4:  # We expect at least 4 parts: ['', 'fenykepezogep', 'make', 'model']
                    # Find the position in the full text where the model part starts
                    # by looking for the first character after the make name
                    make_part = parts[2]
                    # Find the position in the full text where the make part ends
                    # This will be the first space after the make name
                    make_end_pos = len(make_part)
                    # Split the full text at this position
                    camera_make = full_text[:make_end_pos].strip()
                    camera_model = full_text[make_end_pos:].strip()
            else:
                # Fallback to using the raw text if no link found
                camera_make = exif_data['Gyártó']

        # Extract taken date from EXIF
        taken_date = None
        if 'Készült' in exif_data:
            taken_date = parse_hungarian_date(exif_data['Készült'])

        # Extract upload date from image_data div
        upload_date = None
        upload_date_elem = soup.find('li', class_='upload_date')
        if upload_date_elem:
            date_value = upload_date_elem.find('span', class_='value')
            if date_value:
                upload_date = parse_hungarian_date(date_value.text.strip())

        # Find highest resolution image URL
        high_res_url = None
        # First look for direct image URLs
        img_patterns = ['_xxl.jpg', '_xl.jpg', '_l.jpg', '_m.jpg']
        
        # Try to find the image URL from meta tags first
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            base_url = og_image["content"]
            # Use our optimized function to get the highest resolution version
            high_res_url = get_high_res_url(base_url, session=session)
        
        # Fallback to looking for direct links if meta tag approach failed
        if not high_res_url:
            for pattern in img_patterns:
                img_link = soup.find('a', href=lambda x: x and pattern in x)
                if img_link:
                    high_res_url = img_link['href']
                    break

        metadata = {
            'title': title,
            'description': description,
            'author': author,
            'author_url': author_url,
            'license': license_info,
            'collections': collections,
            'albums': albums,
            'camera_make': camera_make,
            'camera_model': camera_model,
            'focal_length': exif_data.get('Fókusztáv'),
            'aperture': exif_data.get('Rekesz'),
            'shutter_speed': exif_data.get('Zársebesség'),
            'taken_date': taken_date,
            'upload_date': upload_date,
            'page_url': photo_page_url,
            'tags': tags
        }

        return metadata
    except (requests.exceptions.ConnectionError, 
            requests.exceptions.ChunkedEncodingError) as e:
        # Store the error type on the session object
        if session:
            session._last_error = e
            
        # Network connectivity issues - could be local network or system sleep
        if attempt < 3:
            backoff_time = min(60 * attempt, 300)  # Longer backoff for connection issues
            logger.warning(f"Connection error for {photo_page_url}, backing off for {backoff_time}s: {e}")
            time.sleep(backoff_time)
            # Create fresh session on connection errors
            return extract_metadata(photo_page_url, attempt=attempt + 1, session=create_session())
        logger.error(f"Max connection retries reached for {photo_page_url}")
        return None

    except requests.exceptions.Timeout as e:
        # Request timeout - could be server or network latency
        if attempt < 3:
            backoff_time = min(30 * attempt, 180)
            logger.warning(f"Timeout for {photo_page_url}, backing off for {backoff_time}s")
            time.sleep(backoff_time)
            return extract_metadata(photo_page_url, attempt=attempt + 1, session=session)
        logger.error(f"Max timeout retries reached for {photo_page_url}")
        return None

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        
        # Handle 408 errors with restart
        if status_code == 408:
            logger.warning(f"Request timeout (408) for {photo_page_url}, triggering error-based restart")
            restart_script(error_restart=True)
            return None
            
        # Temporary server errors - retry with backoff
        if status_code in [500, 502, 503, 504] and attempt < 3:
            backoff_time = min(2.408 * attempt, 240)
            logger.warning(f"Server error {status_code} for {photo_page_url}, backing off for {backoff_time}s")
            time.sleep(backoff_time)
            return extract_metadata(photo_page_url, attempt=attempt + 1, session=session)
            
        # Permanent errors - don't retry
        elif status_code in [400, 401, 403, 404]:
            logger.error(f"Permanent error {status_code} for {photo_page_url}")
            return None
            
        # Other errors - log but don't retry
        logger.error(f"HTTP error {status_code} for {photo_page_url}")
        return None

    except requests.exceptions.RequestException as e:
        # Other request errors (including MustRedialError)
        # Let the built-in retry mechanism handle these
        logger.error(f"Request error for {photo_page_url}: {e}")
        return None

    except Exception as e:
        # Unexpected errors (like parsing errors) - don't retry
        logger.error(f"Unexpected error for {photo_page_url}: {e}")
        return None

def calculate_file_hash(filepath):
    """Calculate SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Failed to calculate hash for {filepath}: {e}")
        return None

counter = 0

def download_image(image_url, author, session=None):
    """Download an image and save locally. Returns (filename, hash) tuple or (None, None) if failed."""
    try:
        # Get the directory for this author
        save_dir = get_image_directory(author)
        
        # Extract the image ID and resolution from the URL
        url_parts = urlparse(image_url)
        image_id = re.search(r'/(\d+)_[a-f0-9]+', url_parts.path)
        if image_id:
            base_name = f"image_{image_id.group(1)}"
        else:
            # Fallback to using the last part of the path without the resolution suffix
            base_name = re.sub(r'_[a-z]+\.jpg$', '', os.path.basename(url_parts.path))
        
        # Add timestamp to ensure uniqueness
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")  # Added microseconds for better uniqueness
        filename = os.path.join(save_dir, f"{base_name}_{timestamp}.jpg")
        
        # Create a lock file to prevent race conditions
        lock_filename = filename + '.lock'
        
        try:
            # Create the lock file
            with open(lock_filename, 'w') as lock_file:
                try:
                    # Get an exclusive lock using portalocker
                    portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
                except portalocker.LockException:
                    # Another thread is downloading this file
                    logger.warning(f"Another thread is downloading {filename}, skipping")
                    return None, None
                
                # Use the provided session if available, otherwise make direct request
                if session:
                    response = session.get(image_url, timeout=60, stream=True)
                else:
                    # Create a new session with HTTP/2 support
                    temp_session = create_session()
                    response = temp_session.get(image_url, timeout=60, stream=True)
                
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                if total_size == 0:
                    raise ValueError("Server reported content length of 0")
                
                # Use BytesIO as an in-memory buffer
                buffer = BytesIO()
                sha256_hash = hashlib.sha256()
                downloaded_size = 0
                
                # Generate a random number between 5 and 17
                global counter
                random_number = counter % 8
                counter += 1
                # Generate a subsequent green coluor in HEX using the counter, e.g. 00FF00, 00FE00, 00FD00, etc.
                green_hex = f"#00{hex(255 - counter % 256)[2:].zfill(2)}00"
                with tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"Downloading {os.path.basename(filename)}",
                    colour=green_hex,
                    position=random_number
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=BLOCK_SIZE):
                        if not chunk:  # Filter out keep-alive chunks
                            continue
                        buffer.write(chunk)
                        sha256_hash.update(chunk)
                        downloaded_size += len(chunk)
                        pbar.update(len(chunk))
                
                # Verify the download
                if downloaded_size != total_size:
                    logger.error(f"Download size mismatch for {image_url}. Expected {total_size}, got {downloaded_size}")
                    return None, None
                
                # Write the verified data directly to final location
                with open(filename, 'wb') as f:
                    f.write(buffer.getvalue())
                
                # Return the filename and hash
                return filename, sha256_hash.hexdigest()
                
        finally:
            # Clean up the lock file
            try:
                os.remove(lock_filename)
            except OSError:
                pass
            
    except FileExistsError:
        # Another thread is already downloading this file
        logger.warning(f"Another thread is downloading {filename}, skipping")
        return None, None
    except Exception as e:
        logger.error(f"Failed to download {image_url}: {e}")
        return None, None


def extract_image_id(url):
    """Extract the unique image ID from an Indafoto URL."""
    # The URL format is like: .../68973_42b3ac220f8b136347cfc067e6cf2b05/27113791_7f3d073a...
    # We want the second ID (27113791)
    try:
        # First try to find the image ID from the URL path
        match = re.search(r'/(\d+)_[a-f0-9]+/(\d+)_[a-f0-9]+', url)
        if match:
            return match.group(2)  # Return the second ID
        
        # Fallback: try to find any numeric ID in the URL
        match = re.search(r'image/(\d+)-[a-f0-9]+', url)
        if match:
            return match.group(1)
            
        # If no ID found, use a hash of the URL
        return hashlib.md5(url.encode()).hexdigest()[:12]
    except Exception as e:
        logger.error(f"Failed to extract image ID from URL {url}: {e}")
        return hashlib.md5(url.encode()).hexdigest()[:12]

def retry_failed_pages(conn, cursor):
    """Retry downloading pages that previously failed."""
    cursor.execute("""
        SELECT page_number, url, attempts 
        FROM failed_pages 
        WHERE status = 'pending' 
        AND attempts < 3
        ORDER BY page_number
    """)
    failed_pages = cursor.fetchall()
    
    if not failed_pages:
        logger.info("No failed pages to retry")
        return
    
    logger.info(f"Retrying {len(failed_pages)} failed pages...")
    for page_number, url, attempts in failed_pages:
        try:
            logger.info(f"Retrying page {page_number} (attempt {attempts + 1})")
            # Pass the attempt number to get_image_links for progressive timeout
            image_data_list = get_image_links(url, attempt=attempts + 1)
            
            if process_image_list(image_data_list, conn, cursor, attempt=attempts + 1):
                # Update status to success if all images were processed
                cursor.execute("""
                    UPDATE failed_pages 
                    SET status = 'success', 
                        attempts = attempts + 1,
                        last_attempt = ?
                    WHERE page_number = ?
                """, (datetime.now().isoformat(), page_number))
            else:
                # Update attempt count if some images failed
                cursor.execute("""
                    UPDATE failed_pages 
                    SET attempts = attempts + 1,
                        last_attempt = ?,
                        status = CASE 
                            WHEN attempts + 1 >= 3 THEN 'failed'
                            ELSE 'pending'
                        END
                    WHERE page_number = ?
                """, (datetime.now().isoformat(), page_number))
            
            conn.commit()
            # Progressive rate limiting
            time.sleep(BASE_RATE_LIMIT * (attempts + 1))
            
        except Exception as e:
            logger.error(f"Error retrying page {page_number}: {e}")
            cursor.execute("""
                UPDATE failed_pages 
                SET attempts = attempts + 1,
                    last_attempt = ?,
                    error = ?,
                    status = CASE 
                        WHEN attempts + 1 >= 3 THEN 'failed'
                        ELSE 'pending'
                    END
                WHERE page_number = ?
            """, (datetime.now().isoformat(), str(e), page_number))
            conn.commit()

def is_author_banned(author):
    """Check if an author is banned using the in-memory set."""
    return author in banned_authors_set

def ban_author(conn, author, reason, banned_by="system"):
    """Add an author to the banned list."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO banned_authors (author, reason, banned_date, banned_by)
            VALUES (?, ?, ?, ?)
        """, (author, reason, datetime.now().isoformat(), banned_by))
        conn.commit()
        global banned_authors_set
        banned_authors_set.add(author)
        return True
    except sqlite3.IntegrityError:
        return False  # Author is already banned

def unban_author(conn, author):
    """Remove an author from the banned list."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM banned_authors WHERE author = ?", (author,))
    conn.commit()
    if cursor.rowcount > 0:
        global banned_authors_set
        banned_authors_set.discard(author)
        return True
    return False

def get_banned_authors(conn):
    """Get list of banned authors."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM banned_authors ORDER BY banned_date DESC")
    return cursor.fetchall()

def cleanup_banned_author_content(conn, author):
    """Delete all content associated with a banned author."""
    cursor = conn.cursor()
    
    # Get all image IDs for this author
    cursor.execute("SELECT id FROM images WHERE author = ?", (author,))
    image_ids = [row[0] for row in cursor.fetchall()]
    
    if not image_ids:
        return True
    
    # Delete from all related tables
    for image_id in image_ids:
        cursor.execute("DELETE FROM image_tags WHERE image_id = ?", (image_id,))
        cursor.execute("DELETE FROM image_albums WHERE image_id = ?", (image_id,))
        cursor.execute("DELETE FROM image_collections WHERE image_id = ?", (image_id,))
        cursor.execute("DELETE FROM marked_images WHERE image_id = ?", (image_id,))
        cursor.execute("DELETE FROM image_notes WHERE image_id = ?", (image_id,))
    
    # Delete the images
    cursor.execute("DELETE FROM images WHERE author = ?", (author,))
    
    conn.commit()
    return True

class ThreadPool:
    """A simple thread pool that reuses threads instead of creating new ones each time."""
    def __init__(self, num_threads, name):
        self.tasks = queue.Queue()
        self.results = queue.Queue()
        self.errors = queue.Queue()
        self.threads = []
        self.running = True
        self.name = name
        self.shutdown_event = threading.Event()
        
        # Create worker threads
        for i in range(num_threads):
            thread = threading.Thread(target=self._worker, name=f"{name}-{i}")
            thread.daemon = True
            thread.start()
            self.threads.append(thread)
    
    def _worker(self):
        """Worker thread that processes tasks from the queue."""
        while self.running and not self.shutdown_event.is_set():
            try:
                task = self.tasks.get(timeout=1)  # 1 second timeout to check running flag
                if task is None:  # Poison pill
                    self.tasks.task_done()
                    break
                
                func, args = task
                try:
                    result = func(*args)
                    if result is not None:
                        self.results.put(result)
                except Exception as e:
                    self.errors.put((args, str(e)))
                finally:
                    self.tasks.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in {self.name} worker: {e}")
                continue
    
    def add_task(self, func, *args):
        """Add a task to the pool."""
        if not self.shutdown_event.is_set():
            self.tasks.put((func, args))
    
    def wait_completion(self):
        """Wait for all tasks to complete."""
        self.tasks.join()
    
    def shutdown(self):
        """Shutdown the thread pool."""
        self.running = False
        self.shutdown_event.set()
        
        # Clear the task queue
        try:
            while True:
                self.tasks.get_nowait()
                self.tasks.task_done()
        except queue.Empty:
            pass
        
        # Add poison pills for each thread
        for _ in self.threads:
            self.tasks.put(None)
            
        # Wait for all threads to complete with timeout
        for thread in self.threads:
            thread.join(timeout=2)
            
        # Force clear the results and errors queues
        try:
            while True:
                self.results.get_nowait()
                self.results.task_done()
        except queue.Empty:
            pass
            
        try:
            while True:
                self.errors.get_nowait()
                self.errors.task_done()
        except queue.Empty:
            pass

def process_image_list(image_data_list, conn, cursor, sample_rate=1.0, progress_callback=None):
    """Process a list of images with improved connection handling."""
    global consecutive_failures, consecutive_successes, current_wait_time, current_workers
    
    author_image_counts = {}
    processed_count = 0
    failed_count = 0
    skipped_count = 0
    banned_count = 0
    
    # Create a session pool for better connection reuse
    session_pool = queue.Queue(maxsize=current_workers * 2)
    for _ in range(current_workers * 2):
        session = create_session(max_retries=3, pool_connections=current_workers)
        session_pool.put(session)
    
    # Queue for next page's metadata with larger buffer
    next_batch_queue = queue.Queue(maxsize=current_workers * 4)  # Increased buffer size
    db_check_queue = queue.Queue(maxsize=current_workers * 4)    # Added size limit
    
    # Track active tasks and completion status
    active_tasks = {
        'metadata': 0,
        'download': 0,
        'validation': 0
    }
    completion_status = {
        'producer_done': False,
        'metadata_done': False,
        'all_tasks_done': False
    }
    
    # Track metadata extraction progress
    metadata_progress = {
        'current_page': 0,
        'next_page': 0,
        'total_current': len(image_data_list),
        'total_next': 0  # Will be updated when we know the next page size
    }
    
    def get_session():
        """Get a session from the pool with timeout."""
        try:
            return session_pool.get(timeout=5)
        except queue.Empty:
            return create_session(max_retries=3, pool_connections=current_workers)
    
    def return_session(session):
        """Return a session to the pool if it's healthy."""
        try:
            if session and not session_pool.full():
                session_pool.put(session, timeout=1)
        except queue.Full:
            session.close()
    
    def process_metadata(image_data):
        """Process metadata with connection pooling."""
        session = get_session()
        try:
            metadata = extract_metadata(image_data['page_url'], session=session)
            if metadata:
                if is_author_banned(metadata['author']):
                    return ('banned', None)
                
                url = image_data['image_url']
                high_res_url = get_high_res_url(url, session=session)
                download_url = high_res_url if high_res_url else url
                return ('success', (download_url, metadata))
            return ('error', ('metadata', image_data['page_url'], "Failed to extract metadata"))
        except Exception as e:
            return ('error', ('metadata', image_data['page_url'], str(e)))
        finally:
            return_session(session)
    
    def process_download(download_url, metadata):
        """Process download with connection pooling."""
        session = get_session()
        try:
            result = download_image(download_url, metadata['author'], session=session)
            if result:
                filename, _ = result
                return ('success', (filename, download_url, metadata))
            return ('error', ('download', download_url, "Failed to download image"))
        except Exception as e:
            return ('error', ('download', download_url, str(e)))
        finally:
            return_session(session)
    
    def process_validation(filename, url, metadata):
        """Process validation with connection pooling."""
        try:
            file_hash = calculate_file_hash(filename)
            return ('success', (filename, file_hash, url, metadata))
        except Exception as e:
            try:
                if os.path.exists(filename):
                    os.remove(filename)
            except:
                pass
            return ('error', ('validation', url, str(e)))
    
    # Create thread pools for parallel processing
    metadata_pool = ThreadPool(current_workers, "metadata")
    download_pool = ThreadPool(current_workers, "download")
    validation_pool = ThreadPool(current_workers, "validation")
    
    def metadata_producer():
        """Producer function that feeds the metadata pool with new images."""
        try:
            next_page_started = False
            for image_data in image_data_list:
                try:
                    url = image_data.get('image_url', '')
                    page_url = image_data.get('page_url', '')
                    
                    if not url or not page_url:
                        try:
                            db_check_queue.put(('error', image_data, "Skipping image with no URL or page URL"), timeout=30)
                        except queue.Full:
                            logger.warning("DB check queue full, waiting for space...")
                            db_check_queue.put(('error', image_data, "Skipping image with no URL or page URL"), block=True)
                        continue
                    
                    # Extract image ID from URL
                    image_id = extract_image_id(url)
                    if not image_id:
                        try:
                            db_check_queue.put(('error', image_data, f"Could not extract image ID from URL: {url}"), timeout=30)
                        except queue.Full:
                            logger.warning("DB check queue full, waiting for space...")
                            db_check_queue.put(('error', image_data, f"Could not extract image ID from URL: {url}"), block=True)
                        continue
                    
                    # Check if this is from the next page
                    is_next_page = 'next_page' in page_url
                    if is_next_page and not next_page_started:
                        next_page_started = True
                        logger.debug("Starting to process next page metadata")
                    
                    # Queue the image for database check in main thread with backpressure
                    try:
                        db_check_queue.put(('check', image_data, image_id), timeout=30)
                    except queue.Full:
                        logger.warning("DB check queue full, implementing backpressure...")
                        # Wait for queue to have space
                        db_check_queue.put(('check', image_data, image_id), block=True)
                        
                except Exception as e:
                    try:
                        db_check_queue.put(('error', image_data, str(e)), timeout=30)
                    except queue.Full:
                        logger.warning("DB check queue full on error, waiting...")
                        db_check_queue.put(('error', image_data, str(e)), block=True)
        except Exception as e:
            logger.error(f"Error in metadata producer: {e}")
        finally:
            # Signal end of input - keep trying until it succeeds
            while True:
                try:
                    db_check_queue.put(('done', None, None), timeout=30)
                    break
                except queue.Full:
                    logger.warning("DB check queue full when trying to signal completion, retrying...")
                    time.sleep(1)
            completion_status['producer_done'] = True
    
    try:
        # Start the metadata producer in a separate thread
        producer_thread = threading.Thread(target=metadata_producer)
        producer_thread.daemon = True
        producer_thread.start()
        
        logger.info("Starting image processing pipeline")
        
        # Create progress bars
        with tqdm(total=len(image_data_list), desc="Processing images", colour='yellow', position=9) as pbar, \
             tqdm(total=36, desc="Metadata extraction", colour='#800080', position=10) as metadata_pbar, \
             tqdm(total=36, desc="Next page metadata", colour='#FFA500', position=11) as next_page_pbar:
            
            # Process results and chain tasks
            while not completion_status['all_tasks_done']:
                # Check if all tasks are complete with more granular conditions
                if completion_status['producer_done']:
                    # If producer is done and no active tasks, we can proceed
                    if (active_tasks['metadata'] == 0 and 
                        active_tasks['download'] == 0 and 
                        active_tasks['validation'] == 0):
                        # Only wait for db_check_queue if we have no active tasks
                        if db_check_queue.empty():
                            completion_status['all_tasks_done'] = True
                            break
                
                # Check database for next image with timeout
                try:
                    action, image_data, image_id = db_check_queue.get(timeout=1)
                    if action == 'done':
                        continue
                    elif action == 'error':
                        failed_count += 1
                        pbar.update(1)
                        continue
                    
                    # Check if we already have this image
                    cursor.execute("""
                        SELECT i.id, i.local_path, i.author 
                        FROM images i 
                        WHERE i.url LIKE ?
                    """, (f"%{image_id}%",))
                    existing = cursor.fetchone()
                    
                    if existing:
                        logger.debug(f"Image ID {image_id} already exists, skipping...")
                        skipped_count += 1
                        processed_count += 1
                        
                        # Update author counts
                        author = existing[2]
                        author_image_counts[author] = author_image_counts.get(author, 0) + 1
                        pbar.update(1)
                        continue
                    
                    # Add to next batch queue with backpressure
                    try:
                        next_batch_queue.put(image_data, timeout=30)
                    except queue.Full:
                        logger.warning("Next batch queue full, implementing backpressure...")
                        # If queue is full, wait for space
                        next_batch_queue.put(image_data, block=True)
                except queue.Empty:
                    pass
                
                # Process next batch with controlled rate and better progress tracking
                try:
                    while not next_batch_queue.empty():
                        # Check if metadata pool is getting too full
                        if active_tasks['metadata'] >= current_workers * 2:
                            logger.debug("Metadata pool busy, pausing batch processing...")
                            break
                            
                        image_data = next_batch_queue.get_nowait()
                        if random.random() <= sample_rate:
                            active_tasks['metadata'] += 1
                            metadata_pool.add_task(process_metadata, image_data)
                            # Update metadata progress bar based on URL
                            if 'next_page' in image_data.get('page_url', ''):
                                metadata_progress['next_page'] += 1
                                next_page_pbar.update(1)
                                # Log progress for next page metadata
                                if metadata_progress['next_page'] % 10 == 0:
                                    logger.info(f"Next page metadata progress: {metadata_progress['next_page']}/36")
                            else:
                                metadata_progress['current_page'] += 1
                                metadata_pbar.update(1)
                                
                                # Call progress callback if provided
                                if progress_callback:
                                    total = metadata_progress['total_current']
                                    current = metadata_progress['current_page']
                                    progress_callback(current, total)
                except queue.Empty:
                    pass
                
                # Process metadata results
                try:
                    while True:
                        result = metadata_pool.results.get_nowait()
                        active_tasks['metadata'] -= 1
                        status, data = result
                        
                        if status == 'success':
                            download_url, metadata = data
                            active_tasks['download'] += 1
                            download_pool.add_task(process_download, download_url, metadata)
                        elif status == 'banned':
                            banned_count += 1
                            pbar.update(1)
                        elif status == 'error':
                            failed_count += 1
                            pbar.update(1)
                        
                        metadata_pool.results.task_done()
                except queue.Empty:
                    pass
                
                # Process download results
                try:
                    while True:
                        result = download_pool.results.get_nowait()
                        active_tasks['download'] -= 1
                        status, data = result
                        
                        if status == 'success':
                            filename, url, metadata = data
                            active_tasks['validation'] += 1
                            validation_pool.add_task(process_validation, filename, url, metadata)
                        elif status == 'error':
                            failed_count += 1
                            pbar.update(1)
                        
                        download_pool.results.task_done()
                except queue.Empty:
                    pass
                
                # Process validation results
                try:
                    while True:
                        result = validation_pool.results.get_nowait()
                        active_tasks['validation'] -= 1
                        status, data = result
                        
                        if status == 'success':
                            filename, file_hash, url, metadata = data
                            # Save to database
                            try:
                                # Insert image data
                                cursor.execute("""
                                    INSERT INTO images (url, local_path, sha256_hash, title, description, 
                                                     author, author_url, license, camera_make, camera_model, 
                                                     focal_length, aperture, shutter_speed, taken_date, 
                                                     upload_date, page_url)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    url, filename, file_hash, metadata.get('title'),
                                    metadata.get('description'), metadata.get('author'),
                                    metadata.get('author_url'), metadata.get('license'),
                                    metadata.get('camera_make'), metadata.get('camera_model'),
                                    metadata.get('focal_length'), metadata.get('aperture'),
                                    metadata.get('shutter_speed'), metadata.get('taken_date'),
                                    metadata.get('upload_date'), metadata.get('page_url')
                                ))
                                
                                image_id = cursor.lastrowid
                                
                                # Batch process collections
                                if metadata.get('collections'):
                                    collection_values = [(g['id'], g['title'], g['url'], g['is_public']) 
                                                      for g in metadata['collections']]
                                    cursor.executemany("""
                                        INSERT OR IGNORE INTO collections (collection_id, title, url, is_public)
                                        VALUES (?, ?, ?, ?)
                                    """, collection_values)
                                    
                                    # Get collection IDs in a single query
                                    cursor.execute("""
                                        SELECT collection_id, id FROM collections 
                                        WHERE collection_id IN ({})
                                    """.format(','.join('?' * len(collection_values))), 
                                    [v[0] for v in collection_values])
                                    collection_ids = {row[0]: row[1] for row in cursor.fetchall()}
                                    
                                    # Batch insert image_collections
                                    image_collection_values = [(image_id, collection_ids[g['id']]) 
                                                            for g in metadata['collections']]
                                    cursor.executemany("""
                                        INSERT INTO image_collections (image_id, collection_id)
                                        VALUES (?, ?)
                                    """, image_collection_values)
                                
                                # Batch process albums
                                if metadata.get('albums'):
                                    album_values = [(g['id'], g['title'], g['url'], g['is_public']) 
                                                  for g in metadata['albums']]
                                    cursor.executemany("""
                                        INSERT OR IGNORE INTO albums (album_id, title, url, is_public)
                                        VALUES (?, ?, ?, ?)
                                    """, album_values)
                                    
                                    # Get album IDs in a single query
                                    cursor.execute("""
                                        SELECT album_id, id FROM albums 
                                        WHERE album_id IN ({})
                                    """.format(','.join('?' * len(album_values))), 
                                    [v[0] for v in album_values])
                                    album_ids = {row[0]: row[1] for row in cursor.fetchall()}
                                    
                                    # Batch insert image_albums
                                    image_album_values = [(image_id, album_ids[g['id']]) 
                                                        for g in metadata['albums']]
                                    cursor.executemany("""
                                        INSERT INTO image_albums (image_id, album_id)
                                        VALUES (?, ?)
                                    """, image_album_values)
                                
                                # Batch process tags
                                if metadata.get('tags'):
                                    tag_values = [(tag['name'], tag['count']) for tag in metadata['tags']]
                                    cursor.executemany("""
                                        INSERT OR IGNORE INTO tags (name, count)
                                        VALUES (?, ?)
                                    """, tag_values)
                                    
                                    # Get tag IDs in a single query
                                    cursor.execute("""
                                        SELECT name, id FROM tags 
                                        WHERE name IN ({})
                                    """.format(','.join('?' * len(tag_values))), 
                                    [v[0] for v in tag_values])
                                    tag_ids = {row[0]: row[1] for row in cursor.fetchall()}
                                    
                                    # Batch insert image_tags
                                    image_tag_values = [(image_id, tag_ids[tag['name']]) 
                                                      for tag in metadata['tags']]
                                    cursor.executemany("""
                                        INSERT INTO image_tags (image_id, tag_id)
                                        VALUES (?, ?)
                                    """, image_tag_values)
                                
                                processed_count += 1
                                author = metadata.get('author', 'unknown')
                                author_image_counts[author] = author_image_counts.get(author, 0) + 1
                                
                                conn.commit()
                            except Exception as e:
                                logger.error(f"Error saving to database: {e}")
                                failed_count += 1
                                try:
                                    if os.path.exists(filename):
                                        os.remove(filename)
                                except:
                                    pass
                        elif status == 'error':
                            failed_count += 1
                        
                        pbar.update(1)
                        validation_pool.results.task_done()
                except queue.Empty:
                    pass
                
                # Process errors from all pools
                for pool in [metadata_pool, download_pool, validation_pool]:
                    try:
                        while True:
                            error_args, error_msg = pool.errors.get_nowait()
                            logger.error(f"Error in {pool.name}: {error_msg}")
                            pool.errors.task_done()
                    except queue.Empty:
                        pass
                
                time.sleep(0.1)  # Small sleep to prevent busy waiting
        
        # Wait for producer thread to finish
        producer_thread.join(timeout=5)
        
        # Shutdown thread pools
        metadata_pool.shutdown()
        download_pool.shutdown()
        validation_pool.shutdown()
        
        # Return success status and stats
        success = (processed_count + failed_count) == len(image_data_list) and failed_count == 0
        stats = {
            'processed_count': processed_count,
            'failed_count': failed_count,
            'skipped_count': skipped_count,
            'banned_count': banned_count,
            'total_images': len(image_data_list),
            'author_counts': author_image_counts,
            'current_workers': current_workers,
            'consecutive_successes': consecutive_successes,
            'consecutive_failures': consecutive_failures,
            'current_wait_time': current_wait_time
        }
        
        return success, stats
        
    except Exception as e:
        logger.error(f"Error in process_image_list: {str(e)}")
        return False, {
            'processed_count': processed_count,
            'failed_count': failed_count + len(image_data_list) - processed_count,
            'skipped_count': skipped_count,
            'total_images': len(image_data_list),
            'author_counts': author_image_counts,
            'error': str(e)
        }
    finally:
        # Ensure thread pools are shut down
        try:
            metadata_pool.shutdown()
            download_pool.shutdown()
            validation_pool.shutdown()
        except:
            pass
        
        # Clean up session pool
        while not session_pool.empty():
            try:
                session = session_pool.get_nowait()
                session.close()
            except queue.Empty:
                break

def get_free_space_gb(path):
    """Return free space in GB for the given path."""
    stats = shutil.disk_usage(path)
    return stats.free / (2**30)  # Convert bytes to GB

def check_disk_space(path, min_space_gb=2):
    """Check if there's enough disk space available."""
    free_space_gb = get_free_space_gb(path)
    if free_space_gb < min_space_gb:
        raise RuntimeError(f"Not enough disk space. Only {free_space_gb:.2f}GB available, minimum required is {min_space_gb}GB")
    return free_space_gb

def estimate_space_requirements(downloaded_bytes, pages_processed, total_pages):
    """Estimate total space requirements based on current usage."""
    if pages_processed == 0:
        return 0
    
    avg_bytes_per_page = downloaded_bytes / pages_processed
    remaining_pages = total_pages - pages_processed
    estimated_remaining_bytes = avg_bytes_per_page * remaining_pages
    
    return estimated_remaining_bytes / (1024 * 1024 * 1024)  # Return in GB

def get_search_url(page_number):
    """Return the search URL for a given page number."""
    return SEARCH_URL_TEMPLATE.format(page_number)

def crawl_images(start_offset=0, retry_mode=False):
    """Crawl and download images with parallel page processing."""
    global should_restart, current_page, current_wait_time, consecutive_failures, consecutive_successes
    
    try:
        # Start the restart timer
        global restart_timer
        restart_timer = threading.Timer(60, check_restart_timer)
        restart_timer.start()
        
        conn = init_db()
        cursor = conn.cursor()
        
        # Get banned authors
        banned_authors = get_banned_authors(conn)
        for author in banned_authors:
            banned_authors_set.add(author)
        
        # Check initial disk space
        free_space_gb = check_disk_space(BASE_DIR)
        logger.info(f"Initial free space: {free_space_gb:.2f}GB")
        
        # Initialize space tracking
        total_downloaded_bytes = 0
        pages_processed = 0
        
        # Process pages with retry mode
        if retry_mode:
            logger.info("Running in retry mode. Processing failed pages first...")
            retry_count = retry_failed_pages(conn, cursor)
            logger.info(f"Retry mode completed. Processed {retry_count} failed pages.")
        
        # Track running status
        logger.info(f"Starting crawl from page {start_offset}")
        
        # Setup for parallel page processing
        page_queue = queue.Queue(maxsize=5)        # Queue for pages to process
        db_command_queue = queue.Queue(maxsize=50) # Queue for database operations
        active_page_threads = {}                   # Track active page processing threads
        pending_results = {}                       # Store results by page number
        completed_pages = set()                    # Set of completed page numbers
        page_metadata_progress = {}                # Track metadata progress by page
        
        # Define database operation types
        DB_CHECK_PAGE = 1         # Check if page is already completed
        DB_INSERT_FAILED = 2      # Insert into failed_pages
        DB_PROCESS_IMAGES = 3     # Process a list of images
        DB_MARK_COMPLETED = 4     # Mark a page as completed
        DB_UPDATE_AUTHOR = 5      # Update author stats
        
        # Helper function to process a single page in its own thread
        def process_page_thread(page_number, page_url):
            """Thread function to process a single page"""
            logger.info(f"Starting processing for page {page_number}")
            try:
                # First check if page already exists - ask main thread to check
                response_queue = queue.Queue()
                db_command_queue.put((
                    DB_CHECK_PAGE, 
                    {
                        'page_number': page_number,
                        'callback_queue': response_queue
                    }
                ))
                
                # Wait for the response from the main thread
                check_result = response_queue.get()
                
                if check_result and check_result.get('exists', False):
                    # Page exists and has images
                    if check_result.get('image_count', 0) > 0:
                        logger.info(f"Skipping page {page_number} - already completed with {check_result['image_count']} images")
                        pending_results[page_number] = {
                            'success': True, 
                            'skipped': True,
                            'image_count': check_result['image_count']
                        }
                        return
                    else:
                        logger.info(f"Retrying page {page_number} - completed but had 0 images")
                
                # Track page size before downloading
                page_size_before = sum(f.stat().st_size for f in Path(BASE_DIR).rglob('*') if f.is_file())
                
                # Create a dedicated session for this thread
                session = create_session(max_retries=3)
                
                try:
                    # Get images from the page
                    image_data_list = get_image_links(page_url, session=session)
                    
                    if not image_data_list:
                        logger.warning(f"No images found on page {page_number}")
                        
                        # Add to failed pages - ask main thread to do this
                        response_queue = queue.Queue()
                        db_command_queue.put((
                            DB_INSERT_FAILED,
                            {
                                'page_number': page_number,
                                'page_url': page_url,
                                'error': "Page had 0 images",
                                'status': 'zero_images',
                                'callback_queue': response_queue
                            }
                        ))
                        
                        # Wait for completion
                        response_queue.get()
                        
                        pending_results[page_number] = {
                            'success': False,
                            'image_count': 0,
                            'message': 'No images found'
                        }
                        return
                    
                    # Request image processing from the main thread
                    result_queue = queue.Queue()
                    db_command_queue.put((
                        DB_PROCESS_IMAGES,
                        {
                            'image_data_list': image_data_list,
                            'page_number': page_number,
                            'callback_queue': result_queue
                        }
                    ))
                    
                    # Wait for processing result
                    process_result = result_queue.get()
                    success = process_result.get('success', False)
                    stats = process_result.get('stats', {})
                    
                    # Calculate space used by this page
                    page_size_after = sum(f.stat().st_size for f in Path(BASE_DIR).rglob('*') if f.is_file())
                    page_downloaded_bytes = page_size_after - page_size_before
                    
                    if success:
                        # Mark page as completed - ask main thread to do this
                        response_queue = queue.Queue()
                        db_command_queue.put((
                            DB_MARK_COMPLETED,
                            {
                                'page_number': page_number,
                                'image_count': stats['processed_count'],
                                'downloaded_bytes': page_downloaded_bytes,
                                'callback_queue': response_queue
                            }
                        ))
                        
                        # Wait for completion
                        response_queue.get()
                        
                        # Update author stats - ask main thread to do this
                        for author, count in stats['author_counts'].items():
                            response_queue = queue.Queue()
                            db_command_queue.put((
                                DB_UPDATE_AUTHOR,
                                {
                                    'author': author,
                                    'count': count,
                                    'callback_queue': response_queue
                                }
                            ))
                            
                            # Wait for completion
                            response_queue.get()
                        
                        pending_results[page_number] = {
                            'success': True,
                            'image_count': stats['processed_count'],
                            'stats': stats,
                            'downloaded_bytes': page_downloaded_bytes,
                            'message': 'Completed successfully'
                        }
                    else:
                        # If processing wasn't completely successful, add to failed_pages
                        error_msg = stats.get('error', 'Unknown error')
                        
                        # Add to failed pages - ask main thread to do this
                        response_queue = queue.Queue()
                        db_command_queue.put((
                            DB_INSERT_FAILED,
                            {
                                'page_number': page_number,
                                'page_url': page_url,
                                'error': error_msg,
                                'status': 'pending',
                                'callback_queue': response_queue
                            }
                        ))
                        
                        # Wait for completion
                        response_queue.get()
                        
                        pending_results[page_number] = {
                            'success': False,
                            'image_count': stats['processed_count'],
                            'stats': stats,
                            'downloaded_bytes': page_downloaded_bytes,
                            'message': f'Failed with error: {error_msg}'
                        }
                
                except requests.exceptions.HTTPError as e:
                    if any(str(code) in str(e) for code in [503, 504, 500, 502, 507, 508, 509]):
                        # Handle 50x Server Error - add to failed pages
                        error_code = re.search(r'\((\d+)\)', str(e))
                        error_code = error_code.group(1) if error_code else 'unknown'
                        logger.error(f"Server Error ({error_code}) for page {page_number}")
                        
                        # Add to failed pages - ask main thread to do this
                        response_queue = queue.Queue()
                        db_command_queue.put((
                            DB_INSERT_FAILED,
                            {
                                'page_number': page_number,
                                'page_url': page_url,
                                'error': f"Server Error ({error_code})",
                                'status': 'server_error',
                                'callback_queue': response_queue
                            }
                        ))
                        
                        # Wait for completion
                        response_queue.get()
                        
                        pending_results[page_number] = {
                            'success': False,
                            'image_count': 0,
                            'message': f'Server error {error_code}'
                        }
                    else:
                        # Handle other HTTP errors
                        logger.error(f"HTTP error for page {page_number}: {e}")
                        
                        # Add to failed pages - ask main thread to do this
                        response_queue = queue.Queue()
                        db_command_queue.put((
                            DB_INSERT_FAILED,
                            {
                                'page_number': page_number,
                                'page_url': page_url,
                                'error': str(e),
                                'status': 'error',
                                'callback_queue': response_queue
                            }
                        ))
                        
                        # Wait for completion
                        response_queue.get()
                        
                        pending_results[page_number] = {
                            'success': False,
                            'image_count': 0,
                            'message': f'HTTP error: {str(e)}'
                        }
                
                logger.info(f"Completed processing for page {page_number}")
                
            except Exception as e:
                logger.error(f"Error processing page {page_number}: {e}")
                
                try:
                    # Add to failed pages - ask main thread to do this
                    response_queue = queue.Queue()
                    db_command_queue.put((
                        DB_INSERT_FAILED,
                        {
                            'page_number': page_number,
                            'page_url': page_url,
                            'error': str(e),
                            'status': 'error',
                            'callback_queue': response_queue
                        }
                    ))
                    
                    # Wait for completion
                    response_queue.get()
                except Exception as inner_e:
                    logger.error(f"Failed to log error in database: {inner_e}")
                
                pending_results[page_number] = {
                    'success': False,
                    'image_count': 0,
                    'message': f'Error: {str(e)}'
                }
        
        # Update metadata progress and potentially start the next page
        def update_metadata_progress(page_number, current, total):
            """Update progress for a page and possibly start next page processing"""
            progress = current / total if total > 0 else 0
            page_metadata_progress[page_number] = progress
            
            # If we've processed 80% of the metadata for the current page, start the next page
            if progress >= 0.8 and page_number + 1 not in active_page_threads and page_number + 1 not in completed_pages:
                try:
                    # Only start next page if not at capacity and not already queued
                    if not page_queue.full() and page_number + 1 < TOTAL_PAGES:
                        next_page_url = get_search_url(page_number + 1)
                        page_queue.put((page_number + 1, next_page_url), block=False)
                        logger.info(f"Queued page {page_number + 1} after reaching 80% metadata on page {page_number}")
                except queue.Full:
                    # Queue is full, we'll try again later
                    logger.debug(f"Page queue full, waiting to queue page {page_number + 1}")
        
        # Queue the initial page to start processing
        initial_page = start_offset
        page_queue.put((initial_page, get_search_url(initial_page)))
        logger.info(f"Queued initial page {initial_page}")
        
        # Function to handle database commands in the main thread
        def process_db_command(command_type, params):
            try:
                if command_type == DB_CHECK_PAGE:
                    # Check if a page is already completed
                    page_number = params['page_number']
                    callback_queue = params['callback_queue']
                    
                    cursor.execute("""
                        SELECT completion_date, image_count 
                        FROM completed_pages 
                        WHERE page_number = ?
                    """, (page_number,))
                    
                    result = cursor.fetchone()
                    if result:
                        completion_date, image_count = result
                        callback_queue.put({
                            'exists': True,
                            'completion_date': completion_date,
                            'image_count': image_count
                        })
                    else:
                        callback_queue.put({'exists': False})
                    
                elif command_type == DB_INSERT_FAILED:
                    # Insert a page into the failed_pages table
                    page_number = params['page_number']
                    page_url = params['page_url']
                    error = params['error']
                    status = params['status']
                    callback_queue = params['callback_queue']
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO failed_pages (
                            page_number, url, error, last_attempt, status, attempts
                        ) VALUES (?, ?, ?, ?, ?, 0)
                    """, (page_number, page_url, error, datetime.now().isoformat(), status))
                    
                    conn.commit()
                    callback_queue.put({'success': True})
                
                elif command_type == DB_PROCESS_IMAGES:
                    # Process a list of images
                    image_data_list = params['image_data_list']
                    page_number = params['page_number']
                    callback_queue = params['callback_queue']
                    
                    # Process images with progress tracking
                    success, stats = process_image_list(
                        image_data_list, 
                        conn, 
                        cursor,
                        progress_callback=lambda current, total: update_metadata_progress(page_number, current, total)
                    )
                    
                    callback_queue.put({
                        'success': success,
                        'stats': stats
                    })
                
                elif command_type == DB_MARK_COMPLETED:
                    # Mark a page as completed
                    page_number = params['page_number']
                    image_count = params['image_count']
                    downloaded_bytes = params['downloaded_bytes']
                    callback_queue = params['callback_queue']
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO completed_pages (
                            page_number, completion_date, image_count, total_size_bytes
                        ) VALUES (?, ?, ?, ?)
                    """, (page_number, datetime.now().isoformat(), image_count, downloaded_bytes))
                    
                    conn.commit()
                    callback_queue.put({'success': True})
                
                elif command_type == DB_UPDATE_AUTHOR:
                    # Update author stats
                    author = params['author']
                    count = params['count']
                    callback_queue = params['callback_queue']
                    
                    cursor.execute("""
                        UPDATE author_stats 
                        SET total_images = total_images + ?,
                            last_updated = ?
                        WHERE author = ?
                    """, (count, datetime.now().isoformat(), author))
                    
                    conn.commit()
                    callback_queue.put({'success': True})
                
                return True
            except Exception as e:
                logger.error(f"Error processing database command {command_type}: {e}")
                if 'callback_queue' in params:
                    params['callback_queue'].put({'success': False, 'error': str(e)})
                return False
        
        # Main processing loop
        with tqdm(desc="Pages processed", colour='blue', position=0) as page_pbar:
            while True:
                try:
                    # Check for restart timer
                    if should_restart:
                        logger.info("Restart timer triggered, initiating restart...")
                        return
                    
                    # Check disk space periodically
                    free_space_gb = get_free_space_gb(BASE_DIR)
                    if free_space_gb < 2:
                        logger.critical(f"Only {free_space_gb:.2f}GB free space - stopping crawler")
                        raise RuntimeError(f"Critical: Only {free_space_gb:.2f}GB space remaining.")
                    
                    # Process database commands first
                    try:
                        while True:
                            cmd = db_command_queue.get_nowait()
                            process_db_command(cmd[0], cmd[1])
                    except queue.Empty:
                        pass
                    
                    # Clean up completed threads and process results
                    for page_num in list(active_page_threads.keys()):
                        thread = active_page_threads[page_num]
                        if not thread.is_alive():
                            # Thread is done
                            del active_page_threads[page_num]
                            completed_pages.add(page_num)
                            page_pbar.update(1)
                            
                            # Process the results
                            if page_num in pending_results:
                                result = pending_results[page_num]
                                if result.get('success', False):
                                    # Successful page
                                    image_count = result.get('image_count', 0)
                                    if result.get('skipped', False):
                                        logger.info(f"Page {page_num} was already processed with {image_count} images")
                                    else:
                                        # Update downloaded bytes and page count for new pages
                                        downloaded_bytes = result.get('downloaded_bytes', 0)
                                        total_downloaded_bytes += downloaded_bytes
                                        pages_processed += 1
                                        
                                        stats = result.get('stats', {})
                                        logger.info(f"Successfully completed page {page_num} - "
                                                 f"Processed: {stats.get('processed_count', 0)}, "
                                                 f"Failed: {stats.get('failed_count', 0)}, "
                                                 f"Total: {stats.get('total_images', 0)}, "
                                                 f"Workers: {stats.get('current_workers', 0)}")
                                else:
                                    # Failed page
                                    logger.warning(f"Failed to process page {page_num}: {result.get('message', 'Unknown error')}")
                    
                    # Start new threads if needed and we're below the worker limit
                    if len(active_page_threads) < 3:  # Maximum concurrent pages
                        try:
                            page_number, page_url = page_queue.get(block=False)
                            
                            # Skip if already processing or completed
                            if page_number in active_page_threads or page_number in completed_pages:
                                continue
                            
                            # Start the thread
                            thread = threading.Thread(
                                target=process_page_thread,
                                args=(page_number, page_url)
                            )
                            thread.daemon = True
                            thread.start()
                            active_page_threads[page_number] = thread
                            logger.info(f"Started processing thread for page {page_number}")
                        except queue.Empty:
                            # Nothing in the queue right now
                            pass
                    
                    # Check if we've reached the end of processing
                    if not active_page_threads and page_queue.empty():
                        highest_completed = max(completed_pages) if completed_pages else initial_page - 1
                        if highest_completed >= TOTAL_PAGES - 1:
                            logger.info("Reached the last page, crawl complete")
                            break
                        elif highest_completed < TOTAL_PAGES - 1:
                            # Queue the next page after the highest completed one
                            next_page = highest_completed + 1
                            logger.info(f"Queueing next page {next_page} after completion")
                            page_queue.put((next_page, get_search_url(next_page)))
                    
                    # Short sleep to prevent busy waiting
                    time.sleep(0.5)
                
                except KeyboardInterrupt:
                    logger.info("Keyboard interrupt received, stopping...")
                    break
        
        logger.info("\nCrawling Summary:")
        logger.info(f"Pages processed: {pages_processed}")
        logger.info(f"Total downloaded: {total_downloaded_bytes / (1024*1024*1024):.2f}GB")
        logger.info(f"Final worker count: {current_workers}")
        logger.info("Crawl completed successfully")
    
    except Exception as e:
        logger.error(f"Crawl failed with error: {e}")
        logger.exception("Exception details:")
    
    finally:
        # Ensure database connection is closed
        try:
            conn.close()
        except:
            pass

def test_album_extraction():
    """Test function to verify album and collection extraction from specific pages."""
    # Test case 1: Page with only albums
    url1 = "https://indafoto.hu/vasvarvar/image/27344784-e761530c/815586"
    print("\nTesting URL 1 (albums only):", url1)
    metadata1 = extract_metadata(url1)
    if metadata1:
        print("\nCollections:")
        for collection in metadata1['collections']:
            print(f"- {collection['title']} (ID: {collection['id']})")
        print("\nAlbums:")
        for album in metadata1['albums']:
            print(f"- {album['title']} (ID: {album['id']})")
    else:
        print("Failed to extract metadata from URL 1")

    # Test case 2: Page with both albums and collections
    url2 = "https://indafoto.hu/jani58/image/27055949-52959d03"
    print("\nTesting URL 2 (albums and collections):", url2)
    metadata2 = extract_metadata(url2)
    if metadata2:
        print("\nCollections:")
        for collection in metadata2['collections']:
            print(f"- {collection['title']} (ID: {collection['id']})")
        print("\nAlbums:")
        for album in metadata2['albums']:
            print(f"- {album['title']} (ID: {album['id']})")
    else:
        print("Failed to extract metadata from URL 2")

def test_tag_extraction():
    """Test function to verify tag extraction from specific pages."""
    # Test case: Page with specific tags
    url = "https://indafoto.hu/vertesi76/image/27063213-3c4ac9f5"
    print("\nTesting tag extraction from URL:", url)
    metadata = extract_metadata(url)
    if metadata:
        print("\nTags:")
        for tag in metadata['tags']:
            print(f"- {tag['name']} (count: {tag['count']})")
    else:
        print("Failed to extract metadata from URL")

def test_camera_extraction():
    """Test function to verify camera make and model extraction from specific pages."""
    test_cases = [
        {
            'url': 'https://indafoto.hu/jani58/image/27055949-52959d03',
            'expected_make': 'SONY',
            'expected_model': 'DSC-HX350'
        },
        {
            'url': 'https://indafoto.hu/yegenye/image/27024997-bb54932f',
            'expected_make': 'Apple',
            'expected_model': 'iPhone SE (2nd generation)'
        }
    ]
    
    print("\nTesting camera make and model extraction:")
    for case in test_cases:
        print(f"\nTesting URL: {case['url']}")
        metadata = extract_metadata(case['url'])
        if metadata:
            print(f"Extracted make: '{metadata['camera_make']}'")
            print(f"Extracted model: '{metadata['camera_model']}'")
            print(f"Expected make: '{case['expected_make']}'")
            print(f"Expected model: '{case['expected_model']}'")
            print(f"Make matches: {metadata['camera_make'] == case['expected_make']}")
            print(f"Model matches: {metadata['camera_model'] == case['expected_model']}")
        else:
            print("Failed to extract metadata from URL")

def delete_image_data(conn, cursor, image_id):
    """Delete all database entries related to an image."""
    try:
        # Delete from image_collections
        cursor.execute("DELETE FROM image_collections WHERE image_id = ?", (image_id,))
        
        # Delete from image_albums
        cursor.execute("DELETE FROM image_albums WHERE image_id = ?", (image_id,))
        cursor.execute("DELETE FROM image_tags WHERE image_id = ?", (image_id,))
        
        # Delete from marked_images if exists
        cursor.execute("DELETE FROM marked_images WHERE image_id = ?", (image_id,))
        
        # Delete from image_notes if exists
        cursor.execute("DELETE FROM image_notes WHERE image_id = ?", (image_id,))
        
        # Finally delete the image record
        cursor.execute("DELETE FROM images WHERE id = ?", (image_id,))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Failed to delete image data for ID {image_id}: {e}")
        conn.rollback()
        return False

def redownload_author_images(author_name):
    """Redownload all images from a specific author."""
    conn = init_db()
    cursor = conn.cursor()
    
    logger.info(f"Starting redownload of images for author: {author_name}")
    
    # Get all images from this author
    cursor.execute("""
        SELECT id, url, page_url, local_path, sha256_hash
        FROM images
        WHERE author = ?
        ORDER BY id
    """, (author_name,))
    
    images = cursor.fetchall()
    if not images:
        logger.info(f"No images found for author {author_name}")
        return
        
    logger.info(f"Found {len(images)} images to redownload")
    
    # Create a session to reuse for all requests
    session = create_session()
    
    # Keep track of changes
    redownloaded = 0
    failed = 0
    different = 0
    
    try:
        for image in tqdm(images, desc=f"Redownloading images for {author_name}", colour='yellow'):
            image_id, url, page_url, old_path, old_hash = image
            
            try:
                # Get fresh metadata from the page
                metadata = extract_metadata(page_url, session=session)
                if not metadata:
                    logger.error(f"Failed to get metadata for {page_url}")
                    failed += 1
                    continue
                
                # Try to get high-res version of the image using our optimized function
                download_url = get_high_res_url(url, session=session)
                
                # Download the image
                new_path, new_hash = download_image(download_url, author_name, session=session)
                
                if not new_path or not new_hash:
                    logger.error(f"Failed to download {download_url}")
                    failed += 1
                    continue
                
                # Compare hashes
                if new_hash != old_hash:
                    logger.warning(f"Hash mismatch for {url}")
                    logger.warning(f"Old hash: {old_hash}")
                    logger.warning(f"New hash: {new_hash}")
                    different += 1
                    
                    # Delete old database entries
                    delete_image_data(conn, cursor, image_id)
                    
                    # Insert new data
                    cursor.execute("""
                        INSERT INTO images (
                            url, local_path, sha256_hash, title, author, author_url,
                            license, camera_make, camera_model, focal_length, aperture,
                            shutter_speed, taken_date, upload_date, page_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        url, new_path, new_hash, metadata['title'],
                        metadata['author'], metadata['author_url'], metadata['license'],
                        metadata['camera_make'], metadata['camera_model'], metadata['focal_length'],
                        metadata['aperture'], metadata['shutter_speed'],
                        metadata['taken_date'], metadata['upload_date'], metadata['page_url']
                    ))
                    
                    # Get the new image ID
                    new_image_id = cursor.lastrowid
                    
                    # Process galleries
                    for gallery in metadata.get('collections', []):
                        cursor.execute("""
                            INSERT OR IGNORE INTO collections (collection_id, title, url, is_public)
                            VALUES (?, ?, ?, ?)
                        """, (gallery['id'], gallery['title'], gallery['url'], gallery['is_public']))
                        
                        cursor.execute("SELECT id FROM collections WHERE collection_id = ?", (gallery['id'],))
                        gallery_db_id = cursor.fetchone()[0]
                        
                        cursor.execute("""
                            INSERT INTO image_collections (image_id, collection_id)
                            VALUES (?, ?)
                        """, (new_image_id, gallery_db_id))
                    
                    # Process albums
                    for gallery in metadata.get('albums', []):
                        cursor.execute("""
                            INSERT OR IGNORE INTO albums (album_id, title, url, is_public)
                            VALUES (?, ?, ?, ?)
                        """, (gallery['id'], gallery['title'], gallery['url'], gallery['is_public']))
                        
                        cursor.execute("SELECT id FROM albums WHERE album_id = ?", (gallery['id'],))
                        gallery_db_id = cursor.fetchone()[0]
                        
                        cursor.execute("""
                            INSERT INTO image_albums (image_id, album_id)
                            VALUES (?, ?)
                        """, (new_image_id, gallery_db_id))
                    
                    # Process tags
                    for tag in metadata.get('tags', []):
                        cursor.execute("""
                            INSERT OR IGNORE INTO tags (name, count)
                            VALUES (?, ?)
                        """, (tag['name'], tag['count']))
                        
                        cursor.execute("SELECT id FROM tags WHERE name = ?", (tag['name'],))
                        tag_db_id = cursor.fetchone()[0]
                        
                        cursor.execute("""
                            INSERT INTO image_tags (image_id, tag_id)
                            VALUES (?, ?)
                        """, (new_image_id, tag_db_id))
                    
                    conn.commit()
                    
                    # Delete the old file
                    try:
                        if os.path.exists(old_path):
                            os.remove(old_path)
                    except Exception as e:
                        logger.error(f"Failed to delete old file {old_path}: {e}")
                    
                    redownloaded += 1
                    
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                failed += 1
                continue
    finally:
        # Clean up the session
        session.close()
    
    # Print summary
    logger.info("\nRedownload Summary:")
    logger.info(f"Total images processed: {len(images)}")
    logger.info(f"Successfully redownloaded: {redownloaded}")
    logger.info(f"Different from original: {different}")
    logger.info(f"Failed: {failed}")
    
    conn.close()

def get_high_res_url(url, session=None):
    """Try to get the highest resolution version of an image URL."""
    if '_l.jpg' not in url:
        return url
        
    # Only request a tiny amount of data to verify existence (1024 bytes)
    headers = {'Range': 'bytes=0-1023'}
    # Very short timeout - fail fast
    timeout = 1.5
    
    original_adapters = None
    try:
        if session:
            # Save original adapters
            original_adapters = {
                'http': session.adapters['http://'],
                'https': session.adapters['https://']
            }
            # Create new adapters with no retries for this specific use
            no_retry_adapter = requests.adapters.HTTPAdapter(max_retries=0)
            session.mount('https://', no_retry_adapter)
            session.mount('http://', no_retry_adapter)
        else:
            # Create a fresh session with no retries
            session = requests.Session()
            session.headers.update(HEADERS)
            session.cookies.update(COOKIES)
            no_retry_adapter = requests.adapters.HTTPAdapter(max_retries=0)
            session.mount('https://', no_retry_adapter)
            session.mount('http://', no_retry_adapter)
        
        # Try higher resolution versions in order
        for res in ['_xxl.jpg', '_xl.jpg']:
            test_url = url.replace('_l.jpg', res)
            try:
                response = session.get(
                    test_url, 
                    headers=headers,
                    timeout=timeout, 
                    stream=True
                )
                
                # If the response is successful (200 OK or 206 Partial Content)
                if response.status_code in [200, 206]:
                    # Read just a tiny bit to verify the content exists
                    for chunk in response.iter_content(chunk_size=256):
                        if chunk:  # Filter out keep-alive new chunks
                            response.close()
                            return test_url
                        break  # Only need the first chunk
                    
                # Not found, close the response
                response.close()
                    
            except (requests.exceptions.Timeout, 
                    requests.exceptions.ConnectionError,
                    OSError) as e:
                # Silent failure - just continue to next resolution
                continue
            finally:
                # Ensure response is closed in all cases
                if 'response' in locals():
                    try:
                        response.close()
                    except:
                        pass
                    
        # If we can't find higher res versions, return the original
        return url
    finally:
        # Restore original adapters if we modified an existing session
        if original_adapters:
            session.mount('http://', original_adapters['http'])
            session.mount('https://', original_adapters['https'])

def run_benchmark(iterations=100):
    """Run performance benchmarks on critical components."""
    import time
    import platform
    import psutil
    
    results = {
        "system_info": {
            "platform": platform.system(),
            "platform_version": platform.version(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
            "cpu_count": psutil.cpu_count(logical=False),
            "logical_cpu_count": psutil.cpu_count(logical=True),
            "memory_total": psutil.virtual_memory().total // (1024 * 1024)
        },
        "benchmarks": {}
    }
    
    logger.info(f"Starting benchmark on {platform.system()} with {iterations} iterations")
    
    # CPU usage monitoring function
    def monitor_cpu_usage(duration, interval=0.1):
        """Monitor CPU usage over a period of time."""
        start_time = time.time()
        cpu_usage = []
        process = psutil.Process()
        
        # Get initial CPU times for process
        initial_proc_cpu_times = process.cpu_times()
        initial_sys_cpu_times = psutil.cpu_times()
        initial_real_time = time.time()
        
        while time.time() - start_time < duration:
            # Get current CPU utilization percentage for this process
            current_proc_percent = process.cpu_percent(interval=0)
            system_percent = psutil.cpu_percent(interval=0)
            
            # Get detailed CPU time information
            current_proc_cpu_times = process.cpu_times()
            current_sys_cpu_times = psutil.cpu_times()
            current_real_time = time.time()
            
            # Calculate deltas
            real_time_delta = current_real_time - initial_real_time
            
            if real_time_delta > 0:
                proc_user_delta = current_proc_cpu_times.user - initial_proc_cpu_times.user
                proc_system_delta = current_proc_cpu_times.system - initial_proc_cpu_times.system
                proc_total_delta = proc_user_delta + proc_system_delta
                
                # Calculate CPU utilization as percentage of real time
                proc_utilization = (proc_total_delta / real_time_delta) * 100.0
                
                # On multi-core systems, this can exceed 100%
                proc_utilization_per_core = proc_utilization / psutil.cpu_count()
                
                # Record data
                cpu_usage.append({
                    'timestamp': time.time() - start_time,
                    'process_percent': current_proc_percent,
                    'system_percent': system_percent,
                    'process_utilization': proc_utilization,
                    'process_utilization_per_core': proc_utilization_per_core,
                    'num_threads': len(process.threads())
                })
            
            time.sleep(interval)
        
        return {
            'samples': cpu_usage,
            'avg_process_percent': sum(sample['process_percent'] for sample in cpu_usage) / len(cpu_usage) if cpu_usage else 0,
            'max_process_percent': max(sample['process_percent'] for sample in cpu_usage) if cpu_usage else 0,
            'avg_system_percent': sum(sample['system_percent'] for sample in cpu_usage) / len(cpu_usage) if cpu_usage else 0,
            'max_system_percent': max(sample['system_percent'] for sample in cpu_usage) if cpu_usage else 0,
            'avg_process_utilization': sum(sample['process_utilization'] for sample in cpu_usage) / len(cpu_usage) if cpu_usage else 0,
            'max_process_utilization': max(sample['process_utilization'] for sample in cpu_usage) if cpu_usage else 0,
            'avg_threads': sum(sample['num_threads'] for sample in cpu_usage) / len(cpu_usage) if cpu_usage else 0,
            'max_threads': max(sample['num_threads'] for sample in cpu_usage) if cpu_usage else 0
        }
    
    # Benchmark SQLite operations
    logger.info("Benchmarking SQLite operations...")
    start_time = time.time()
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    
    # Create test tables
    cursor.execute("""
        CREATE TABLE test_images (
            id INTEGER PRIMARY KEY,
            image_id TEXT UNIQUE,
            title TEXT,
            url TEXT,
            author TEXT,
            date TEXT,
            page_url TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE test_tags (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            count INTEGER
        )
    """)
    
    cursor.execute("""
        CREATE TABLE test_image_tags (
            id INTEGER PRIMARY KEY,
            image_id INTEGER,
            tag_id INTEGER
        )
    """)
    
    # Benchmark single inserts vs batch inserts
    single_insert_start = time.time()
    for i in range(iterations):
        cursor.execute("""
            INSERT INTO test_images (image_id, title, url, author, date, page_url)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (f"img_{i}", f"Title {i}", f"http://example.com/img_{i}.jpg", 
              f"author_{i % 10}", f"2023-01-{i % 30 + 1}", f"http://example.com/page_{i}"))
        
        # Insert tags
        for j in range(5):
            cursor.execute("""
                INSERT OR IGNORE INTO test_tags (name, count)
                VALUES (?, ?)
            """, (f"tag_{j}_{i % 20}", i * j))
            
            cursor.execute("SELECT id FROM test_tags WHERE name = ?", (f"tag_{j}_{i % 20}",))
            tag_id = cursor.fetchone()[0]
            
            cursor.execute("""
                INSERT INTO test_image_tags (image_id, tag_id)
                VALUES (?, ?)
            """, (i, tag_id))
            
        if i % 10 == 0:
            conn.commit()
    
    conn.commit()
    single_insert_time = time.time() - single_insert_start
    
    # Benchmark batch operations
    cursor.execute("DELETE FROM test_images")
    cursor.execute("DELETE FROM test_tags")
    cursor.execute("DELETE FROM test_image_tags")
    conn.commit()
    
    batch_insert_start = time.time()
    image_data = [(f"img_{i}", f"Title {i}", f"http://example.com/img_{i}.jpg", 
                   f"author_{i % 10}", f"2023-01-{i % 30 + 1}", f"http://example.com/page_{i}")
                  for i in range(iterations)]
    
    cursor.executemany("""
        INSERT INTO test_images (image_id, title, url, author, date, page_url)
        VALUES (?, ?, ?, ?, ?, ?)
    """, image_data)
    
    tags_data = [(f"tag_{j}_{i % 20}", i * j) 
                for i in range(iterations) 
                for j in range(5)]
    
    cursor.executemany("""
        INSERT OR IGNORE INTO test_tags (name, count)
        VALUES (?, ?)
    """, tags_data)
    
    # Need to get tag IDs for the many-to-many relationships
    image_tags_data = []
    for i in range(iterations):
        for j in range(5):
            cursor.execute("SELECT id FROM test_tags WHERE name = ?", (f"tag_{j}_{i % 20}",))
            tag_id = cursor.fetchone()[0]
            image_tags_data.append((i, tag_id))
    
    cursor.executemany("""
        INSERT INTO test_image_tags (image_id, tag_id)
        VALUES (?, ?)
    """, image_tags_data)
    
    conn.commit()
    batch_insert_time = time.time() - batch_insert_start
    conn.close()
    
    results["benchmarks"]["sqlite"] = {
        "single_inserts_ms": single_insert_time * 1000,
        "batch_inserts_ms": batch_insert_time * 1000,
        "single_to_batch_ratio": single_insert_time / batch_insert_time
    }
    
    # Benchmark threading with SQLite - this is likely where Windows vs Mac differences occur
    logger.info("Benchmarking SQLite under threading load...")
    
    # Start CPU monitoring in a separate thread
    cpu_monitor_thread = threading.Thread(target=lambda: None)  # Placeholder
    cpu_monitor_results = {}
    
    # Create a shared database file
    db_path = "benchmark_threading.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE test_data (
            id INTEGER PRIMARY KEY,
            thread_id INTEGER,
            iteration INTEGER,
            timestamp REAL,
            value TEXT
        )
    """)
    conn.commit()
    conn.close()
    
    # Track time spent waiting for locks
    lock_wait_times = []
    thread_results = []
    
    # Use a threading event to coordinate all threads starting at once
    start_event = threading.Event()
    complete_count = {"value": 0}
    lock = threading.Lock()

    # Worker function that performs database operations
    def db_worker(thread_id, iterations):
        # Wait for start signal
        start_event.wait()
        
        thread_start_time = time.time()
        local_lock_waits = []
        
        try:
            # Each thread gets its own connection - this is key
            local_conn = sqlite3.connect(db_path, timeout=30.0)  # 30 second timeout
            local_cursor = local_conn.cursor()
            
            for i in range(iterations):
                # Time how long the transaction takes (may include lock waiting)
                txn_start = time.time()
                
                try:
                    # Start transaction
                    local_cursor.execute("BEGIN IMMEDIATE")
                    
                    # Got lock, record actual work time without lock wait
                    work_start = time.time()
                    lock_wait = work_start - txn_start
                    local_lock_waits.append(lock_wait)
                    
                    # Do some work
                    local_cursor.execute("""
                        INSERT INTO test_data (thread_id, iteration, timestamp, value)
                        VALUES (?, ?, ?, ?)
                    """, (thread_id, i, time.time(), f"Data from thread {thread_id}, iteration {i}"))
                    
                    # Add a small select query too
                    local_cursor.execute("""
                        SELECT COUNT(*) FROM test_data WHERE thread_id = ?
                    """, (thread_id,))
                    
                    # Simulate some processing time
                    _ = sum(range(10000))
                    
                    # Commit transaction
                    local_conn.commit()
                    
                except sqlite3.OperationalError as e:
                    # If we get a timeout or busy error, log it
                    logger.warning(f"Thread {thread_id} got SQLite error: {e}")
                    local_conn.rollback()
                
                # Small sleep to yield to other threads
                time.sleep(0.001)
            
            local_conn.close()
            
        except Exception as e:
            logger.error(f"Thread {thread_id} failed: {e}")
        
        thread_time = time.time() - thread_start_time
        
        # Report results
        with lock:
            thread_results.append({
                "thread_id": thread_id,
                "time_ms": thread_time * 1000,
                "iterations": iterations,
                "avg_lock_wait_ms": sum(local_lock_waits) * 1000 / len(local_lock_waits) if local_lock_waits else 0,
                "max_lock_wait_ms": max(local_lock_waits) * 1000 if local_lock_waits else 0
            })
            complete_count["value"] += 1
    
    # Create and start threads
    thread_count = min(8, psutil.cpu_count(logical=True))
    iterations_per_thread = max(10, iterations // 5)  # Reduce for threading test
    logger.info(f"Starting {thread_count} threads with {iterations_per_thread} iterations each")
    
    # Start CPU monitoring in a separate thread
    monitor_complete = {"value": False}
    cpu_results_container = {"results": None}
    
    def cpu_monitor_func():
        cpu_results_container["results"] = monitor_cpu_usage(60)  # Monitor for up to 60 seconds
        
    cpu_monitor_thread = threading.Thread(target=cpu_monitor_func)
    cpu_monitor_thread.daemon = True
    cpu_monitor_thread.start()
    
    threads = []
    for i in range(thread_count):
        t = threading.Thread(target=db_worker, args=(i, iterations_per_thread))
        threads.append(t)
        t.start()
    
    # Let all threads start at the same time for maximum contention
    start_event.set()
    
    # Wait for threads to complete
    thread_benchmark_start = time.time()
    while complete_count["value"] < thread_count:
        time.sleep(0.1)
        if time.time() - thread_benchmark_start > 300:  # 5 minute timeout
            logger.error("Threading benchmark timed out")
            break
    
    thread_benchmark_time = time.time() - thread_benchmark_start
    
    # Give the CPU monitor a moment to record final stats
    time.sleep(1)
    monitor_complete["value"] = True
    cpu_monitor_thread.join(2)  # Wait for CPU monitoring to finish, timeout after 2 seconds
    
    # Get CPU monitoring results
    cpu_monitoring = cpu_results_container["results"]
    if cpu_monitoring:
        results["benchmarks"]["cpu_usage"] = {
            "avg_process_percent": cpu_monitoring["avg_process_percent"],
            "max_process_percent": cpu_monitoring["max_process_percent"],
            "avg_system_percent": cpu_monitoring["avg_system_percent"],
            "max_system_percent": cpu_monitoring["max_system_percent"],
            "avg_process_utilization": cpu_monitoring["avg_process_utilization"],
            "max_process_utilization": cpu_monitoring["max_process_utilization"],
            "avg_threads": cpu_monitoring["avg_threads"],
            "max_threads": cpu_monitoring["max_threads"]
        }
    
    # Calculate threading metrics
    avg_lock_wait = sum(t["avg_lock_wait_ms"] for t in thread_results) / len(thread_results)
    max_lock_wait = max(t["max_lock_wait_ms"] for t in thread_results)
    total_operations = thread_count * iterations_per_thread
    operations_per_sec = total_operations / thread_benchmark_time
    
    results["benchmarks"]["sqlite_threading"] = {
        "total_time_ms": thread_benchmark_time * 1000,
        "thread_count": thread_count,
        "operations_per_thread": iterations_per_thread,
        "total_operations": total_operations,
        "operations_per_second": operations_per_sec,
        "avg_lock_wait_ms": avg_lock_wait,
        "max_lock_wait_ms": max_lock_wait,
        "thread_details": thread_results
    }
    
    # Clean up benchmark database
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except:
            pass
    
    # Benchmark hash calculation
    logger.info("Benchmarking hash calculations...")
    hash_start = time.time()
    test_data = os.urandom(1024 * 1024)  # 1MB random data
    for i in range(iterations):
        sha256_hash = hashlib.sha256()
        sha256_hash.update(test_data)
        hash_result = sha256_hash.hexdigest()
    hash_time = time.time() - hash_start
    
    results["benchmarks"]["hash_calculation"] = {
        "time_ms": hash_time * 1000,
        "iterations": iterations,
        "data_size": "1MB"
    }
    
    # Benchmark threading
    logger.info("Benchmarking thread pool...")
    thread_start = time.time()
    
    def thread_task(num):
        # Simulate some work
        time.sleep(0.01)
        result = 0
        for i in range(10000):
            result += i
        return result
    
    pool = ThreadPool(8, "benchmark")
    for i in range(iterations * 2):
        pool.add_task(thread_task, i)
    pool.wait_completion()
    thread_time = time.time() - thread_start
    
    results["benchmarks"]["threading"] = {
        "time_ms": thread_time * 1000,
        "iterations": iterations * 2,
        "thread_count": 8
    }
    
    # Overall results
    total_time = time.time() - start_time
    results["total_time_ms"] = total_time * 1000
    
    # Print results
    logger.info("Benchmark Results:")
    logger.info(f"System: {results['system_info']['platform']} {results['system_info']['platform_version']}")
    logger.info(f"Processor: {results['system_info']['processor']}")
    logger.info(f"CPU cores: {results['system_info']['cpu_count']} physical, {results['system_info']['logical_cpu_count']} logical")
    logger.info(f"Memory: {results['system_info']['memory_total']} MB")
    logger.info(f"Total time: {results['total_time_ms']:.2f}ms")
    logger.info("SQLite:")
    logger.info(f"  - Single inserts: {results['benchmarks']['sqlite']['single_inserts_ms']:.2f}ms")
    logger.info(f"  - Batch inserts: {results['benchmarks']['sqlite']['batch_inserts_ms']:.2f}ms")
    logger.info(f"  - Ratio (single/batch): {results['benchmarks']['sqlite']['single_to_batch_ratio']:.2f}x")
    logger.info("SQLite Threading:")
    logger.info(f"  - Total operations: {results['benchmarks']['sqlite_threading']['total_operations']}")
    logger.info(f"  - Operations/sec: {results['benchmarks']['sqlite_threading']['operations_per_second']:.2f}")
    logger.info(f"  - Avg lock wait: {results['benchmarks']['sqlite_threading']['avg_lock_wait_ms']:.2f}ms")
    logger.info(f"  - Max lock wait: {results['benchmarks']['sqlite_threading']['max_lock_wait_ms']:.2f}ms")
    
    if "cpu_usage" in results["benchmarks"]:
        logger.info("CPU Usage:")
        logger.info(f"  - Avg process CPU: {results['benchmarks']['cpu_usage']['avg_process_percent']:.2f}%")
        logger.info(f"  - Max process CPU: {results['benchmarks']['cpu_usage']['max_process_percent']:.2f}%")
        logger.info(f"  - Avg system CPU: {results['benchmarks']['cpu_usage']['avg_system_percent']:.2f}%")
        logger.info(f"  - Max system CPU: {results['benchmarks']['cpu_usage']['max_system_percent']:.2f}%")
        logger.info(f"  - Process utilization: {results['benchmarks']['cpu_usage']['avg_process_utilization']:.2f}%")
        logger.info(f"  - Avg thread count: {results['benchmarks']['cpu_usage']['avg_threads']:.2f}")
    
    logger.info(f"Hash calculation: {results['benchmarks']['hash_calculation']['time_ms']:.2f}ms")
    logger.info(f"Thread pool: {results['benchmarks']['threading']['time_ms']:.2f}ms")
    
    # Save results to file
    with open(f"benchmark_{platform.system().lower()}.json", "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Results saved to benchmark_{platform.system().lower()}.json")
    return results

def validate_image(filename, url, metadata, session=None):
    """Validate and hash a single image."""
    try:
        file_hash = calculate_file_hash(filename)
        return ('success', (filename, file_hash, url, metadata))
    except Exception as e:
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except:
            pass
        return ('error', ('validation', url, str(e)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indafoto Crawler')
    parser.add_argument('--start-offset', type=int, default=0,
                       help='Page offset to start crawling from')
    parser.add_argument('--retry', action='store_true',
                       help='Retry failed pages instead of normal crawling')
    parser.add_argument('--test', action='store_true',
                       help='Run test function instead of crawler')
    parser.add_argument('--test-tags', action='store_true',
                       help='Run tag extraction test')
    parser.add_argument('--test-camera', action='store_true',
                       help='Run camera make/model extraction test')
    parser.add_argument('--redownload-author',
                       help='Redownload all images from a specific author')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS,
                       help=f'Number of parallel download workers (default: {DEFAULT_WORKERS})')
    parser.add_argument('--ban-author',
                       help='Add an author to the banned list')
    parser.add_argument('--ban-reason',
                       help='Reason for banning an author')
    parser.add_argument('--unban-author',
                       help='Remove an author from the banned list')
    parser.add_argument('--cleanup-banned',
                       help='Clean up all content from a banned author')
    parser.add_argument('--no-update-check', action='store_true',
                       help='Skip checking for updates')
    parser.add_argument('--auto-restart', action='store_true',
                       help='Enable automatic restart every 24 hours (default: disabled)')
    parser.add_argument('--no-error-restart', action='store_true',
                       help='Disable automatic restart even in case of errors (default: enabled)')
    parser.add_argument('--benchmark', action='store_true',
                       help='Run performance benchmark tests')
    parser.add_argument('--benchmark-iterations', type=int, default=100,
                       help='Number of iterations for benchmark tests')
    parser.add_argument('--sqlite-test', choices=['connection-per-thread', 'shared-connection', 'connection-pool'],
                      help='Test specific SQLite connection patterns')
    parser.add_argument('--monitor', action='store_true',
                      help='Enable real-time CPU, memory and thread monitoring')
    parser.add_argument('--monitor-interval', type=int, default=5,
                      help='How often to log monitoring stats (in seconds)')
    parser.add_argument('--monitor-functions', action='store_true',
                      help='Try to identify CPU-intensive functions during monitoring')
    args = parser.parse_args()
    
    try:
        # Check for updates unless explicitly disabled
        if not args.no_update_check:
            check_for_updates()
            
        # Set the number of workers from command line argument
        current_workers = args.workers
        MAX_WORKERS = args.workers
        logger.info(f"Starting crawler with {current_workers} workers")
        
        # Start resource monitoring if requested
        stop_monitoring = None
        if args.monitor:
            stop_monitoring = monitor_system_resources(
                interval=args.monitor_interval,
                report_functions=args.monitor_functions
            )
        
        conn = init_db()
        
        if args.benchmark:
            run_benchmark(iterations=args.benchmark_iterations)
        elif args.sqlite_test:
            # Special SQLite threading tests
            def test_sqlite_threading_patterns(mode, thread_count=8, iterations=50):
                """Test different SQLite connection patterns under threading load."""
                import time
                import threading
                import sqlite3
                import psutil
                import queue
                
                logger.info(f"Testing SQLite {mode} mode with {thread_count} threads")
                
                # Create test database
                db_path = f"sqlite_test_{mode}.db"
                if os.path.exists(db_path):
                    os.remove(db_path)
                
                # Setup database
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE test_data (
                        id INTEGER PRIMARY KEY,
                        thread_id INTEGER,
                        iteration INTEGER,
                        timestamp REAL,
                        data TEXT
                    )
                """)
                conn.commit()
                
                # For connection pool
                connection_pool = queue.Queue()
                if mode == 'connection-pool':
                    # Pre-create connections
                    pool_size = min(thread_count * 2, 32)  # 2 connections per thread, max 32
                    for i in range(pool_size):
                        pool_conn = sqlite3.connect(db_path, timeout=30.0)
                        # Enable WAL mode for better concurrency
                        pool_conn.execute("PRAGMA journal_mode=WAL")
                        connection_pool.put(pool_conn)
                
                # For shared connection mode
                shared_conn = None
                shared_conn_lock = threading.Lock()
                
                if mode == 'shared-connection':
                    shared_conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
                    # Enable WAL mode for better concurrency
                    shared_conn.execute("PRAGMA journal_mode=WAL")
                
                # Track time spent waiting for locks
                workers_complete = {"count": 0}
                cpu_process = psutil.Process()
                
                # Get initial CPU times
                initial_process_time = sum(cpu_process.cpu_times()[:2])  # User + System time
                initial_cpu_percent = cpu_process.cpu_percent()
                
                start_time = time.time()
                
                # Create a monitor thread to track CPU usage
                cpu_samples = []
                
                def cpu_monitor():
                    while workers_complete["count"] < thread_count:
                        cpu_samples.append({
                            "timestamp": time.time() - start_time,
                            "cpu_percent": cpu_process.cpu_percent(interval=0.1),
                            "num_threads": len(cpu_process.threads())
                        })
                        time.sleep(0.2)
                
                monitor_thread = threading.Thread(target=cpu_monitor)
                monitor_thread.daemon = True
                monitor_thread.start()
                
                # Sync point for all threads to start at once
                ready_event = threading.Event()
                
                def worker(thread_id):
                    # Wait until all threads are ready
                    ready_event.wait()
                    
                    # Use appropriate connection based on mode
                    local_conn = None
                    
                    if mode == 'connection-per-thread':
                        local_conn = sqlite3.connect(db_path, timeout=30.0)
                        # Enable WAL mode for better concurrency
                        local_conn.execute("PRAGMA journal_mode=WAL")
                    
                    elif mode == 'connection-pool':
                        local_conn = connection_pool.get()
                    
                    success_count = 0
                    error_count = 0
                    lock_waits = []
                    
                    try:
                        for i in range(iterations):
                            start_op = time.time()
                            
                            try:
                                if mode == 'shared-connection':
                                    # Use the shared connection with a lock
                                    with shared_conn_lock:
                                        shared_conn.execute("""
                                            INSERT INTO test_data (thread_id, iteration, timestamp, data)
                                            VALUES (?, ?, ?, ?)
                                        """, (thread_id, i, time.time(), f"Data from thread {thread_id}, iteration {i}"))
                                        
                                        # Read some data
                                        shared_conn.execute("""
                                            SELECT COUNT(*) FROM test_data WHERE thread_id = ?
                                        """, (thread_id,))
                                        
                                        # Commit within the lock
                                        shared_conn.commit()
                                
                                else:  # connection-per-thread or connection-pool
                                    # Each thread has its own connection or from pool
                                    local_conn.execute("""
                                        INSERT INTO test_data (thread_id, iteration, timestamp, data)
                                        VALUES (?, ?, ?, ?)
                                    """, (thread_id, i, time.time(), f"Data from thread {thread_id}, iteration {i}"))
                                    
                                    # Read some data
                                    local_conn.execute("""
                                        SELECT COUNT(*) FROM test_data WHERE thread_id = ?
                                    """, (thread_id,))
                                    
                                    # Commit the transaction
                                    local_conn.commit()
                                
                                success_count += 1
                                
                            except sqlite3.OperationalError as e:
                                error_count += 1
                                if mode != 'shared-connection':  # No need to rollback with shared lock
                                    try:
                                        local_conn.rollback()
                                    except:
                                        pass
                            
                            end_op = time.time()
                            lock_waits.append((end_op - start_op) * 1000)  # ms
                            
                            # Small pause to reduce CPU contention
                            time.sleep(0.001)
                    
                    finally:
                        # Return connection to pool if using pool
                        if mode == 'connection-pool':
                            connection_pool.put(local_conn)
                        
                        # Close per-thread connection if using that mode
                        elif mode == 'connection-per-thread' and local_conn:
                            local_conn.close()
                    
                    # Report completion
                    with threading.Lock():
                        workers_complete["count"] += 1
                        logger.debug(f"Thread {thread_id} completed: {success_count} success, {error_count} errors")
                
                # Start worker threads
                threads = []
                for i in range(thread_count):
                    t = threading.Thread(target=worker, args=(i,))
                    threads.append(t)
                    t.start()
                
                # All threads can start now
                ready_event.set()
                
                # Wait for all workers to complete
                while workers_complete["count"] < thread_count:
                    time.sleep(0.1)
                
                elapsed = time.time() - start_time
                
                # Calculate CPU usage
                final_process_time = sum(cpu_process.cpu_times()[:2])
                cpu_time_used = final_process_time - initial_process_time
                
                # Calculate average CPU percentage
                avg_cpu = sum(sample["cpu_percent"] for sample in cpu_samples) / len(cpu_samples) if cpu_samples else 0
                max_cpu = max(sample["cpu_percent"] for sample in cpu_samples) if cpu_samples else 0
                
                # Clean up
                if mode == 'shared-connection' and shared_conn:
                    shared_conn.close()
                
                if mode == 'connection-pool':
                    while not connection_pool.empty():
                        conn = connection_pool.get_nowait()
                        conn.close()
                
                # Get database size
                db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
                
                # Report results
                logger.info(f"SQLite {mode} Results:")
                logger.info(f"  - Total time: {elapsed:.2f} seconds")
                logger.info(f"  - Operations: {thread_count * iterations}")
                logger.info(f"  - Operations/sec: {(thread_count * iterations) / elapsed:.2f}")
                logger.info(f"  - Average CPU: {avg_cpu:.2f}%")
                logger.info(f"  - Maximum CPU: {max_cpu:.2f}%")
                logger.info(f"  - CPU time used: {cpu_time_used:.2f} seconds")
                logger.info(f"  - CPU efficiency: {(cpu_time_used / elapsed) * 100:.2f}%")
                logger.info(f"  - Final DB size: {db_size / 1024:.2f} KB")
                
                # Clean up test database
                if os.path.exists(db_path):
                    try:
                        conn.close()
                        os.remove(db_path)
                    except:
                        pass
                
                # Also try to clean up any WAL files
                for ext in ['-wal', '-shm']:
                    wal_file = db_path + ext
                    if os.path.exists(wal_file):
                        try:
                            os.remove(wal_file)
                        except:
                            pass
            
            test_sqlite_threading_patterns(args.sqlite_test)
        elif args.ban_author:
            if not args.ban_reason:
                print("Error: --ban-reason is required when banning an author")
                sys.exit(1)
            if ban_author(conn, args.ban_author, args.ban_reason):
                print(f"Successfully banned author: {args.ban_author}")
            else:
                print(f"Failed to ban author: {args.ban_author}")
        elif args.unban_author:
            if unban_author(conn, args.unban_author):
                print(f"Successfully unbanned author: {args.unban_author}")
            else:
                print(f"Failed to unban author: {args.unban_author}")
        elif args.cleanup_banned:
            if cleanup_banned_author_content(conn, args.cleanup_banned):
                print(f"Successfully cleaned up content for banned author: {args.cleanup_banned}")
            else:
                print(f"Failed to clean up content for banned author: {args.cleanup_banned}")
        elif args.redownload_author:
            redownload_author_images(args.redownload_author)
        elif args.test:
            test_album_extraction()
        elif args.test_tags:
            test_tag_extraction()
        elif args.test_camera:
            test_camera_extraction()
        else:
            crawl_images(start_offset=args.start_offset, 
                        retry_mode=args.retry)
    except Exception as e:
        logger.critical(f"Unexpected error occurred: {e}", exc_info=True)
    finally:
        if stop_monitoring:
            stop_monitoring()
        if 'conn' in locals():
            conn.close()
