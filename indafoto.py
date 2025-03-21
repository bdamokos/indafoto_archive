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
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please install the required modules using the requirements.txt file:\n")
    print("pip install -r requirements.txt")
    sys.exit(1)

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
    """
    global should_restart, restart_timer, args, restart_in_progress
    
    # Use the lock to ensure only one thread can attempt restart
    with restart_lock:
        # Check if restart is already in progress
        if restart_in_progress:
            logger.info("Restart already in progress, skipping additional restart attempt")
            return
            
        if not error_restart and not args.auto_restart:
            logger.info("Auto-restart is disabled, continuing without restart")
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
        cursor.execute("""
            SELECT MAX(page_number) 
            FROM completed_pages
        """)
        last_page = cursor.fetchone()[0]
        next_page = (last_page + 1) if last_page is not None else 0
        logger.info(f"Restarting from page {next_page}")
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

def create_session(max_retries=3, pool_connections=10):
    """Create a standardized session with HTTP/2 and multiplexing enabled"""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(COOKIES)
    
    # Enable HTTP/2 with multiplexing
    session.http2 = True
    session.multiplexed = True
    
    # Configure adapter for better performance
    adapter = requests.adapters.HTTPAdapter(
        pool_maxsize=pool_connections,
        pool_block=True,
        max_retries=max_retries
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

def init_db():
    """Initialize the database with required tables."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Set performance optimizing PRAGMAs
    cursor.execute("PRAGMA journal_mode=WAL")  # Use Write-Ahead Logging for better concurrency
    cursor.execute("PRAGMA synchronous=NORMAL")  # Reduce fsync calls while maintaining safety
    cursor.execute("PRAGMA cache_size=-1000000")  # Use 1GB of memory for cache (-ve number means kilobytes)
    cursor.execute("PRAGMA temp_store=MEMORY")  # Store temp tables and indices in memory
    cursor.execute("PRAGMA mmap_size=8589934592")  # Allow up to 8GB memory-mapped I/O (8 * 1024 * 1024 * 1024)
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
                    
                    if image_url.startswith('https://'):
                        image_data.append({
                            'image_url': image_url,
                            'page_url': photo_page_url,
                            'caption': caption
                        })
                        logger.debug(f"Found image: {image_url} from page: {photo_page_url}")
                except Exception as e:
                    logger.error(f"Failed to parse Tumblr share URL: {e}")
                    continue

        logger.info(f"Found {len(image_data)} images with metadata on page {search_page_url}")
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
                
                with tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"Downloading {os.path.basename(filename)}",
                    colour='green'
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

def process_image_list(image_data_list, conn, cursor, sample_rate=1.0):
    """Process a list of images and save them to the database."""
    global consecutive_failures, consecutive_successes, current_wait_time, current_workers
    
    author_image_counts = {}
    processed_count = 0
    failed_count = 0
    skipped_count = 0
    banned_count = 0
    
    # Create thread pools for different operations
    metadata_pool = ThreadPool(max(current_workers*4, 12), "metadata")
    download_pool = ThreadPool(current_workers, "download")
    validation_pool = ThreadPool(current_workers, "validation")
    
    # Create session pool with more sessions for better parallelization
    session_pool = queue.Queue(maxsize=current_workers * 4)
    for _ in range(current_workers * 4):
        session = create_session(max_retries=3, pool_connections=current_workers * 2)
        session_pool.put(session)
    
    def process_metadata(image_data):
        """Process metadata for a single image."""
        session = session_pool.get()
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
            session_pool.put(session)
    
    def process_download(download_url, metadata):
        """Download a single image."""
        session = session_pool.get()
        try:
            result = download_image(download_url, metadata['author'], session=session)
            if result:
                filename, _ = result
                return ('success', (filename, download_url, metadata))
            return ('error', ('download', download_url, "Failed to download image"))
        except Exception as e:
            return ('error', ('download', download_url, str(e)))
        finally:
            session_pool.put(session)
    
    def process_validation(filename, url, metadata):
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
    
    try:
        # First pass: check for duplicates and prepare work queue
        image_ids_to_process = []
        for image_data in image_data_list:
            try:
                url = image_data.get('image_url', '')
                page_url = image_data.get('page_url', '')
                
                if not url or not page_url:
                    logger.warning("Skipping image with no URL or page URL")
                    failed_count += 1
                    continue
                
                # Extract image ID from URL
                image_id = extract_image_id(url)
                if not image_id:
                    logger.warning(f"Could not extract image ID from URL: {url}")
                    failed_count += 1
                    continue
                
                # Check if we already have this image
                cursor.execute("""
                    SELECT i.id, i.local_path, i.author 
                    FROM images i 
                    WHERE i.url LIKE ?
                """, (f"%{image_id}%",))
                existing = cursor.fetchone()
                
                if existing:
                    logger.info(f"Image ID {image_id} already exists, skipping...")
                    skipped_count += 1
                    processed_count += 1
                    
                    # Update author counts
                    author = existing[2]
                    author_image_counts[author] = author_image_counts.get(author, 0) + 1
                    continue
                
                image_ids_to_process.append(image_data)
            except Exception as e:
                logger.error(f"Error checking image: {e}")
                failed_count += 1
                continue
        
        logger.info(f"Found {len(image_ids_to_process)} new images to process")
        
        # Process images through the pipeline using thread pools
        with tqdm(total=len(image_ids_to_process), desc="Processing images", colour='yellow') as pbar:
            # Add metadata tasks
            for image_data in image_ids_to_process:
                metadata_pool.add_task(process_metadata, image_data)
            
            # Process results and chain tasks
            while processed_count + failed_count + banned_count < len(image_ids_to_process):
                # Process metadata results
                try:
                    while True:
                        result = metadata_pool.results.get_nowait()
                        status, data = result
                        
                        if status == 'success':
                            download_url, metadata = data
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
                        status, data = result
                        
                        if status == 'success':
                            filename, url, metadata = data
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
                        status, data = result
                        
                        if status == 'success':
                            filename, file_hash, url, metadata = data
                            # Save to database (this part remains unchanged)
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
                                
                                # Process collections and albums (unchanged code)
                                # ...
                                
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
        
        # Shutdown thread pools
        metadata_pool.shutdown()
        download_pool.shutdown()
        validation_pool.shutdown()
        
        # Return success status and stats
        success = (processed_count + failed_count) == len(image_ids_to_process) and failed_count == 0
        stats = {
            'processed_count': processed_count,
            'failed_count': failed_count,
            'skipped_count': skipped_count,
            'banned_count': banned_count,
            'total_images': len(image_ids_to_process),
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
    
    return {
        'avg_mb_per_page': avg_bytes_per_page / (2**20),  # Convert to MB
        'estimated_remaining_gb': estimated_remaining_bytes / (2**30),  # Convert to GB
        'total_estimated_gb': (downloaded_bytes + estimated_remaining_bytes) / (2**30)
    }

def crawl_images(start_offset=0, retry_mode=False):
    """Main function to crawl images and save metadata."""
    global current_workers, consecutive_failures, consecutive_successes, current_wait_time, last_restart_time
    
    # Start the restart timer
    global restart_timer
    restart_timer = threading.Timer(60, check_restart_timer)
    restart_timer.start()
    
    conn = init_db()
    cursor = conn.cursor()
    
    # Check initial disk space
    free_space_gb = check_disk_space(BASE_DIR)
    logger.info(f"Initial free space: {free_space_gb:.2f}GB")
    
    # Initialize space tracking
    total_downloaded_bytes = 0
    pages_processed = 0
    
    try:
        # If in retry mode, first check for pages with 0 images in completed_pages
        if retry_mode:
            cursor.execute("""
                SELECT page_number, completion_date 
                FROM completed_pages 
                WHERE image_count = 0 
                ORDER BY page_number
            """)
            zero_image_pages = cursor.fetchall()
            
            if zero_image_pages:
                logger.info(f"Found {len(zero_image_pages)} pages with 0 images to retry")
                for page_number, completion_date in zero_image_pages:
                    search_page_url = SEARCH_URL_TEMPLATE.format(page_number)
                    cursor.execute("""
                        INSERT OR REPLACE INTO failed_pages (
                            page_number, url, error, last_attempt, status, attempts
                        ) VALUES (?, ?, ?, ?, 'zero_images', 0)
                    """, (page_number, search_page_url, "Page had 0 images", datetime.now().isoformat()))
                    conn.commit()
                    logger.info(f"Added page {page_number} to retry queue due to 0 images")
        
        for page in tqdm(range(start_offset, TOTAL_PAGES), desc="Crawling pages", colour='blue'):
            # Check if we should restart
            if should_restart:
                logger.info("Restart flag set, initiating restart...")
                restart_script()
            
            try:
                # Check if page was already completed successfully
                cursor.execute("""
                    SELECT completion_date, image_count 
                    FROM completed_pages 
                    WHERE page_number = ?
                """, (page,))
                result = cursor.fetchone()
                
                if result:
                    completion_date, image_count = result
                    if image_count > 0:
                        logger.info(f"Skipping page {page} - already completed successfully with {image_count} images")
                        continue
                    else:
                        logger.info(f"Retrying page {page} - completed but had 0 images")
                
                # Check disk space before each page
                free_space_gb = get_free_space_gb(BASE_DIR)
                if free_space_gb < 2:
                    raise RuntimeError(f"Critical: Only {free_space_gb:.2f}GB space remaining. Stopping crawler.")
                
                search_page_url = SEARCH_URL_TEMPLATE.format(page)
                
                # Track page size before downloading
                page_size_before = sum(f.stat().st_size for f in Path(BASE_DIR).rglob('*') if f.is_file())
                
                try:
                    image_data_list = get_image_links(search_page_url, attempt=1)
                except requests.exceptions.HTTPError as e:
                    if any(str(code) in str(e) for code in [503, 504, 500, 502, 507, 508, 509]):
                        # Handle 50x Server Error with 5-minute cooldown
                        error_code = re.search(r'\((\d+)\)', str(e)).group(1)
                        logger.error(f"Server Error ({error_code}) for page {page}, implementing 5-minute cooldown")
                        
                        # Add to failed_pages with special status
                        cursor.execute("""
                            INSERT OR REPLACE INTO failed_pages (
                                page_number, url, error, last_attempt, status, attempts
                            ) VALUES (?, ?, ?, ?, 'server_error', 0)
                        """, (page, search_page_url, f"Server Error ({error_code})", datetime.now().isoformat()))
                        conn.commit()
                        
                        # Implement 5-minute cooldown
                        cooldown_minutes = 5
                        logger.info(f"Waiting {cooldown_minutes} minutes before continuing...")
                        time.sleep(cooldown_minutes * 60)
                        continue
                    else:
                        # Handle other HTTP errors
                        logger.error(f"HTTP error for page {page}: {e}")
                        cursor.execute("""
                            INSERT OR REPLACE INTO failed_pages (
                                page_number, url, error, last_attempt, status, attempts
                            ) VALUES (?, ?, ?, ?, 'error', 0)
                        """, (page, search_page_url, str(e), datetime.now().isoformat()))
                        conn.commit()
                        continue
                
                # Process the image list
                success, stats = process_image_list(image_data_list, conn, cursor)
                
                # Calculate space used by this page
                page_size_after = sum(f.stat().st_size for f in Path(BASE_DIR).rglob('*') if f.is_file())
                page_downloaded_bytes = page_size_after - page_size_before
                total_downloaded_bytes += page_downloaded_bytes
                
                if success:
                    # Mark page as completed with image count
                    cursor.execute("""
                        INSERT OR REPLACE INTO completed_pages (
                            page_number, completion_date, image_count, total_size_bytes
                        ) VALUES (?, ?, ?, ?)
                    """, (page, datetime.now().isoformat(), stats['processed_count'], page_downloaded_bytes))
                    conn.commit()
                    
                    # Update author stats
                    for author, count in stats['author_counts'].items():
                        cursor.execute("""
                            UPDATE author_stats 
                            SET total_images = total_images + ?,
                                last_updated = ?
                            WHERE author = ?
                        """, (count, datetime.now().isoformat(), author))
                        conn.commit()
                    
                    pages_processed += 1
                    logger.info(f"Successfully completed page {page} - "
                              f"Processed: {stats['processed_count']}, "
                              f"Failed: {stats['failed_count']}, "
                              f"Total: {stats['total_images']}, "
                              f"Workers: {stats['current_workers']}")
                else:
                    # If processing wasn't completely successful, add to failed_pages
                    error_msg = stats.get('error', 'Unknown error')
                    cursor.execute("""
                        INSERT OR REPLACE INTO failed_pages (
                            page_number, url, error, last_attempt, status, attempts
                        ) VALUES (?, ?, ?, ?, 'pending', 0)
                    """, (page, search_page_url, error_msg, datetime.now().isoformat()))
                    conn.commit()
                    logger.warning(f"Failed to process all images on page {page} - "
                                 f"Processed: {stats['processed_count']}, "
                                 f"Failed: {stats['failed_count']}, "
                                 f"Total: {stats['total_images']}, "
                                 f"Workers: {stats['current_workers']}")
                
                # Rate limiting between pages
                time.sleep(BASE_RATE_LIMIT)
                
            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                # Add to failed_pages with error status
                cursor.execute("""
                    INSERT OR REPLACE INTO failed_pages (
                        page_number, url, error, last_attempt, status, attempts
                    ) VALUES (?, ?, ?, ?, 'error', 0)
                """, (page, search_page_url, str(e), datetime.now().isoformat()))
                conn.commit()
                continue
    
    except KeyboardInterrupt:
        logger.info("\nCrawling interrupted by user")
    finally:
        # Print summary
        logger.info("\nCrawling Summary:")
        logger.info(f"Pages processed: {pages_processed}")
        logger.info(f"Total downloaded: {total_downloaded_bytes / (1024*1024*1024):.2f}GB")
        logger.info(f"Final worker count: {current_workers}")
        
        conn.close()

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
    args = parser.parse_args()
    
    try:
        # Check for updates unless explicitly disabled
        if not args.no_update_check:
            check_for_updates()
            
        # Set the number of workers from command line argument
        current_workers = args.workers
        MAX_WORKERS = args.workers
        logger.info(f"Starting crawler with {current_workers} workers")
        
        conn = init_db()
        
        if args.ban_author:
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
        if 'conn' in locals():
            conn.close()
