import sqlite3
import logging
import time
import niquests as requests
from bs4 import BeautifulSoup
from datetime import datetime
import threading
import queue
import signal
import sys
from urllib.parse import urljoin
import re
import argparse

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('author_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_FILE = "indafoto.db"
BASE_TIMEOUT = 30
MAX_RETRIES = 3
BASE_RATE_LIMIT = 1  # Base delay between requests in seconds
MAX_WORKERS = 10  # Maximum number of concurrent workers
START_PAGE = 7427
END_PAGE = 14267

# Headers and cookies required for authentication
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

def create_session():
    """Create a session with authentication."""
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(COOKIES)
    return session

def init_db():
    """Initialize the database with the author_crawl table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create author_crawl table to track crawled authors
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS author_crawl (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        author TEXT UNIQUE,
        author_slug TEXT,
        author_url TEXT,
        page_number INTEGER,
        found_date TEXT,
        processed BOOLEAN DEFAULT 0,
        error TEXT,
        retry_count INTEGER DEFAULT 0
    )
    """)
    
    conn.commit()
    return conn

def get_search_url(page_number):
    """Return the search URL for a given page number."""
    return f"https://indafoto.hu/search/list?profile=main&sphinx=1&search=advanced&textsearch=fulltext&textuser=&textmap=&textcompilation=&photo=&datefrom=&dateto=&licence%5B1%5D=I1%3BI2%3BII1%3BII2&licence%5B2%5D=I1%3BI2%3BI3&page_offset={page_number}"

def extract_authors_from_page(url, session=None):
    """Extract author information from a search result page."""
    if not session:
        session = create_session()
    
    try:
        logger.debug(f"Fetching URL: {url}")
        response = session.get(url, timeout=BASE_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        authors = []
        
        # Find all author links with class 'pic_author'
        author_links = soup.find_all('a', class_='pic_author')
        
        for link in author_links:
            href = link.get('href', '')
            author_name = link.get_text(strip=True)
            
            if href and author_name:
                # Extract author slug from URL
                author_slug = href.rstrip('/').split('/')[-1]
                authors.append({
                    'author': author_name,
                    'author_slug': author_slug,
                    'author_url': href
                })
        
        logger.info(f"Found {len(authors)} authors on page {url}")
        return authors
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error extracting authors from {url}: {e}", exc_info=True)
        return None

def process_page(page_number, page_url, db_file):
    """Process a single search page and extract author information."""
    # Create a new connection for this thread
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    try:
        # Create a session for this page
        session = create_session()
        
        # Extract authors from the page
        authors = extract_authors_from_page(page_url, session)
        
        if authors:
            # Insert or update authors in the database
            for author_info in authors:
                cursor.execute("""
                    INSERT INTO author_crawl 
                    (author, author_slug, author_url, page_number, found_date, processed, error, retry_count)
                    VALUES (?, ?, ?, ?, ?, 0, NULL, 0)
                    ON CONFLICT(author) DO UPDATE SET
                        author_slug = excluded.author_slug,
                        author_url = excluded.author_url,
                        page_number = excluded.page_number,
                        found_date = excluded.found_date,
                        processed = 0,
                        error = NULL,
                        retry_count = 0
                """, (
                    author_info['author'],
                    author_info['author_slug'],
                    author_info['author_url'],
                    page_number,
                    datetime.now().isoformat()
                ))
            
            conn.commit()
            logger.info(f"Successfully processed page {page_number} with {len(authors)} authors")
            return True
            
        else:
            logger.warning(f"No authors found on page {page_number}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing page {page_number}: {e}")
        
        # Update error in database
        cursor.execute("""
            INSERT INTO author_crawl 
            (page_number, found_date, error, retry_count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(page_number) DO UPDATE SET
                found_date = excluded.found_date,
                error = ?,
                retry_count = author_crawl.retry_count + 1
        """, (
            page_number,
            datetime.now().isoformat(),
            str(e),
            str(e)
        ))
        
        conn.commit()
        return False
    finally:
        conn.close()

def get_pages_to_process(db_file):
    """Get list of pages that need processing."""
    # Create a new connection for this thread
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    try:
        # Get pages that:
        # 1. Have never been processed
        # 2. Have failed but haven't exceeded retry limit
        cursor.execute("""
            SELECT DISTINCT page_number
            FROM author_crawl
            WHERE processed = 0
               OR (error IS NOT NULL AND retry_count < ?)
            ORDER BY page_number
            LIMIT 100  -- Process in batches
        """, (MAX_RETRIES,))
        
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

def crawler_loop(start_page=START_PAGE, end_page=END_PAGE):
    """Main crawler loop that processes pages in parallel."""
    # Create a queue for pages to process
    page_queue = queue.Queue()
    
    # Create a queue for results
    result_queue = queue.Queue()
    
    # Create a list to track active threads
    active_threads = []
    
    def worker():
        """Worker thread function to process pages."""
        while True:
            try:
                # Get a page number from the queue
                page_number = page_queue.get_nowait()
                page_url = get_search_url(page_number)
                
                # Process the page
                success = process_page(page_number, page_url, DB_FILE)
                
                # Report result
                result_queue.put((page_number, success))
                
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Worker thread error: {e}")
                result_queue.put((page_number, False))
            finally:
                try:
                    page_queue.task_done()
                except ValueError:
                    # Ignore task_done() errors - they happen when the queue is empty
                    pass
    
    while True:
        try:
            # Get pages to process
            pages = get_pages_to_process(DB_FILE)
            
            if not pages:
                # If no pages to process, check if we've reached the end
                if start_page >= end_page:
                    logger.info("Reached end page, stopping crawler")
                    break
                # Add next batch of pages
                for page in range(start_page, min(start_page + 100, end_page + 1)):
                    page_queue.put(page)
                start_page += 100
            
            # Start worker threads
            while len(active_threads) < MAX_WORKERS and not page_queue.empty():
                thread = threading.Thread(target=worker)
                thread.daemon = True
                thread.start()
                active_threads.append(thread)
            
            # Wait for results
            while active_threads:
                # Check for completed threads
                active_threads = [t for t in active_threads if t.is_alive()]
                
                # Process any results
                try:
                    page_number, success = result_queue.get_nowait()
                    if success:
                        logger.info(f"Successfully processed page {page_number}")
                    else:
                        logger.warning(f"Failed to process page {page_number}")
                except queue.Empty:
                    time.sleep(0.1)  # Small delay to prevent CPU spinning
            
            # Small delay between batches
            time.sleep(BASE_RATE_LIMIT)
            
        except Exception as e:
            logger.error(f"Error in crawler loop: {e}")
            time.sleep(BASE_RATE_LIMIT * 2)  # Longer delay on error

def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Crawl indafoto.hu for author information')
    parser.add_argument('--start-page', type=int, required=True, help='Starting page number to crawl')
    parser.add_argument('--end-page', type=int, required=True, help='Ending page number to crawl')
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info(f"Starting author crawler from page {args.start_page} to {args.end_page}...")
    
    try:
        # Initialize database
        init_db()
        
        # Start the crawler loop with command line arguments
        crawler_loop(start_page=args.start_page, end_page=args.end_page)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1) 