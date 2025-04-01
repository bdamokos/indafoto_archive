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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('author_details_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DB_FILE = "indafoto.db"
CHECK_INTERVAL =  60  # 1 minutes in seconds
MAX_RETRIES = 3

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
    """Initialize the database with the author_details table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create author_details table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS author_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        author TEXT UNIQUE,
        author_slug TEXT,  -- Added field for the URL slug
        bio TEXT,
        website TEXT,
        registration_date TEXT,
        image_count INTEGER,
        album_count INTEGER,
        tag_cloud TEXT,  -- Store as JSON string
        last_updated TEXT,
        details_url TEXT,
        error TEXT,
        retry_count INTEGER DEFAULT 0
    )
    """)
    
    # Add author_slug column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE author_details ADD COLUMN author_slug TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    conn.commit()
    return conn

def parse_hungarian_date(date_str):
    """Parse a date string in YYYY.MM.DD. format."""
    if not date_str:
        return None
    
    try:
        # Remove any leading/trailing whitespace and any extra dots at the end
        date_str = date_str.strip().rstrip('.')
        
        # Split into parts
        parts = date_str.split('.')
        if len(parts) >= 3:
            year = parts[0].strip()
            month = parts[1].strip()
            day = parts[2].strip()
            
            # Validate parts
            if (len(year) == 4 and year.isdigit() and
                len(month) <= 2 and month.isdigit() and 1 <= int(month) <= 12 and
                len(day) <= 2 and day.isdigit() and 1 <= int(day) <= 31):
                
                # Format date as YYYY-MM-DD
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            
    except Exception as e:
        logger.warning(f"Failed to parse date '{date_str}': {e}")
    return None

def extract_author_details(url, session=None):
    """Extract details from an author's details page."""
    if not session:
        session = create_session()
    
    try:
        logger.debug(f"Fetching URL: {url}")
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        # Extract author slug from URL
        author_slug = url.rstrip('/').split('/')[-2]  # Get the second-to-last part of the URL
        logger.debug(f"Extracted author slug: {author_slug}")
        
        # Log response details for debugging
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response length: {len(response.text)}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # First verify we have the user-properties table
        user_props_table = soup.find('table', class_='user-properties')
        if not user_props_table:
            logger.error(f"Could not find user-properties table for {url}")
            # Save the HTML for debugging
            with open(f"debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            return None
            
        # Helper function to safely find text in th element
        def find_row_by_header(table, header_text):
            for row in table.find_all('tr'):
                th = row.find('th')
                if th and th.text and header_text in th.text:
                    return row
            return None
            
        # Extract bio/introduction from the user-properties table
        bio = None
        bio_row = find_row_by_header(user_props_table, 'Bemutatkozás')
        if bio_row:
            bio_cell = bio_row.find('td')
            if bio_cell:
                bio = bio_cell.get_text(strip=True)
                logger.debug(f"Found bio: {bio}")
        
        # Extract website from the user-properties table
        website = None
        website_row = find_row_by_header(user_props_table, 'Weboldal')
        if website_row:
            website_cell = website_row.find('td')
            if website_cell:
                website_link = website_cell.find('a')
                if website_link:
                    website = website_link.get('href', '')
                    logger.debug(f"Found website: {website}")
        
        # Extract registration date from the user-properties table
        reg_date = None
        reg_row = find_row_by_header(user_props_table, 'Regisztrált')
        if reg_row:
            reg_cell = reg_row.find('td')
            if reg_cell:
                reg_text = reg_cell.get_text(strip=True)
                reg_date = parse_hungarian_date(reg_text)
                logger.debug(f"Found registration date: {reg_date} (from {reg_text})")
        
        # Extract image count from the user-properties table
        image_count = None
        images_row = find_row_by_header(user_props_table, 'Képei')
        if images_row:
            images_cell = images_row.find('td')
            if images_cell:
                images_link = images_cell.find('a')
                if images_link:
                    count_text = images_link.get_text(strip=True)
                    try:
                        # Extract number from format like "85976 db"
                        image_count = int(count_text.replace('db', '').strip())
                        logger.debug(f"Found image count: {image_count}")
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Could not parse image count from '{count_text}': {e}")
        
        # Extract album count from the user-properties table
        album_count = None
        albums_row = find_row_by_header(user_props_table, 'Albumai')
        if albums_row:
            albums_cell = albums_row.find('td')
            if albums_cell:
                albums_link = albums_cell.find('a')
                if albums_link:
                    count_text = albums_link.get_text(strip=True)
                    try:
                        # Extract number from format like "725 db"
                        album_count = int(count_text.replace('db', '').strip())
                        logger.debug(f"Found album count: {album_count}")
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Could not parse album count from '{count_text}': {e}")
        
        # Extract tag cloud from the tag-row div
        tag_cloud = []
        tag_row = soup.find('div', class_='tag-row')
        if tag_row:
            tag_box = tag_row.find('div', class_='tags')
            if tag_box:
                content_div = tag_box.find('div', class_='content')
                if content_div:
                    for tag in content_div.find_all('a', class_=lambda x: x and x.startswith('tag-')):
                        tag_name = tag.get_text(strip=True)
                        # Skip empty tags
                        if not tag_name:
                            continue
                            
                        tag_url = tag.get('href', '')
                        # Extract tag weight from class (e.g., 'tag-3' means weight 3)
                        tag_weight = None
                        for class_name in tag.get('class', []):
                            if class_name.startswith('tag-'):
                                try:
                                    tag_weight = int(class_name.replace('tag-', ''))
                                    break
                                except ValueError:
                                    pass
                        
                        tag_cloud.append({
                            'name': tag_name,
                            'url': tag_url,
                            'weight': tag_weight
                        })
                    logger.debug(f"Found {len(tag_cloud)} tags")
        
        result = {
            'bio': bio,
            'website': website,
            'registration_date': reg_date,
            'image_count': image_count,
            'album_count': album_count,
            'tag_cloud': tag_cloud,
            'author_slug': author_slug  # Add author_slug to the result
        }
        
        # Log the extracted data
        logger.info(f"Successfully extracted details for {url}: {result}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error extracting details from {url}: {e}", exc_info=True)
        # Save the HTML for debugging on any error
        try:
            with open(f"error_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
                f.write(response.text)
        except:
            pass
        return None

def process_author(conn, author, author_url):
    """Process a single author's details."""
    cursor = conn.cursor()
    
    try:
        # Construct details URL - fix the URL construction
        if not author_url:
            logger.warning(f"No author URL provided for {author}, skipping")
            return False
            
        # Remove any trailing slash
        author_url = author_url.rstrip('/')
        
        # Append /details to the author's base URL
        details_url = f"{author_url}/details"
        
        # Extract details
        details = extract_author_details(details_url)
        
        if details:
            # Convert tag cloud to string for storage
            import json
            tag_cloud_str = json.dumps(details['tag_cloud'])
            
            # Update or insert author details
            cursor.execute("""
                INSERT INTO author_details 
                (author, author_slug, bio, website, registration_date, image_count, album_count, 
                 tag_cloud, last_updated, details_url, error, retry_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0)
                ON CONFLICT(author) DO UPDATE SET
                    author_slug = excluded.author_slug,
                    bio = excluded.bio,
                    website = excluded.website,
                    registration_date = excluded.registration_date,
                    image_count = excluded.image_count,
                    album_count = excluded.album_count,
                    tag_cloud = excluded.tag_cloud,
                    last_updated = excluded.last_updated,
                    details_url = excluded.details_url,
                    error = NULL,
                    retry_count = 0
            """, (
                author,
                details['author_slug'],
                details['bio'],
                details['website'],
                details['registration_date'],
                details['image_count'],
                details['album_count'],
                tag_cloud_str,
                datetime.now().isoformat(),
                details_url
            ))
            
            conn.commit()
            logger.info(f"Successfully processed author: {author}")
            return True
            
        else:
            # Update error count
            cursor.execute("""
                INSERT INTO author_details 
                (author, author_slug, last_updated, details_url, error, retry_count)
                VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(author) DO UPDATE SET
                    author_slug = excluded.author_slug,
                    last_updated = excluded.last_updated,
                    error = excluded.error,
                    retry_count = author_details.retry_count + 1
            """, (
                author,
                details['author_slug'] if details else None,
                datetime.now().isoformat(),
                details_url,
                "Failed to extract details"
            ))
            
            conn.commit()
            return False
            
    except Exception as e:
        logger.error(f"Error processing author {author}: {e}")
        
        # Update error in database
        cursor.execute("""
            INSERT INTO author_details 
            (author, author_slug, last_updated, details_url, error, retry_count)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(author) DO UPDATE SET
                author_slug = excluded.author_slug,
                last_updated = excluded.last_updated,
                error = ?,
                retry_count = author_details.retry_count + 1
        """, (
            author,
            details['author_slug'] if details else None,
            datetime.now().isoformat(),
            details_url,
            str(e),
            str(e)
        ))
        
        conn.commit()
        return False

def get_authors_to_process(conn):
    """Get list of authors that need processing."""
    cursor = conn.cursor()
    
    # Get authors that:
    # 1. Have never been processed
    # 2. Have failed but haven't exceeded retry limit
    # 3. Have errors in their data
    cursor.execute("""
        WITH author_status AS (
            SELECT 
                i.author,
                i.author_url,
                ad.retry_count,
                ad.last_updated,
                ad.error
            FROM images i
            LEFT JOIN author_details ad ON i.author = ad.author
            WHERE i.author_url IS NOT NULL  -- Only include authors with valid URLs
            UNION
            SELECT 
                ac.author,
                ac.author_url,
                ad.retry_count,
                ad.last_updated,
                ad.error
            FROM author_crawl ac
            LEFT JOIN author_details ad ON ac.author = ad.author
            WHERE ac.author_url IS NOT NULL  -- Only include authors with valid URLs
        )
        SELECT DISTINCT author, author_url
        FROM author_status
        WHERE retry_count IS NULL  -- Never processed
           OR (error IS NOT NULL AND retry_count < ?)  -- Failed but can retry
           OR error IS NOT NULL  -- Have errors in their data
        ORDER BY COALESCE(retry_count, 0) ASC, COALESCE(last_updated, '1970-01-01') ASC
        LIMIT 100  -- Process in batches
    """, (MAX_RETRIES,))
    
    return cursor.fetchall()

def scraper_loop():
    """Main scraper loop that runs continuously."""
    while True:
        try:
            conn = init_db()
            
            # Get authors to process
            authors = get_authors_to_process(conn)
            
            if authors:
                logger.info(f"Found {len(authors)} authors to process")
                
                # Process each author
                for author, author_url in authors:
                    if not author_url:
                        continue
                        
                    process_author(conn, author, author_url)
                    time.sleep(0.5)  # Small delay between requests
                    
            else:
                logger.info("No authors to process at this time")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error in scraper loop: {e}")
            
        # Wait before next check
        time.sleep(CHECK_INTERVAL)

def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Starting author details scraper...")
    
    try:
        # Initialize database
        init_db()
        
        # Start the scraper loop
        scraper_loop()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1) 