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
import random
import hashlib

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

# TODO: Add intelligent resume feature, and error handling (e.g. if we find an error that prevents the middle 20% of the pages from being downloaded, we should skip them and continue from the next page, after the run we would fix the error and the script should find and download the missing images)

# Configurations
# If you want to save images with different characteristics, you can change the search URL template and the total number of pages.
SEARCH_URL_TEMPLATE = "https://indafoto.hu/search/list?profile=main&sphinx=1&search=advanced&textsearch=fulltext&textuser=&textmap=&textcompilation=&photo=&datefrom=&dateto=&licence%5B1%5D=I1%3BI2%3BII1%3BII2&licence%5B2%5D=I1%3BI2%3BI3&page_offset={}"
BASE_DIR = "indafoto_archive"
DB_FILE = "indafoto.db"
TOTAL_PAGES = 14267
BASE_RATE_LIMIT = 2  # Base seconds between requests
BASE_TIMEOUT = 60    # Base timeout in seconds
FILES_PER_DIR = 1000  # Maximum number of files per directory
ARCHIVE_SAMPLE_RATE = 0.005  # 0.5% sample rate for Internet Archive submissions

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

# Initialize database
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Create images table with UUID and hash
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        uuid TEXT UNIQUE,
        url TEXT UNIQUE,
        local_path TEXT,
        sha256_hash TEXT,
        title TEXT,
        author TEXT,
        author_url TEXT,
        license TEXT,
        camera_make TEXT,
        focal_length TEXT,
        aperture TEXT,
        shutter_speed TEXT,
        taken_date TEXT,
        page_url TEXT
    )
    """)
    
    # Add sha256_hash column if it doesn't exist (for backwards compatibility)
    try:
        cursor.execute("ALTER TABLE images ADD COLUMN sha256_hash TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
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
    
    # Create archive_submissions table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS archive_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE,
        submission_date TEXT,
        type TEXT,  -- 'author_page', 'image_page', 'gallery_page'
        status TEXT,
        archive_url TEXT
    )
    """)
    
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

def get_image_links(search_page_url, attempt=1):
    """Extract image links from a search result page."""
    try:
        logger.info(f"Attempting to fetch URL: {search_page_url} (attempt {attempt})")
        timeout = BASE_TIMEOUT * attempt  # Progressive timeout
        response = requests.get(
            search_page_url,
            headers=HEADERS,
            cookies=COOKIES,
            timeout=timeout,
            allow_redirects=True
        )
        
        # Log response details
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response URL after redirects: {response.url}")
        
        response.raise_for_status()
        
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
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {search_page_url}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error while processing {search_page_url}: {e}")
        return []

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

def extract_metadata(photo_page_url, attempt=1):
    """Extract metadata from a photo page."""
    try:
        timeout = BASE_TIMEOUT * attempt  # Progressive timeout
        response = requests.get(photo_page_url, headers=HEADERS, cookies=COOKIES, timeout=timeout)
        if not response.ok:
            logger.error(f"Failed to fetch metadata from {photo_page_url}: HTTP {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extract title from h1 tag
        title = soup.find("h1")
        if title:
            title = title.text.strip()
        else:
            title_meta = soup.find("meta", property="og:title")
            title = title_meta["content"] if title_meta else "Unknown"

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

        # Extract tags
        tags = []
        tags_container = soup.find("div", class_="box_data")
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

        # Extract taken date from EXIF
        taken_date = None
        if 'Készült' in exif_data:
            taken_date = parse_hungarian_date(exif_data['Készült'])

        # Find highest resolution image URL
        high_res_url = None
        # First look for direct image URLs
        img_patterns = ['_xxl.jpg', '_xl.jpg', '_l.jpg', '_m.jpg']
        
        # Try to find the image URL from meta tags first
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            base_url = og_image["content"]
            # Replace the resolution suffix to get higher resolution
            for pattern in img_patterns:
                test_url = re.sub(r'_[a-z]+\.jpg$', pattern, base_url)
                try:
                    head_response = requests.head(test_url, headers=HEADERS, timeout=5)
                    if head_response.ok:
                        high_res_url = test_url
                        break
                except:
                    continue
        
        # Fallback to looking for direct links if meta tag approach failed
        if not high_res_url:
            for pattern in img_patterns:
                img_link = soup.find('a', href=lambda x: x and pattern in x)
                if img_link:
                    high_res_url = img_link['href']
                    break

        metadata = {
            'title': title,
            'author': author,
            'author_url': author_url,
            'license': license_info,
            'collections': collections,
            'albums': albums,
            'camera_make': exif_data.get('Gyártó'),
            'focal_length': exif_data.get('Fókusztáv'),
            'aperture': exif_data.get('Rekesz'),
            'shutter_speed': exif_data.get('Zársebesség'),
            'taken_date': taken_date,
            'page_url': photo_page_url,
            'tags': tags
        }

        return metadata
    except Exception as e:
        logger.error(f"Error extracting metadata from {photo_page_url}: {e}")
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

def download_image(image_url, author):
    """Download an image and save locally."""
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
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(save_dir, f"{base_name}_{timestamp}.jpg")
        
        if os.path.exists(filename):
            logger.warning(f"Skipping download, file already exists: {filename}")
            file_hash = calculate_file_hash(filename)
            return filename, file_hash

        response = requests.get(image_url, headers=HEADERS, timeout=10, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        
        # Calculate hash while downloading
        sha256_hash = hashlib.sha256()
        
        with open(filename, "wb") as file, tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            desc=f"Downloading {os.path.basename(filename)}"
        ) as pbar:
            for chunk in response.iter_content(block_size):
                file.write(chunk)
                sha256_hash.update(chunk)
                pbar.update(len(chunk))
        
        file_hash = sha256_hash.hexdigest()
        logger.info(f"Successfully downloaded image to {filename} (SHA-256: {file_hash})")
        return filename, file_hash
    except Exception as e:
        logger.error(f"Failed to download {image_url}: {e}")
        return None, None

class ArchiveSubmitter(threading.Thread):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.daemon = True

    def run(self):
        # Create connection in the thread that will use it
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        while True:
            try:
                url, url_type = self.queue.get()
                if url is None:  # Poison pill
                    break
                    
                # Check if already submitted
                cursor.execute("SELECT 1 FROM archive_submissions WHERE url = ?", (url,))
                if cursor.fetchone():
                    self.queue.task_done()
                    continue

                # Submit to Internet Archive
                try:
                    archive_url = f"https://web.archive.org/save/{url}"
                    response = requests.get(archive_url, timeout=30)
                    status = "success" if response.ok else "failed"
                    
                    cursor.execute("""
                        INSERT INTO archive_submissions (url, submission_date, type, status, archive_url)
                        VALUES (?, ?, ?, ?, ?)
                    """, (url, datetime.now().isoformat(), url_type, status, 
                         f"https://web.archive.org/web/*/{url}" if response.ok else None))
                    conn.commit()
                    
                except Exception as e:
                    logger.error(f"Failed to submit {url} to Internet Archive: {e}")
                    
                self.queue.task_done()
                time.sleep(5)  # Rate limit archive submissions
                
            except Exception as e:
                logger.error(f"Error in archive submitter: {e}")
                continue
                
        # Close connection when thread exits
        conn.close()

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

def process_image_list(image_data_list, conn, cursor, archive_queue=None, attempt=1):
    """Process a list of images, returns True if all images were processed successfully."""
    if not image_data_list:
        return True
        
    success = True
    # Track authors and their image counts for archive sampling
    author_image_counts = {}
    
    for image_data in image_data_list:
        try:
            image_url = image_data['image_url']
            photo_page_url = image_data['page_url']
            
            # Generate UUID for the image using the actual image ID
            image_uuid = f"indafoto_{extract_image_id(photo_page_url)}"
            
            # Check if already processed
            cursor.execute("SELECT 1 FROM images WHERE uuid=?", (image_uuid,))
            if cursor.fetchone():
                continue

            # Pass attempt number to extract_metadata for progressive timeout
            metadata = extract_metadata(photo_page_url, attempt=attempt)
            if not metadata:
                success = False
                continue

            # Try to get high-res version of the image
            download_url = image_url
            if '_l.jpg' in image_url:
                for res in ['_xxl.jpg', '_xl.jpg']:
                    test_url = image_url.replace('_l.jpg', res)
                    try:
                        head_response = requests.head(test_url, headers=HEADERS, timeout=5)
                        if head_response.ok:
                            download_url = test_url
                            break
                    except:
                        continue

            # Download image
            download_result = download_image(download_url, metadata['author'])
            if not download_result[0]:  # If filename is None
                success = False
                continue
            
            local_path, file_hash = download_result

            # Insert image data
            cursor.execute("""
                INSERT INTO images (
                    uuid, url, local_path, sha256_hash, title, author, author_url,
                    license, camera_make, focal_length, aperture,
                    shutter_speed, taken_date, page_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                image_uuid, download_url, local_path, file_hash, metadata['title'],
                metadata['author'], metadata['author_url'], metadata['license'],
                metadata['camera_make'], metadata['focal_length'],
                metadata['aperture'], metadata['shutter_speed'],
                metadata['taken_date'], photo_page_url
            ))

            # Process galleries
            for gallery in metadata['collections']:
                # Insert or update gallery
                cursor.execute("""
                    INSERT OR IGNORE INTO collections (collection_id, title, url, is_public)
                    VALUES (?, ?, ?, ?)
                """, (gallery['id'], gallery['title'], gallery['url'], gallery['is_public']))
                
                # Get gallery database ID
                cursor.execute("SELECT id FROM collections WHERE collection_id = ?", (gallery['id'],))
                gallery_db_id = cursor.fetchone()[0]
                
                # Link image to gallery
                cursor.execute("""
                    INSERT INTO image_collections (image_id, collection_id)
                    VALUES ((SELECT id FROM images WHERE uuid = ?), ?)
                """, (image_uuid, gallery_db_id))

            for gallery in metadata['albums']:
                # Insert or update gallery
                cursor.execute("""
                    INSERT OR IGNORE INTO albums (album_id, title, url, is_public)
                    VALUES (?, ?, ?, ?)
                """, (gallery['id'], gallery['title'], gallery['url'], gallery['is_public']))
                
                # Get gallery database ID
                cursor.execute("SELECT id FROM albums WHERE album_id = ?", (gallery['id'],))
                gallery_db_id = cursor.fetchone()[0]
                
                # Link image to gallery
                cursor.execute("""
                    INSERT INTO image_albums (image_id, album_id)
                    VALUES ((SELECT id FROM images WHERE uuid = ?), ?)
                """, (image_uuid, gallery_db_id))

            # Process tags
            for tag in metadata.get('tags', []):
                # Insert or update tag
                cursor.execute("""
                    INSERT OR IGNORE INTO tags (name, count)
                    VALUES (?, ?)
                """, (tag['name'], tag['count']))
                
                # Get tag database ID
                cursor.execute("SELECT id FROM tags WHERE name = ?", (tag['name'],))
                tag_db_id = cursor.fetchone()[0]
                
                # Link image to tag
                cursor.execute("""
                    INSERT INTO image_tags (image_id, tag_id)
                    VALUES ((SELECT id FROM images WHERE uuid = ?), ?)
                """, (image_uuid, tag_db_id))

            conn.commit()

            # Handle Internet Archive submissions if enabled
            if archive_queue:
                author = metadata['author']
                author_image_counts[author] = author_image_counts.get(author, 0) + 1
                
                # Submit author page if this is their first image
                if author_image_counts[author] == 1 and metadata['author_url']:
                    archive_queue.put((metadata['author_url'], 'author_page'))
                
                # Submit image page based on sampling rate
                if author_image_counts[author] == 1 or random.random() < ARCHIVE_SAMPLE_RATE:
                    archive_queue.put((photo_page_url, 'image_page'))

        except Exception as e:
            logger.error(f"Error processing image {image_url}: {e}")
            success = False
            continue
            
    return success

def crawl_images(start_offset=0, enable_archive=False, retry_mode=False):
    """Main function to crawl images and save metadata."""
    conn = init_db()
    cursor = conn.cursor()
    
    # Handle retry mode
    if retry_mode:
        retry_failed_pages(conn, cursor)
        conn.close()
        return
    
    # Initialize archive submission queue and worker if enabled
    archive_queue = None
    archive_worker = None
    if enable_archive:
        archive_queue = queue.Queue()
        archive_worker = ArchiveSubmitter(archive_queue)
        archive_worker.start()
    
    try:
        for page in tqdm(range(start_offset, TOTAL_PAGES), desc="Crawling pages"):
            try:
                search_page_url = SEARCH_URL_TEMPLATE.format(page)
                
                # Skip if we've already successfully processed this page
                cursor.execute("""
                    SELECT status FROM failed_pages 
                    WHERE page_number = ? AND status = 'success'
                """, (page,))
                if cursor.fetchone():
                    continue
                
                image_data_list = get_image_links(search_page_url, attempt=1)
                
                if not process_image_list(image_data_list, conn, cursor, archive_queue, attempt=1):
                    # If processing wasn't completely successful, add to failed_pages
                    cursor.execute("""
                        INSERT OR REPLACE INTO failed_pages (
                            page_number, url, last_attempt, status
                        ) VALUES (?, ?, ?, 'pending')
                    """, (page, search_page_url, datetime.now().isoformat()))
                    conn.commit()
                
                time.sleep(BASE_RATE_LIMIT)

            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                # Record the failed page
                cursor.execute("""
                    INSERT OR REPLACE INTO failed_pages (
                        page_number, url, error, last_attempt, status
                    ) VALUES (?, ?, ?, ?, 'pending')
                """, (page, search_page_url, str(e), datetime.now().isoformat()))
                conn.commit()
                continue

    except KeyboardInterrupt:
        logger.info("Crawler interrupted by user")
    except Exception as e:
        logger.critical(f"Unexpected error occurred: {e}", exc_info=True)
    finally:
        # Stop archive worker if it was started
        if enable_archive and archive_worker:
            archive_queue.put((None, None))
            archive_worker.join()
        
        conn.close()
        logger.info("Crawler finished, database connection closed")

def test_album_extraction():
    """Test function to verify album extraction from a specific page."""
    url = "https://indafoto.hu/vasvarvar/image/27344784-e761530c/815586"
    metadata = extract_metadata(url)
    if metadata:
        print("\nCollections:")
        for collection in metadata['collections']:
            print(f"- {collection['title']} (ID: {collection['id']})")
        print("\nAlbums:")
        for album in metadata['albums']:
            print(f"- {album['title']} (ID: {album['id']})")
    else:
        print("Failed to extract metadata")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indafoto Crawler')
    parser.add_argument('--start-offset', type=int, default=0,
                       help='Page offset to start crawling from')
    parser.add_argument('--enable-archive', action='store_true',
                       help='Enable Internet Archive submissions (disabled by default)')
    parser.add_argument('--retry', action='store_true',
                       help='Retry failed pages instead of normal crawling')
    parser.add_argument('--test', action='store_true',
                       help='Run test function instead of crawler')
    args = parser.parse_args()
    
    try:
        if args.test:
            test_album_extraction()
        else:
            crawl_images(start_offset=args.start_offset, 
                        enable_archive=args.enable_archive,
                        retry_mode=args.retry)
    except Exception as e:
        logger.critical(f"Unexpected error occurred: {e}", exc_info=True)
