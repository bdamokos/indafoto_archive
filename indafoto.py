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
import portalocker  # Replace fcntl with portalocker
import json

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
NUM_WORKERS = 8  # Number of parallel download workers

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
        url TEXT,
        local_path TEXT,
        sha256_hash TEXT,
        title TEXT,
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
        
        # Download to a temporary file first
        temp_filename = filename + '.tmp'
        
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
                
                response = requests.get(image_url, headers=HEADERS, timeout=60, stream=True)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                if total_size == 0:
                    raise ValueError("Server reported content length of 0")
                    
                block_size = 1024
                downloaded_size = 0
                
                # Calculate hash while downloading
                sha256_hash = hashlib.sha256()
                
                with open(temp_filename, "wb") as file, tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"Downloading {os.path.basename(filename)}",
                    colour='green'
                ) as pbar:
                    for chunk in response.iter_content(block_size):
                        if not chunk:  # Filter out keep-alive chunks
                            continue
                        file.write(chunk)
                        sha256_hash.update(chunk)
                        downloaded_size += len(chunk)
                        pbar.update(len(chunk))
                
                # Verify the download
                if downloaded_size != total_size:
                    logger.error(f"Download size mismatch for {image_url}. Expected {total_size}, got {downloaded_size}")
                    os.remove(temp_filename)
                    return None, None
                    
                # Verify it's a valid JPEG
                try:
                    from PIL import Image
                    with Image.open(temp_filename) as img:
                        if img.format != 'JPEG':
                            raise ValueError(f"Downloaded file is not a JPEG (format: {img.format})")
                        # Force load the image to verify it's not corrupted
                        img.load()
                except Exception as e:
                    logger.error(f"Invalid image file downloaded from {image_url}: {e}")
                    os.remove(temp_filename)
                    return None, None
                    
                # If all validation passes, move the temp file to final location
                os.rename(temp_filename, filename)
                
                file_hash = sha256_hash.hexdigest()
                logger.info(f"Successfully downloaded and validated image to {filename} (SHA-256: {file_hash})")
                return filename, file_hash
                
        finally:
            # Clean up the lock file
            try:
                os.remove(lock_filename)
            except OSError:
                pass
            
            # Clean up temp file if it still exists
            try:
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
            except OSError:
                pass
            
    except FileExistsError:
        # Another thread is already downloading this file
        logger.warning(f"Another thread is downloading {filename}, skipping")
        return None, None
    except Exception as e:
        logger.error(f"Failed to download {image_url}: {e}")
        return None, None

class ArchiveSubmitter(threading.Thread):
    def __init__(self, queue, service_name):
        super().__init__()
        self.queue = queue
        self.service_name = service_name
        self.daemon = True
        self.running = True
        self.archive_queue_file = f"archive_queue_{service_name}.json"
        
    def check_archive_org(self, url):
        """Check if URL is already archived on archive.org."""
        try:
            check_url = f"https://web.archive.org/cdx/search/cdx?url={url}&output=json"
            response = requests.get(check_url, timeout=60)
            if response.ok:
                data = response.json()
                if len(data) > 1:  # First row is header
                    latest_snapshot = data[-1]
                    timestamp = latest_snapshot[1]
                    snapshot_date = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
                    cutoff_date = datetime(2024, 7, 1)
                    return snapshot_date >= cutoff_date
            return False
        except Exception as e:
            logger.error(f"Failed to check archive.org for {url}: {e}")
            return False

    def check_archive_ph(self, url):
        """Check if URL is already archived on archive.ph."""
        try:
            timemap_url = f"https://archive.ph/timemap/{url}"
            response = requests.get(timemap_url, timeout=60)
            if response.ok:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    latest_archive = lines[-1]
                    datetime_match = re.search(r'datetime="([^"]+)"', latest_archive)
                    if datetime_match:
                        archive_date = datetime.strptime(datetime_match.group(1), '%a, %d %b %Y %H:%M:%S GMT')
                        cutoff_date = datetime(2024, 7, 1)
                        return archive_date >= cutoff_date
            return False
        except Exception as e:
            logger.error(f"Failed to check archive.ph for {url}: {e}")
            return False

    def submit_to_archive_org(self, url):
        """Submit URL to archive.org."""
        try:
            if self.check_archive_org(url):
                return {'success': True, 'archive_url': f"https://web.archive.org/web/*/{url}"}
            
            archive_url = f"https://web.archive.org/save/{url}"
            response = requests.get(archive_url, timeout=60)
            return {
                'success': response.ok,
                'archive_url': f"https://web.archive.org/web/*/{url}" if response.ok else None
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def submit_to_archive_ph(self, url):
        """Submit URL to archive.ph."""
        try:
            if self.check_archive_ph(url):
                return {'success': True, 'archive_url': f"https://archive.ph/{url}"}
            
            data = {'url': url}
            response = requests.post('https://archive.ph/submit/', data=data, headers=HEADERS, timeout=60)
            
            if response.ok:
                time.sleep(5)
                if self.check_archive_ph(url):
                    return {'success': True, 'archive_url': f"https://archive.ph/{url}"}
            
            return {'success': False, 'error': 'Submission verification failed'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def save_queue(self):
        """Save pending submissions to file."""
        try:
            # Create a temporary list to store items
            queue_list = []
            temp_queue = queue.Queue()
            
            # Get all items from the queue
            while True:
                try:
                    item = self.queue.get_nowait()
                    if item is not None:
                        queue_list.append(item)
                        temp_queue.put(item)
                except queue.Empty:
                    break
            
            # Save to file if we have items
            if queue_list:
                with open(self.archive_queue_file, 'w') as f:
                    json.dump(queue_list, f)
                logger.info(f"Saved {len(queue_list)} items to {self.archive_queue_file}")
            
            # Put items back in the original queue
            while True:
                try:
                    item = temp_queue.get_nowait()
                    self.queue.put(item)
                except queue.Empty:
                    break
                
        except Exception as e:
            logger.error(f"Failed to save archive queue to {self.archive_queue_file}: {str(e)}")

    def load_queue(self):
        """Load pending submissions from file."""
        try:
            if os.path.exists(self.archive_queue_file):
                with open(self.archive_queue_file, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        self.queue.put(item)
                    logger.info(f"Loaded {len(data)} items from {self.archive_queue_file}")
        except Exception as e:
            logger.error(f"Failed to load archive queue: {e}")

    def run(self):
        """Main thread loop for processing archive submissions."""
        conn = None
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            
            self.load_queue()  # Load pending submissions
            
            while self.running:
                try:
                    item = self.queue.get(timeout=1)
                    if item is None:  # Poison pill
                        self.queue.task_done()
                        break
                    
                    url, url_type = item
                    if url is None:  # Skip invalid items
                        self.queue.task_done()
                        continue
                    
                    # Check if already submitted successfully
                    cursor.execute("""
                        SELECT id, archive_url, retry_count 
                        FROM archive_submissions 
                        WHERE url = ?
                    """, (url,))
                    result = cursor.fetchone()
                    
                    current_retry_count = result[2] if result and len(result) > 2 else 0
                    
                    # Submit to appropriate archive service
                    if self.service_name == 'archive.org':
                        submission_result = self.submit_to_archive_org(url)
                    else:  # archive.ph
                        submission_result = self.submit_to_archive_ph(url)
                    
                    if submission_result['success']:
                        status = 'success'
                        archive_url = submission_result['archive_url']
                        error = None
                    else:
                        status = 'failed'
                        archive_url = None
                        error = submission_result.get('error')
                        
                        # If failed, increment retry count and requeue if under limit
                        current_retry_count += 1
                        if current_retry_count < 3:  # Maximum 3 retries
                            logger.info(f"Requeuing {url} for retry {current_retry_count}/3")
                            self.queue.put((url, url_type))
                    
                    # Update database
                    try:
                        if result:
                            cursor.execute("""
                                UPDATE archive_submissions 
                                SET status = ?, archive_url = ?, error = ?,
                                    retry_count = ?, last_attempt = ?
                                WHERE url = ?
                            """, (
                                status,
                                archive_url,
                                error,
                                current_retry_count,
                                datetime.now().isoformat(),
                                url
                            ))
                        else:
                            cursor.execute("""
                                INSERT INTO archive_submissions (
                                    url, submission_date, type, status, archive_url,
                                    error, retry_count, last_attempt
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                url,
                                datetime.now().isoformat(),
                                url_type,
                                status,
                                archive_url,
                                error,
                                current_retry_count,
                                datetime.now().isoformat()
                            ))
                        conn.commit()
                    except sqlite3.Error as e:
                        logger.error(f"Database error while updating archive submission for {url}: {str(e)}")
                        conn.rollback()
                    
                    if status == 'success':
                        logger.info(f"Successfully archived {url} to {archive_url}")
                    else:
                        logger.warning(f"Failed to archive {url}. Error: {error}")
                    
                    self.queue.task_done()
                    self.save_queue()  # Save queue state after each operation
                    time.sleep(5)  # Rate limiting
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Error processing URL in archive submitter: {str(e)}", exc_info=True)
                    continue
                    
        except Exception as e:
            logger.error(f"Critical error in archive submitter thread: {str(e)}", exc_info=True)
        finally:
            if conn:
                conn.close()

    def stop(self):
        """Stop the archive submitter thread."""
        self.running = False
        self.save_queue()
        self.queue.put(None)

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

def process_image_list(image_data_list, conn, cursor, sample_rate=1.0):
    """Process a list of images using a pipeline pattern for metadata extraction and downloading."""
    author_image_counts = {}
    processed_count = 0
    failed_count = 0
    skipped_count = 0
    
    # Initialize queues
    metadata_queue = queue.Queue(maxsize=NUM_WORKERS * 2)  # Buffer for pending metadata extractions
    download_queue = queue.Queue(maxsize=NUM_WORKERS)      # Buffer for pending downloads
    results_queue = queue.Queue()                         # Buffer for completed downloads
    error_queue = queue.Queue()                          # Buffer for errors
    
    def metadata_worker():
        """Worker function for metadata extraction."""
        while True:
            try:
                image_data = metadata_queue.get()
                if image_data is None:  # Poison pill
                    metadata_queue.task_done()
                    break
                    
                try:
                    metadata = extract_metadata(image_data['page_url'])
                    if metadata:
                        # Try to get high-res version URL
                        url = image_data['image_url']
                        high_res_url = get_high_res_url(url)
                        download_url = high_res_url if high_res_url else url
                        download_queue.put((download_url, metadata))
                    else:
                        error_queue.put(('metadata', image_data['page_url'], "Failed to extract metadata"))
                except Exception as e:
                    error_queue.put(('metadata', image_data['page_url'], str(e)))
                
                metadata_queue.task_done()
            except Exception as e:
                logger.error(f"Error in metadata worker: {e}")
                continue
    
    def download_worker():
        """Worker function for downloading images."""
        while True:
            try:
                item = download_queue.get()
                if item is None:  # Poison pill
                    download_queue.task_done()
                    break
                    
                url, metadata = item
                try:
                    result = download_image(url, metadata['author'])
                    if result:
                        local_path, file_hash = result
                        results_queue.put((local_path, file_hash, url, metadata))
                    else:
                        error_queue.put(('download', url, "Failed to download image"))
                except Exception as e:
                    error_queue.put(('download', url, str(e)))
                
                download_queue.task_done()
            except Exception as e:
                logger.error(f"Error in download worker: {e}")
                continue
    
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
        
        # Start metadata workers
        metadata_threads = []
        for _ in range(NUM_WORKERS):
            t = threading.Thread(target=metadata_worker)
            t.daemon = True
            t.start()
            metadata_threads.append(t)
        
        # Start download workers
        download_threads = []
        for _ in range(NUM_WORKERS):
            t = threading.Thread(target=download_worker)
            t.daemon = True
            t.start()
            download_threads.append(t)
        
        # Feed metadata queue
        for image_data in image_ids_to_process:
            metadata_queue.put(image_data)
        
        # Add poison pills for metadata workers
        for _ in range(NUM_WORKERS):
            metadata_queue.put(None)
        
        # Wait for metadata queue to be processed
        metadata_queue.join()
        
        # Add poison pills for download workers
        for _ in range(NUM_WORKERS):
            download_queue.put(None)
        
        # Wait for download queue to be processed
        download_queue.join()
        
        # Process results and errors
        downloaded_images = []
        while True:
            try:
                result = results_queue.get_nowait()
                downloaded_images.append(result)
                results_queue.task_done()
            except queue.Empty:
                break
        
        errors = []
        while True:
            try:
                error = error_queue.get_nowait()
                errors.append(error)
                error_queue.task_done()
            except queue.Empty:
                break
        
        # Process database operations in the main thread
        for local_path, file_hash, url, metadata in tqdm(downloaded_images, desc="Saving to database", colour='cyan'):
            try:
                # Insert image data
                cursor.execute("""
                    INSERT INTO images (url, local_path, sha256_hash, title, author, 
                                     author_url, license, camera_make, camera_model, 
                                     focal_length, aperture, shutter_speed, taken_date, 
                                     upload_date, page_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    url,
                    local_path,
                    file_hash,
                    metadata.get('title'),
                    metadata.get('author'),
                    metadata.get('author_url'),
                    metadata.get('license'),
                    metadata.get('camera_make'),
                    metadata.get('camera_model'),
                    metadata.get('focal_length'),
                    metadata.get('aperture'),
                    metadata.get('shutter_speed'),
                    metadata.get('taken_date'),
                    metadata.get('upload_date'),
                    metadata.get('page_url')
                ))
                
                image_id = cursor.lastrowid
                
                # Process collections
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
                    """, (image_id, gallery_db_id))
                
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
                    """, (image_id, gallery_db_id))
                
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
                    """, (image_id, tag_db_id))
                
                processed_count += 1
                author = metadata.get('author', 'unknown')
                author_image_counts[author] = author_image_counts.get(author, 0) + 1
                
                conn.commit()
                
            except Exception as e:
                logger.error(f"Error saving metadata to database for {url}: {str(e)}")
                conn.rollback()
                # Clean up the downloaded file
                try:
                    os.remove(local_path)
                except:
                    pass
                failed_count += 1
                continue
        
        # Log errors
        for error_type, url, error_msg in errors:
            logger.error(f"{error_type.capitalize()} error for {url}: {error_msg}")
            failed_count += 1
        
        # Return success if we processed all images (including skipped ones)
        total_images = len(image_data_list)
        success = (processed_count + failed_count) == total_images and failed_count == 0
        
        stats = {
            'processed_count': processed_count,
            'failed_count': failed_count,
            'skipped_count': skipped_count,
            'total_images': total_images,
            'author_counts': author_image_counts
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
    conn = init_db()
    cursor = conn.cursor()
    
    # Check initial disk space
    free_space_gb = check_disk_space(BASE_DIR)
    logger.info(f"Initial free space: {free_space_gb:.2f}GB")
    
    # Initialize space tracking
    total_downloaded_bytes = 0
    pages_processed = 0
    
    try:
        for page in tqdm(range(start_offset, TOTAL_PAGES), desc="Crawling pages", colour='blue'):
            try:
                # Check if page was already completed successfully
                cursor.execute("SELECT completion_date FROM completed_pages WHERE page_number = ?", (page,))
                if cursor.fetchone():
                    logger.info(f"Skipping page {page} - already completed successfully")
                    continue
                
                # Check disk space before each page
                free_space_gb = get_free_space_gb(BASE_DIR)
                if free_space_gb < 2:
                    raise RuntimeError(f"Critical: Only {free_space_gb:.2f}GB space remaining. Stopping crawler.")
                
                search_page_url = SEARCH_URL_TEMPLATE.format(page)
                
                # Track page size before downloading
                page_size_before = sum(f.stat().st_size for f in Path(BASE_DIR).rglob('*') if f.is_file())
                
                image_data_list = get_image_links(search_page_url, attempt=1)
                
                # Process the image list
                success, stats = process_image_list(image_data_list, conn, cursor)
                
                # Calculate space used by this page
                page_size_after = sum(f.stat().st_size for f in Path(BASE_DIR).rglob('*') if f.is_file())
                page_downloaded_bytes = page_size_after - page_size_before
                total_downloaded_bytes += page_downloaded_bytes
                pages_processed += 1
                
                if success:
                    # Record successful completion
                    cursor.execute("""
                        INSERT INTO completed_pages (
                            page_number, completion_date, image_count, total_size_bytes
                        ) VALUES (?, ?, ?, ?)
                    """, (
                        page,
                        datetime.now().isoformat(),
                        stats['total_images'],
                        page_downloaded_bytes
                    ))
                    conn.commit()
                    
                    # Remove from failed_pages if it was there
                    cursor.execute("DELETE FROM failed_pages WHERE page_number = ?", (page,))
                    conn.commit()
                    
                    logger.info(f"Successfully completed page {page} - "
                              f"Processed: {stats['processed_count']}, "
                              f"Failed: {stats['failed_count']}, "
                              f"Total: {stats['total_images']}")
                else:
                    # If processing wasn't completely successful, add to failed_pages
                    error_msg = stats.get('error', 'Unknown error')
                    cursor.execute("""
                        INSERT OR REPLACE INTO failed_pages (
                            page_number, url, error, last_attempt, status
                        ) VALUES (?, ?, ?, ?, 'pending')
                    """, (page, search_page_url, error_msg, datetime.now().isoformat()))
                    conn.commit()
                    logger.warning(f"Failed to process all images on page {page} - "
                                 f"Processed: {stats['processed_count']}, "
                                 f"Failed: {stats['failed_count']}, "
                                 f"Total: {stats['total_images']}")
                
                # Estimate space requirements
                if pages_processed % 10 == 0:  # Check every 10 pages
                    estimates = estimate_space_requirements(total_downloaded_bytes, pages_processed, TOTAL_PAGES)
                    logger.info(
                        f"Space usage estimates:\n"
                        f"  Average per page: {estimates['avg_mb_per_page']:.2f}MB\n"
                        f"  Estimated remaining: {estimates['estimated_remaining_gb']:.2f}GB\n"
                        f"  Total estimated: {estimates['total_estimated_gb']:.2f}GB\n"
                        f"  Current free space: {free_space_gb:.2f}GB"
                    )
                    
                    # Warn if estimated space requirement exceeds 80% of available space
                    if estimates['estimated_remaining_gb'] > free_space_gb * 0.8:
                        logger.warning(
                            f"Warning: Estimated remaining space requirement "
                            f"({estimates['estimated_remaining_gb']:.2f}GB) "
                            f"is more than 80% of available space ({free_space_gb:.2f}GB)"
                        )
                
                time.sleep(BASE_RATE_LIMIT)

            except RuntimeError as e:
                logger.critical(str(e))
                break
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
        # Print summary of progress
        cursor.execute("SELECT COUNT(*) FROM completed_pages")
        completed_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM failed_pages WHERE status = 'pending'")
        pending_count = cursor.fetchone()[0]
        
        logger.info("\nCrawling Summary:")
        logger.info(f"Total pages completed: {completed_count}")
        logger.info(f"Pages pending retry: {pending_count}")
        logger.info(f"Total pages processed this run: {pages_processed}")
        
        conn.close()
        logger.info("Crawler finished, database connection closed")

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
    
    # Keep track of changes
    redownloaded = 0
    failed = 0
    different = 0
    
    for image in tqdm(images, desc=f"Redownloading images for {author_name}", colour='yellow'):
        image_id, url, page_url, old_path, old_hash = image
        
        try:
            # Get fresh metadata from the page
            metadata = extract_metadata(page_url)
            if not metadata:
                logger.error(f"Failed to get metadata for {page_url}")
                failed += 1
                continue
            
            # Try to get high-res version of the image
            download_url = url
            if '_l.jpg' in url:
                for res in ['_xxl.jpg', '_xl.jpg']:
                    test_url = url.replace('_l.jpg', res)
                    try:
                        head_response = requests.head(test_url, headers=HEADERS, timeout=5)
                        if head_response.ok:
                            download_url = test_url
                            break
                    except:
                        continue
            
            # Download the image
            new_path, new_hash = download_image(download_url, author_name)
            
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
    
    # Print summary
    logger.info("\nRedownload Summary:")
    logger.info(f"Total images processed: {len(images)}")
    logger.info(f"Successfully redownloaded: {redownloaded}")
    logger.info(f"Different from original: {different}")
    logger.info(f"Failed: {failed}")
    
    conn.close()

def get_high_res_url(url):
    """Try to get the highest resolution version of an image URL."""
    if '_l.jpg' not in url:
        return url
        
    for res in ['_xxl.jpg', '_xl.jpg']:
        test_url = url.replace('_l.jpg', res)
        try:
            head_response = requests.head(test_url, headers=HEADERS, timeout=5)
            if head_response.ok:
                return test_url
        except:
            continue
            
    return url

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
    parser.add_argument('--workers', type=int, default=8,
                       help='Number of parallel download workers (default: 8)')
    args = parser.parse_args()
    
    try:
        # Set the number of workers from command line argument
        NUM_WORKERS = args.workers
        
        if args.redownload_author:
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
