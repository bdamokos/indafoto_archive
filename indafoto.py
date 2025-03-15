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
SEARCH_URL_TEMPLATE = "https://indafoto.hu/search/list?profile=main&sphinx=1&search=advanced&textsearch=fulltext&textuser=&textmap=&textcompilation=&photo=&datefrom=&dateto=&licence%5B1%5D=I1%3BI2%3BII1%3BII2&licence%5B2%5D=I1%3BI2%3BI3&page_offset={}"
BASE_DIR = "indafoto_archive"
DB_FILE = "indafoto.db"
TOTAL_PAGES = 15000
RATE_LIMIT = 2  # seconds between requests
FILES_PER_DIR = 1000  # Maximum number of files per directory

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
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE,
    local_path TEXT,
    title TEXT,
    description TEXT,
    author TEXT,
    license TEXT,
    camera_make TEXT,
    camera_model TEXT,
    focal_length TEXT,
    aperture TEXT,
    shutter_speed TEXT,
    taken_date TEXT,
    gallery_name TEXT,
    gallery_url TEXT,
    upload_date TEXT
)
""")
conn.commit()

def get_image_directory(author):
    """Create and return a directory path for saving images."""
    # Create author directory
    author_dir = os.path.join(BASE_DIR, re.sub(r'[<>:"/\\|?*]', '_', author))
    os.makedirs(author_dir, exist_ok=True)
    
    # Count existing files in author directory
    existing_files = len([f for f in os.listdir(author_dir) if os.path.isfile(os.path.join(author_dir, f))])
    subdir_num = existing_files // FILES_PER_DIR
    
    # Create numbered subdirectory
    subdir = os.path.join(author_dir, str(subdir_num))
    os.makedirs(subdir, exist_ok=True)
    
    return subdir

def get_image_links(search_page_url):
    """Extract image links from a search result page."""
    try:
        logger.info(f"Attempting to fetch URL: {search_page_url}")
        response = requests.get(
            search_page_url,
            headers=HEADERS,
            cookies=COOKIES,
            timeout=10,
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

def extract_metadata(photo_page_url):
    """Extract metadata from a photo page."""
    try:
        response = requests.get(photo_page_url, headers=HEADERS, cookies=COOKIES, timeout=10)
        if not response.ok:
            logger.error(f"Failed to fetch metadata from {photo_page_url}: HTTP {response.status_code}")
            return {
                'title': "Unknown",
                'description': "No description",
                'author': "Unknown",
                'license': "Unknown",
                'camera_make': None,
                'camera_model': None,
                'focal_length': None,
                'aperture': None,
                'shutter_speed': None,
                'taken_date': None,
                'gallery_name': None,
                'gallery_url': None,
                'upload_date': None,
                'high_res_url': None
            }

        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extract basic metadata
        title = soup.find("meta", property="og:title")
        description = soup.find("meta", property="og:description")
        author = soup.find("span", class_="photo-author")
        license_info = soup.find("span", class_="photo-license")
        
        # Extract EXIF data from the table
        exif_data = {}
        exif_table = soup.find('table')
        if exif_table:
            for row in exif_table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) == 2:
                    key = cols[0].text.strip(':')
                    value = cols[1].text.strip()
                    exif_data[key] = value
        
        # Extract gallery information
        gallery_link = soup.find('a', href=lambda x: x and '/album/' in x)
        gallery_name = gallery_link.text if gallery_link else None
        gallery_url = gallery_link['href'] if gallery_link else None
        
        # Extract upload date
        upload_date = None
        date_elem = soup.find(text=re.compile(r'\d{4}\. \w+\. \d{1,2}\.'))
        if date_elem:
            try:
                upload_date = datetime.strptime(date_elem.strip(), '%Y. %b. %d.').strftime('%Y-%m-%d')
            except ValueError:
                pass
        
        # Find highest resolution image URL
        high_res_url = None
        xxl_link = soup.find('a', href=lambda x: x and '_xxl.jpg' in x)
        if xxl_link:
            high_res_url = xxl_link['href']
        
        return {
            'title': title["content"] if title and title.get("content") else "Unknown",
            'description': description["content"] if description and description.get("content") else "No description",
            'author': author.text.strip() if author else "Unknown",
            'license': license_info.text.strip() if license_info else "Unknown",
            'camera_make': exif_data.get('Gyártó'),
            'camera_model': exif_data.get('Model'),
            'focal_length': exif_data.get('Fókusztáv'),
            'aperture': exif_data.get('Rekesz'),
            'shutter_speed': exif_data.get('Zársebesség'),
            'taken_date': exif_data.get('Készült'),
            'gallery_name': gallery_name,
            'gallery_url': gallery_url,
            'upload_date': upload_date,
            'high_res_url': high_res_url
        }
    except Exception as e:
        logger.error(f"Error extracting metadata from {photo_page_url}: {e}")
        return None

def download_image(image_url, author):
    """Download an image and save locally."""
    try:
        # Get the directory for this author
        save_dir = get_image_directory(author)
        
        # Create filename from the last part of the URL
        filename = os.path.join(save_dir, image_url.split("/")[-1])
        
        if os.path.exists(filename):  # Skip if already downloaded
            logger.warning(f"Skipping download, file already exists: {filename}")
            return filename

        response = requests.get(image_url, headers=HEADERS, timeout=10, stream=True)
        response.raise_for_status()
        
        with open(filename, "wb") as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)
        
        logger.info(f"Successfully downloaded image to {filename}")
        return filename
    except Exception as e:
        logger.error(f"Failed to download {image_url}: {e}")
        return None

def crawl_images():
    """Main function to crawl images and save metadata."""
    logger.info(f"Starting crawl process for {TOTAL_PAGES} pages")
    total_downloaded = 0
    total_skipped = 0
    total_errors = 0

    for page in tqdm(range(1, TOTAL_PAGES + 1), desc="Crawling pages"):
        search_page_url = SEARCH_URL_TEMPLATE.format(page)
        logger.info(f"Processing page {page}/{TOTAL_PAGES}")
        image_data_list = get_image_links(search_page_url)

        for image_data in image_data_list:
            image_url = image_data['image_url']
            photo_page_url = image_data['page_url']
            
            # Check if we've already processed this image
            cursor.execute("SELECT 1 FROM images WHERE url=?", (image_url,))
            if cursor.fetchone():
                logger.debug(f"Skipping already processed image: {image_url}")
                total_skipped += 1
                continue

            metadata = extract_metadata(photo_page_url)
            if not metadata:
                total_errors += 1
                continue

            # Use high resolution URL if available
            download_url = metadata['high_res_url'] or image_url
            local_path = download_image(download_url, metadata['author'])

            if local_path:
                cursor.execute("""
                    INSERT INTO images (
                        url, local_path, title, description, author, license,
                        camera_make, camera_model, focal_length, aperture,
                        shutter_speed, taken_date, gallery_name, gallery_url, upload_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    image_url, local_path, metadata['title'], metadata['description'],
                    metadata['author'], metadata['license'], metadata['camera_make'],
                    metadata['camera_model'], metadata['focal_length'], metadata['aperture'],
                    metadata['shutter_speed'], metadata['taken_date'], metadata['gallery_name'],
                    metadata['gallery_url'], metadata['upload_date']
                ))
                conn.commit()
                total_downloaded += 1
            else:
                total_errors += 1

        logger.info(f"Page {page} completed. Progress: Downloaded: {total_downloaded}, Skipped: {total_skipped}, Errors: {total_errors}")
        time.sleep(RATE_LIMIT)

    logger.info(f"Crawl completed. Total images downloaded: {total_downloaded}, Skipped: {total_skipped}, Errors: {total_errors}")

if __name__ == "__main__":
    logger.info("Starting Indafoto crawler")
    try:
        crawl_images()
    except Exception as e:
        logger.critical(f"Unexpected error occurred: {e}", exc_info=True)
    finally:
        conn.close()
        logger.info("Crawler finished, database connection closed")
