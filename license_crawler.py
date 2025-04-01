import niquests as requests
import logging
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
import sys
from typing import Optional, Dict, List
from datetime import datetime
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('license_crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration
SEARCH_URL_TEMPLATE = "https://indafoto.hu/search/list?profile=main&sphinx=1&search=advanced&textsearch=fulltext&textuser=&textmap=&textcompilation=&photo=&datefrom=&dateto=&licence%5B2%5D=I1%3BI2%3BI3&page_offset={}"
BASE_RATE_LIMIT = 1  # Base seconds between requests
BASE_TIMEOUT = 60    # Base timeout in seconds
TOTAL_PAGES = 14267  # Total number of pages to crawl

def create_session(max_retries=3, pool_connections=4):
    """Create a requests session with retry strategy."""
    session = requests.Session()
    retry_strategy = requests.adapters.Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = requests.adapters.HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=pool_connections,
        pool_maxsize=pool_connections
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_image_links(search_page_url: str, attempt: int = 1, session: Optional[requests.Session] = None) -> List[Dict]:
    """Extract image links from a search result page."""
    try:
        logger.info(f"Attempting to fetch URL: {search_page_url} (attempt {attempt})")
        timeout = BASE_TIMEOUT * attempt  # Progressive timeout
        
        if not session:
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

def extract_license(photo_page_url: str, attempt: int = 1, session: Optional[requests.Session] = None) -> Optional[str]:
    """Extract license information from a photo page."""
    if session is None:
        session = create_session()
    
    try:
        response = session.get(photo_page_url, timeout=BASE_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
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
        
        return license_info
    except Exception as e:
        logger.error(f"Error extracting license from {photo_page_url}: {str(e)}")
        if attempt < 3:
            time.sleep(BASE_RATE_LIMIT * attempt)
            return extract_license(photo_page_url, attempt + 1, session)
        return None

def save_license_counts(license_counts: Dict[str, int], filename: str = 'license_counts.txt'):
    """Save license counts to a text file."""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("License Counts:\n")
        f.write("-" * 50 + "\n")
        for license_type, count in sorted(license_counts.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{license_type}: {count}\n")

def crawl_licenses(start_page: int = 0, end_page: Optional[int] = None):
    """Crawl pages and count license types."""
    if end_page is None:
        end_page = TOTAL_PAGES
    
    session = create_session()
    license_counts = {}
    
    try:
        for page_num in range(start_page, end_page):
            offset = page_num * 20  # Each page shows 20 images
            search_url = SEARCH_URL_TEMPLATE.format(offset)
            
            logger.info(f"Processing page {page_num + 1}/{end_page}")
            
            # Get image links from search page
            image_data_list = get_image_links(search_url, session=session)
            
            # Process each image on the page
            for image_data in image_data_list:
                license_type = extract_license(image_data['page_url'], session=session)
                
                if license_type:
                    license_counts[license_type] = license_counts.get(license_type, 0) + 1
                
                # Rate limiting
                time.sleep(BASE_RATE_LIMIT)
            
            # Save progress after each page
            save_license_counts(license_counts)
            
            # Log current counts
            logger.info(f"Completed page {page_num + 1}. Found {len(license_counts)} different license types:")
            for license_type, count in sorted(license_counts.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {license_type}: {count}")
            
    except KeyboardInterrupt:
        logger.info("Crawling interrupted by user. Saving progress...")
    finally:
        session.close()
        # Final save
        save_license_counts(license_counts)
        
        # Print final summary
        logger.info("\nCrawling completed. Final license summary:")
        for license_type, count in sorted(license_counts.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"{license_type}: {count}")

if __name__ == "__main__":
    # Allow specifying start and end pages as command line arguments
    start_page = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    end_page = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    crawl_licenses(start_page, end_page) 