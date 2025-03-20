import os
import requests_cache
from datetime import datetime
import logging

from urllib.parse import unquote

import sqlite3
import argparse
import time
from indafoto import (
    get_image_links, extract_metadata, parse_hungarian_date,
    HEADERS, COOKIES, get_high_res_url, init_db, SEARCH_URL_TEMPLATE
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('indafoto_cache.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cache configuration
CACHE_DIR = "indafoto_cache"
CACHE_EXPIRY = datetime(2024, 4, 1)  # Cache entries expire on April 1st, 2024
CHECK_INTERVAL = 60  # Check for new pages every 60 seconds

def init_cache():
    """Initialize the requests cache."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, 'indafoto_cache')
    session = requests_cache.CachedSession(
        cache_path,
        expire_after=CACHE_EXPIRY,
        headers=HEADERS,
        cookies=COOKIES
    )
    return session

def init_pre_crawled_table():
    """Initialize the pre_crawled_pages table if it doesn't exist."""
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pre_crawled_pages (
            page_number INTEGER PRIMARY KEY,
            pre_crawl_date TEXT,
            image_count INTEGER,
            status TEXT DEFAULT 'success'
        )
        """)
        conn.commit()
    finally:
        conn.close()

def get_last_successful_page():
    """Find the last successfully completed page."""
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT MAX(page_number) 
            FROM completed_pages 
            WHERE image_count > 0
        """)
        last_page = cursor.fetchone()[0]
        return last_page if last_page is not None else 0
    finally:
        conn.close()

def get_last_pre_crawled_page():
    """Find the last page that was pre-crawled."""
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT MAX(page_number) 
            FROM pre_crawled_pages 
            WHERE status = 'success'
        """)
        last_page = cursor.fetchone()[0]
        return last_page if last_page is not None else 0
    finally:
        conn.close()

def mark_page_pre_crawled(page_number, image_count, status='success'):
    """Mark a page as pre-crawled in the database."""
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO pre_crawled_pages 
            (page_number, pre_crawl_date, image_count, status)
            VALUES (?, ?, ?, ?)
        """, (page_number, datetime.now().isoformat(), image_count, status))
        conn.commit()
    finally:
        conn.close()

def is_page_pre_crawled(page_number):
    """Check if a page has been pre-crawled."""
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 1 FROM pre_crawled_pages 
            WHERE page_number = ? AND status = 'success'
        """, (page_number,))
        return cursor.fetchone() is not None
    finally:
        conn.close()

def get_cached_image_links(search_page_url, session=None):
    """Get image links from a search page, using cache if available."""
    if session is None:
        session = init_cache()
    
    try:
        logger.info(f"Fetching URL: {search_page_url}")
        return get_image_links(search_page_url, session=session)
    except Exception as e:
        logger.error(f"Error fetching {search_page_url}: {e}")
        return []

def get_cached_metadata(photo_page_url, session=None):
    """Get metadata from a photo page, using cache if available."""
    if session is None:
        session = init_cache()
    
    try:
        metadata = extract_metadata(photo_page_url, session=session)
        if metadata:
            # Try to get high-res version URL
            if 'image_url' in metadata:
                high_res_url = get_high_res_url(metadata['image_url'], session=session)
                if high_res_url:
                    metadata['image_url'] = high_res_url
        return metadata
    except Exception as e:
        logger.error(f"Error extracting metadata from {photo_page_url}: {e}")
        return None

def pre_crawl_pages(start_page=None, num_pages=10, session=None):
    """Pre-crawl a range of pages and cache their content."""
    if session is None:
        session = init_cache()
    
    # If no start page specified, find the last successful page
    if start_page is None:
        start_page = get_last_successful_page()
        logger.info(f"Found last successful page: {start_page}")
        # Add 10 to get ahead of the main crawler
        start_page += 10
    
    logger.info(f"Starting pre-crawl from page {start_page}")
    
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        for page in range(start_page, start_page + num_pages):
            # Skip if already pre-crawled
            if is_page_pre_crawled(page):
                logger.info(f"Skipping page {page} - already pre-crawled")
                continue
                
            search_page_url = SEARCH_URL_TEMPLATE.format(page)
            
            # Check if page is already completed
            cursor.execute("""
                SELECT completion_date, image_count 
                FROM completed_pages 
                WHERE page_number = ?
            """, (page,))
            result = cursor.fetchone()
            
            if result and result[1] > 0:
                logger.info(f"Skipping page {page} - already completed with {result[1]} images")
                continue
            
            logger.info(f"Pre-crawling page {page}")
            
            # This will automatically cache the response
            image_data_list = get_cached_image_links(search_page_url, session)
            
            if not image_data_list:
                logger.warning(f"No images found on page {page}")
                mark_page_pre_crawled(page, 0, 'no_images')
                continue
            
            # Cache metadata for each image
            for image_data in image_data_list:
                try:
                    page_url = image_data.get('page_url')
                    if page_url:
                        # This will automatically cache the response
                        metadata = get_cached_metadata(page_url, session)
                        if metadata:
                            logger.debug(f"Cached metadata for {page_url}")
                except Exception as e:
                    logger.error(f"Error caching metadata for {page_url}: {e}")
                    continue
            
            # Mark page as successfully pre-crawled
            mark_page_pre_crawled(page, len(image_data_list))
            
            # Add a small delay between pages to avoid rate limiting
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"Error during pre-crawling: {e}")
    finally:
        conn.close()

def remove_from_cache(url, session=None):
    """Remove a URL from the cache."""
    if session is None:
        session = init_cache()
    
    try:
        session.cache.delete(url)
        logger.debug(f"Removed {url} from cache")
    except Exception as e:
        logger.error(f"Error removing {url} from cache: {e}")

def cleanup_pre_crawled_pages(session=None):
    """Remove pre-crawled pages that are no longer needed from the cache."""
    if session is None:
        session = init_cache()
    
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        # Get the last successful page
        last_successful = get_last_successful_page()
        if last_successful is None:
            return
            
        # Get pages that were pre-crawled but are now completed or too old
        cursor.execute("""
            SELECT p.page_number, p.pre_crawl_date, p.status
            FROM pre_crawled_pages p
            LEFT JOIN completed_pages c ON p.page_number = c.page_number
            WHERE c.page_number IS NOT NULL  -- Page is completed
            OR p.page_number < ?  -- Page is too old (more than 20 pages behind)
            OR p.status = 'no_images'  -- Page had no images
            OR p.status = 'error'  -- Page had errors
        """, (last_successful - 20,))
        
        pages_to_cleanup = cursor.fetchall()
        
        for page_number, pre_crawl_date, status in pages_to_cleanup:
            # Get the search page URL
            search_page_url = SEARCH_URL_TEMPLATE.format(page_number)
            
            # Get all image URLs from this page
            cursor.execute("""
                SELECT url, page_url 
                FROM images 
                WHERE page_url LIKE ?
            """, (f"%page={page_number}%",))
            image_urls = cursor.fetchall()
            
            # Remove the search page from cache
            remove_from_cache(search_page_url, session)
            
            # Remove all image pages and image URLs from cache
            for image_url, page_url in image_urls:
                remove_from_cache(page_url, session)
                remove_from_cache(image_url, session)
            
            # Remove from pre_crawled_pages table
            cursor.execute("DELETE FROM pre_crawled_pages WHERE page_number = ?", (page_number,))
            
            logger.info(f"Cleaned up pre-crawled page {page_number} (status: {status})")
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error cleaning up pre-crawled pages from cache: {e}")
        conn.rollback()
    finally:
        conn.close()

def cleanup_completed_pages(session=None):
    """Remove successfully completed pages and downloaded images from the cache."""
    if session is None:
        session = init_cache()
    
    conn = init_db()
    cursor = conn.cursor()
    
    try:
        # Get all completed pages
        cursor.execute("""
            SELECT page_number, completion_date, image_count 
            FROM completed_pages 
            WHERE image_count > 0
        """)
        completed_pages = cursor.fetchall()
        
        for page_number, completion_date, image_count in completed_pages:
            # Get the search page URL
            search_page_url = SEARCH_URL_TEMPLATE.format(page_number)
            
            # Get all image URLs from this page
            cursor.execute("""
                SELECT url, page_url 
                FROM images 
                WHERE page_url LIKE ?
            """, (f"%page={page_number}%",))
            image_urls = cursor.fetchall()
            
            # Remove the search page from cache
            remove_from_cache(search_page_url, session)
            
            # Remove all image pages and image URLs from cache
            for image_url, page_url in image_urls:
                remove_from_cache(page_url, session)
                remove_from_cache(image_url, session)
            
            logger.info(f"Removed page {page_number} and its {image_count} images from cache")
        
        # Clean up caches for all downloaded images (those in the images table)
        cursor.execute("""
            SELECT url, page_url 
            FROM images
        """)
        downloaded_images = cursor.fetchall()
        
        if downloaded_images:
            logger.info(f"Cleaning up caches for {len(downloaded_images)} downloaded images")
            for image_url, page_url in downloaded_images:
                remove_from_cache(page_url, session)
                remove_from_cache(image_url, session)
            logger.info("Finished cleaning up downloaded image caches")
            
        # Clean up pre-crawled pages that are no longer needed
        cleanup_pre_crawled_pages(session)
            
    except Exception as e:
        logger.error(f"Error cleaning up completed pages and downloaded images from cache: {e}")

def continuous_pre_crawl(num_pages=10):
    """Continuously pre-crawl pages that are 10 pages ahead of the last completed page."""
    session = init_cache()
    logger.info("Starting continuous pre-crawler")
    
    # Initialize the pre_crawled_pages table
    init_pre_crawled_table()
    
    # Maximum number of pages to pre-crawl ahead
    MAX_PAGES_AHEAD = 20
    
    while True:
        try:
            # Get the current last successful page
            last_page = get_last_successful_page()
            logger.info(f"Current last successful page: {last_page}")
            
            # Get the last pre-crawled page
            last_pre_crawled = get_last_pre_crawled_page()
            logger.info(f"Last pre-crawled page: {last_pre_crawled}")
            
            # Calculate how far ahead we are
            pages_ahead = last_pre_crawled - last_page if last_pre_crawled else 0
            
            # If we're too far ahead, wait
            if pages_ahead >= MAX_PAGES_AHEAD:
                logger.info(f"We're {pages_ahead} pages ahead, waiting for main crawler to catch up...")
                time.sleep(CHECK_INTERVAL)
                continue
            
            # Calculate the next page to start from
            next_page = last_page + 10
            
            # If we've already pre-crawled further ahead, start from there
            if last_pre_crawled > next_page:
                next_page = last_pre_crawled + 1
            
            # Don't pre-crawl more than MAX_PAGES_AHEAD pages ahead
            max_page = last_page + MAX_PAGES_AHEAD
            if next_page > max_page:
                logger.info(f"Reached maximum pages ahead limit ({MAX_PAGES_AHEAD}), waiting...")
                time.sleep(CHECK_INTERVAL)
                continue
            
            # Adjust num_pages to not exceed the maximum
            pages_to_crawl = min(num_pages, max_page - next_page + 1)
            
            logger.info(f"Pre-crawling pages {next_page} to {next_page + pages_to_crawl - 1}")
            
            # Pre-crawl the next batch of pages
            pre_crawl_pages(next_page, pages_to_crawl, session)
            
            # Clean up completed pages from cache
            cleanup_completed_pages(session)
            
            # Show cache statistics
            stats = get_cache_stats()
            logger.info("\nCache Statistics:")
            logger.info(f"Total requests: {stats['total_requests']}")
            logger.info(f"Cached requests: {stats['cached_requests']}")
            logger.info(f"Cache size: {stats['cache_size']:.2f} MB")
            logger.info(f"Pages ahead of main crawler: {pages_ahead}")
            
            # Wait before checking for new pages
            logger.info(f"Waiting {CHECK_INTERVAL} seconds before checking for new pages...")
            time.sleep(CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Pre-crawler stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in continuous pre-crawler: {e}")
            logger.info(f"Waiting {CHECK_INTERVAL} seconds before retrying...")
            time.sleep(CHECK_INTERVAL)

def get_cache_stats():
    """Get statistics about the cache."""
    cache_path = os.path.join(CACHE_DIR, 'indafoto_cache.sqlite')
    if not os.path.exists(cache_path):
        return {
            'total_requests': 0,
            'cached_requests': 0,
            'cache_size': 0
        }
    
    conn = sqlite3.connect(cache_path)
    cursor = conn.cursor()
    
    try:
        # Get total number of requests
        cursor.execute("SELECT COUNT(*) FROM responses")
        total_requests = cursor.fetchone()[0]
        
        # Get number of cached requests (not expired)
        cursor.execute("""
            SELECT COUNT(*) FROM responses 
            WHERE expires > datetime('now')
        """)
        cached_requests = cursor.fetchone()[0]
        
        # Get cache size
        cache_size = os.path.getsize(cache_path)
        
        return {
            'total_requests': total_requests,
            'cached_requests': cached_requests,
            'cache_size': cache_size / (1024 * 1024)  # Convert to MB
        }
    finally:
        conn.close()

def clear_cache():
    """Clear the entire cache."""
    cache_path = os.path.join(CACHE_DIR, 'indafoto_cache.sqlite')
    if os.path.exists(cache_path):
        try:
            os.remove(cache_path)
            logger.info("Cache cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
    else:
        logger.info("No cache file found to clear")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indafoto Cache Utils')
    parser.add_argument('--start-page', type=int,
                       help='Starting page number to pre-crawl (optional)')
    parser.add_argument('--num-pages', type=int, default=10,
                       help='Number of pages to pre-crawl (default: 10)')
    parser.add_argument('--stats', action='store_true',
                       help='Show cache statistics')
    parser.add_argument('--clear', action='store_true',
                       help='Clear the cache')
    parser.add_argument('--continuous', action='store_true',
                       help='Run in continuous mode, always staying 10 pages ahead')
    args = parser.parse_args()
    
    if args.stats:
        stats = get_cache_stats()
        logger.info("\nCache Statistics:")
        logger.info(f"Total requests: {stats['total_requests']}")
        logger.info(f"Cached requests: {stats['cached_requests']}")
        logger.info(f"Cache size: {stats['cache_size']:.2f} MB")
    elif args.clear:
        clear_cache()
    elif args.continuous:
        continuous_pre_crawl(args.num_pages)
    else:
        session = init_cache()
        pre_crawl_pages(args.start_page, args.num_pages, session)
        stats = get_cache_stats()
        logger.info("\nPre-crawl completed. Cache Statistics:")
        logger.info(f"Total requests: {stats['total_requests']}")
        logger.info(f"Cached requests: {stats['cached_requests']}")
        logger.info(f"Cache size: {stats['cache_size']:.2f} MB") 