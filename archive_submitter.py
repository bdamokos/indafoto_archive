#!/usr/bin/env python3
import sqlite3
import time
import logging
import niquests as requests
import re
from datetime import datetime
from urllib.parse import quote_plus
import argparse
from indafoto import check_for_updates
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('archive_submitter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_FILE = "indafoto.db"
ARCHIVE_SAMPLE_RATE = 0.005  # 0.5% sample rate for image pages
CHECK_INTERVAL = 5  # 5 seconds between full cycles
TASK_INTERVAL = 5  # 5 seconds between different task types
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive'
}

class ArchiveSubmitter:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE)
        self.cursor = self.conn.cursor()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        # Ensure the database schema is properly set up before anything else
        self._ensure_db_schema()
        
    def _ensure_db_schema(self):
        """Ensure the database schema is up to date with any new columns."""
        try:
            # First check if the archive_submissions table exists
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='archive_submissions'")
            if not self.cursor.fetchone():
                logger.info("Creating archive_submissions table - it doesn't exist")
                self.cursor.execute("""
                    CREATE TABLE archive_submissions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT NOT NULL,
                        submission_date TEXT NOT NULL,
                        status TEXT NOT NULL,
                        archive_url TEXT,
                        archive_service TEXT NOT NULL,
                        retry_count INTEGER DEFAULT 0,
                        last_attempt TEXT,
                        is_archived INTEGER DEFAULT 0,
                        type TEXT,
                        error TEXT
                    )
                """)
                # Create the composite unique index
                self.cursor.execute("""
                    CREATE UNIQUE INDEX idx_archive_submissions_url_service 
                    ON archive_submissions(url, archive_service)
                """)
                self.conn.commit()
                logger.info("Created archive_submissions table with proper schema")
                return
            
            # Check for and remove NOT NULL constraints if they cause problems
            # Check if the table has NOT NULL constraints that might need to be relaxed
            try:
                # Try to insert a record with NULL values to see if constraints are an issue
                self.cursor.execute("""
                    INSERT INTO archive_submissions 
                    (url, submission_date, status, archive_service)
                    VALUES (?, ?, ?, ?)
                """, ('test_url_for_constraint_check', 'test_date', 'test_status', None))
                
                # If we reached here, the NOT NULL constraint isn't enforced for archive_service
                # Delete the test record
                self.cursor.execute("DELETE FROM archive_submissions WHERE url = 'test_url_for_constraint_check'")
                self.conn.commit()
            except sqlite3.IntegrityError as e:
                if 'NOT NULL constraint failed' in str(e):
                    logger.info("Detected NOT NULL constraints issue. Will fix by recreating table without constraints...")
                    
                    # Backup the current table
                    self.cursor.execute("ALTER TABLE archive_submissions RENAME TO archive_submissions_old")
                    
                    # Create a new table without NOT NULL constraints
                    self.cursor.execute("""
                        CREATE TABLE archive_submissions (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            url TEXT,
                            submission_date TEXT,
                            status TEXT,
                            archive_url TEXT,
                            archive_service TEXT,
                            retry_count INTEGER DEFAULT 0,
                            last_attempt TEXT,
                            is_archived INTEGER DEFAULT 0,
                            type TEXT,
                            error TEXT
                        )
                    """)
                    
                    # Copy data from the old table to the new table
                    self.cursor.execute("""
                        INSERT INTO archive_submissions
                        SELECT 
                            id,
                            url,
                            submission_date,
                            status,
                            archive_url,
                            archive_service,
                            retry_count,
                            last_attempt,
                            is_archived,
                            type,
                            NULL as error
                        FROM archive_submissions_old
                    """)
                    
                    # Create the composite unique index
                    self.cursor.execute("""
                        CREATE UNIQUE INDEX idx_archive_submissions_url_service 
                        ON archive_submissions(url, archive_service)
                    """)
                    
                    # Drop the old table
                    self.cursor.execute("DROP TABLE archive_submissions_old")
                    
                    self.conn.commit()
                    logger.info("Fixed NOT NULL constraints by recreating table with relaxed constraints")
                else:
                    # If it's another integrity error, rollback and continue
                    self.conn.rollback()
            
            # Check if archive_service column exists
            self.cursor.execute("PRAGMA table_info(archive_submissions)")
            columns = self.cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if 'archive_service' not in column_names:
                # Add the column
                logger.info("Adding archive_service column to archive_submissions table")
                self.cursor.execute("""
                    ALTER TABLE archive_submissions
                    ADD COLUMN archive_service TEXT DEFAULT 'archive.org'
                """)
                # Update existing records to have a value
                self.cursor.execute("""
                    UPDATE archive_submissions
                    SET archive_service = 'archive.org'
                    WHERE archive_service IS NULL
                """)
                self.conn.commit()
                logger.info("Successfully added archive_service column")
                
            if 'is_archived' not in column_names:
                # Add the is_archived column
                logger.info("Adding is_archived column to archive_submissions table")
                self.cursor.execute("""
                    ALTER TABLE archive_submissions
                    ADD COLUMN is_archived INTEGER DEFAULT 0
                """)
                # Update the column for any existing successful records
                self.cursor.execute("""
                    UPDATE archive_submissions
                    SET is_archived = 1
                    WHERE status = 'success'
                """)
                self.conn.commit()
                logger.info("Successfully added is_archived column")
                
            if 'type' not in column_names:
                # Add the type column
                logger.info("Adding type column to archive_submissions table")
                self.cursor.execute("""
                    ALTER TABLE archive_submissions
                    ADD COLUMN type TEXT DEFAULT 'image_page'
                """)
                # Update existing records with type based on URL pattern
                self.cursor.execute("""
                    UPDATE archive_submissions
                    SET type = 
                        CASE 
                            WHEN url LIKE '%/details' THEN 'author_details'
                            WHEN url LIKE '%/image/%' THEN 'image_page'
                            WHEN url LIKE '%indafoto.hu/%' AND 
                                 (url NOT LIKE '%/%/%' OR url LIKE '%/albums' OR url LIKE '%/collections') 
                                 THEN 'author_page'
                            ELSE 'image_page'
                        END
                """)
                self.conn.commit()
                logger.info("Successfully added type column and updated existing records")
            
            if 'error' not in column_names:
                # Add the error column
                logger.info("Adding error column to archive_submissions table")
                self.cursor.execute("""
                    ALTER TABLE archive_submissions
                    ADD COLUMN error TEXT
                """)
                self.conn.commit()
                logger.info("Successfully added error column")
                
            # Check if the composite index exists
            self.cursor.execute("PRAGMA index_list(archive_submissions)")
            indexes = self.cursor.fetchall()
            index_names = [idx[1] for idx in indexes]
            
            if 'idx_archive_submissions_url_service' not in index_names:
                # Create the composite unique index
                logger.info("Creating composite index for url and archive_service")
                try:
                    self.cursor.execute("""
                        CREATE UNIQUE INDEX idx_archive_submissions_url_service 
                        ON archive_submissions(url, archive_service)
                    """)
                    self.conn.commit()
                    logger.info("Successfully created composite index")
                except sqlite3.OperationalError as e:
                    # This might fail if there are duplicate entries
                    logger.warning(f"Could not create index directly: {e}")
                    logger.info("Attempting to deduplicate entries before creating index")
                    
                    # Find and remove duplicates
                    self.cursor.execute("""
                        DELETE FROM archive_submissions
                        WHERE id NOT IN (
                            SELECT MIN(id)
                            FROM archive_submissions
                            GROUP BY url, archive_service
                        )
                    """)
                    self.conn.commit()
                    
                    # Try creating the index again
                    self.cursor.execute("""
                        CREATE UNIQUE INDEX idx_archive_submissions_url_service 
                        ON archive_submissions(url, archive_service)
                    """)
                    self.conn.commit()
                    logger.info("Successfully created composite index after deduplication")
                
        except Exception as e:
            logger.error(f"Error ensuring database schema: {e}")
            self.conn.rollback()
        
    def _determine_url_type(self, url):
        """
        Determine the type of URL based on its pattern.
        Returns specific types for known patterns or None for unknown patterns.
        """
        # Handle None URLs
        if url is None:
            return None
            
        if url.endswith('/details'):
            return 'author_details'
        elif '/image/' in url:
            return 'image_page'
        elif '/albums/' in url:
            return 'album_page'
        elif '/collections/' in url:
            return 'collection_page'
        elif 'indafoto.hu/' in url:
            # Check for author pages - these have format indafoto.hu/author_name
            # and don't have additional path components
            author_match = re.match(r'https?://indafoto\.hu/([^/]+)/?$', url)
            if author_match:
                return 'author_page'
        
        # Return None for unrecognized patterns
        return None

    def _normalize_image_url(self, url):
        """
        Normalize image URLs to handle equivalent patterns when checking archive results.
        For example, URLs with /user or /user?token=123243 suffixes are equivalent to their base image URL.
        """
        if not url:
            return url
            
        # Handle URLs with /user suffix (with or without query parameters)
        if '/user' in url and '/image/' in url:
            # Find the position of /user
            user_pos = url.find('/user')
            # Return everything up to /user
            return url[:user_pos]
            
        return url

    def check_archive_org(self, url):
        """Check if URL is already archived on archive.org using CDX API."""
        try:
            # First check our database for existing successful submissions
            try:
                self.cursor.execute("""
                    SELECT archive_url, submission_date
                    FROM archive_submissions
                    WHERE url = ? AND archive_service = 'archive.org' AND status = 'success'
                    ORDER BY submission_date DESC
                    LIMIT 1
                """, (url,))
                
                result = self.cursor.fetchone()
                if result:
                    archive_url, submission_date = result
                    logger.debug(f"Found {url} already verified in database for archive.org")
                    return True, archive_url
            except sqlite3.OperationalError as e:
                if 'no such column: archive_service' in str(e):
                    # The column doesn't exist yet, ensure schema and retry once
                    logger.warning("archive_service column not found, updating schema")
                    self._ensure_db_schema()
                    # Let's continue to external check after schema update
                else:
                    # Some other database error
                    logger.error(f"Database error checking archive.org status: {e}")
            
            # If not in database, check externally
            check_url = f"https://web.archive.org/cdx/search/cdx?url={quote_plus(url)}&output=json"
            response = self.session.get(check_url, timeout=60)
            if response.ok:
                data = response.json()
                if len(data) > 1:  # First row is header
                    latest_snapshot = data[-1]
                    timestamp = latest_snapshot[1]
                    snapshot_date = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
                    cutoff_date = datetime(2024, 7, 1)
                    if snapshot_date >= cutoff_date:
                        # For image pages, check if we have a normalized version in our database
                        if '/image/' in url:
                            normalized_url = self._normalize_image_url(url)
                            self.cursor.execute("""
                                SELECT archive_url, submission_date
                                FROM archive_submissions
                                WHERE url = ? AND archive_service = 'archive.org' AND status = 'success'
                                ORDER BY submission_date DESC
                                LIMIT 1
                            """, (normalized_url,))
                            normalized_result = self.cursor.fetchone()
                            if normalized_result:
                                return True, normalized_result[0]
                        return True, f"https://web.archive.org/web/{timestamp}/{url}"
            return False, None
        except Exception as e:
            logger.error(f"Failed to check archive.org for {url}: {e}")
            return False, None

    def check_archive_ph(self, url):
        """Check if URL is already archived on archive.ph using Memento TimeMap."""
        try:
            # First check our database for existing successful submissions
            try:
                self.cursor.execute("""
                    SELECT archive_url, submission_date
                    FROM archive_submissions
                    WHERE url = ? AND archive_service = 'archive.ph' AND status = 'success'
                    ORDER BY submission_date DESC
                    LIMIT 1
                """, (url,))
                
                result = self.cursor.fetchone()
                if result:
                    archive_url, submission_date = result
                    logger.debug(f"Found {url} already verified in database for archive.ph")
                    return True, archive_url
            except sqlite3.OperationalError as e:
                if 'no such column: archive_service' in str(e):
                    # The column doesn't exist yet, ensure schema and retry once
                    logger.warning("archive_service column not found, updating schema")
                    self._ensure_db_schema()
                    # Let's continue to external check after schema update
                else:
                    # Some other database error
                    logger.error(f"Database error checking archive.ph status: {e}")
            
            # If not in database, check externally
            timemap_url = f"https://archive.ph/timemap/{url}"
            response = self.session.get(timemap_url, headers=HEADERS, timeout=60)
            if response.ok:
                lines = response.text.strip().split('\n')
                if len(lines) > 1:
                    latest_archive = lines[-1]
                    datetime_match = re.search(r'datetime="([^"]+)"', latest_archive)
                    if datetime_match:
                        archive_date = datetime.strptime(datetime_match.group(1), '%a, %d %b %Y %H:%M:%S GMT')
                        cutoff_date = datetime(2024, 7, 1)
                        if archive_date >= cutoff_date:
                            # For image pages, check if we have a normalized version in our database
                            if '/image/' in url:
                                normalized_url = self._normalize_image_url(url)
                                self.cursor.execute("""
                                    SELECT archive_url, submission_date
                                    FROM archive_submissions
                                    WHERE url = ? AND archive_service = 'archive.ph' AND status = 'success'
                                    ORDER BY submission_date DESC
                                    LIMIT 1
                                """, (normalized_url,))
                                normalized_result = self.cursor.fetchone()
                                if normalized_result:
                                    return True, normalized_result[0]
                            return True, f"https://archive.ph/{url}"
            return False, None
        except Exception as e:
            logger.error(f"Failed to check archive.ph for {url}: {e}")
            return False, None

    def fetch_archive_org_listings(self, domain="indafoto.hu", limit=10000):
        """
        Fetch archived URLs from archive.org for a domain using the CDX API.
        
        Args:
            domain: The domain to search archives for
            limit: Maximum number of results to retrieve
            
        Returns:
            A list of tuples (original_url, archive_url, timestamp)
        """
        results = []
        
        try:
            # Get URLs we've already cataloged from archive.org
            self.cursor.execute("""
                SELECT url 
                FROM archive_submissions 
                WHERE archive_service = 'archive.org' AND status = 'success'
            """)
            already_archived_urls = set([row[0] for row in self.cursor.fetchall()])
            logger.info(f"Already have {len(already_archived_urls)} archive.org URLs in database")
            
            cdx_url = f"https://web.archive.org/cdx/search/cdx?url={domain}/*&output=json&limit={limit}"
            logger.info(f"Fetching archive.org CDX data for {domain}")
            
            response = self.session.get(cdx_url, timeout=120)  # Longer timeout as this can be slow
            if not response.ok:
                logger.error(f"Failed to fetch archive.org CDX data: {response.status_code}")
                return results
                
            try:
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to parse archive.org CDX data as JSON: {e}")
                return results
            
            # Skip the header row
            if len(data) <= 1:
                logger.info("No archive.org data found")
                return results
                
            # Get header indices
            try:
                header = data[0]
                timestamp_idx = header.index("timestamp")
                original_idx = header.index("original")
                status_idx = header.index("statuscode") if "statuscode" in header else None
                mime_idx = header.index("mimetype") if "mimetype" in header else None
            except Exception as e:
                logger.error(f"Error parsing CDX header: {e}")
                return results
            
            # Process data rows
            for row in data[1:]:
                try:
                    if len(row) <= max(timestamp_idx, original_idx):
                        logger.warning(f"Skipping invalid CDX row (too short): {row}")
                        continue
                    
                    timestamp_str = row[timestamp_idx]
                    original_url = row[original_idx]
                    
                    # Only process indafoto.hu URLs
                    if "indafoto.hu" not in original_url:
                        continue
                    
                    # Skip already archived URLs
                    if original_url in already_archived_urls:
                        continue
                    
                    # Verify status code if available (only accept 200 OK)
                    if status_idx is not None and len(row) > status_idx:
                        status_code = row[status_idx]
                        if status_code != "200":
                            continue
                    
                    # Verify mime type if available (only accept HTML)
                    if mime_idx is not None and len(row) > mime_idx:
                        mime_type = row[mime_idx]
                        if not (mime_type.startswith("text/html") or mime_type == ""):
                            continue
                    
                    # Parse the timestamp (format: YYYYMMDDhhmmss)
                    try:
                        timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                        
                        # Only accept captures from July 2024 onwards
                        cutoff_date = datetime(2024, 7, 1)
                        if timestamp < cutoff_date:
                            continue
                        
                        # Create archive.org URL with timestamp
                        archive_url = f"https://web.archive.org/web/{timestamp_str}/{original_url}"
                        results.append((original_url, archive_url, timestamp))
                    except ValueError:
                        logger.warning(f"Could not parse timestamp: {timestamp_str}")
                except Exception as e:
                    logger.warning(f"Error processing CDX row: {e}")
            
            logger.info(f"Found {len(results)} NEW valid archive.org entries for {domain}")
            
        except Exception as e:
            logger.error(f"Error fetching archive.org listings: {e}")
            
        return results

    def fetch_archive_ph_listings(self, domain="indafoto.hu", max_pages=50, author_pattern=None):
        """
        Fetch and parse the archive.ph listings for a domain or specific author pattern.
        
        Args:
            domain: The domain to search archives for
            max_pages: Maximum number of pages to fetch (20 items per page)
            author_pattern: Optional specific author URL pattern (e.g., 'joco_fs')
            
        Returns:
            A list of tuples (original_url, archive_url, timestamp)
        """
        results = []
        offset = 0
        
        try:
            # We'll only skip URLs we already have in our database
            # Get URLs we've already cataloged from archive.ph
            self.cursor.execute("""
                SELECT url 
                FROM archive_submissions 
                WHERE archive_service = 'archive.ph' AND status = 'success'
            """)
            already_archived_urls = set([row[0] for row in self.cursor.fetchall()])
            logger.info(f"Already have {len(already_archived_urls)} archive.ph URLs in database")
                
            # Enhanced browser-like headers
            enhanced_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive'
            }
            
            # Keep track of actual items found for pagination
            total_items_found = 0
            total_items = None
            
            for page in range(max_pages):
                logger.info(f"Fetching archive.ph listing page {page+1}/{max_pages} (offset={offset})")
                
                # Build the URL based on whether we're searching for a specific author pattern or domain
                url = f"https://archive.ph/offset={offset}/" if offset > 0 else "https://archive.ph/"
                
                if author_pattern:
                    # Handle different ways the author pattern could be provided
                    if author_pattern.startswith("http"):
                        # Full URL provided
                        search_query = author_pattern
                        if not search_query.endswith('*'):
                            search_query += '*'
                    elif '/' in author_pattern:
                        # Path with domain provided (indafoto.hu/joco_fs)
                        search_query = f"https://{author_pattern}"
                        if not search_query.endswith('*'):
                            search_query += '*'
                    else:
                        # Just the author username provided (joco_fs)
                        search_query = f"https://{domain}/{author_pattern}/*"
                    
                    logger.info(f"Using author-specific pattern: {search_query}")
                    url += search_query
                else:
                    url += domain
                
                try:
                    response = self.session.get(url, headers=enhanced_headers, timeout=60)
                    if response.status_code == 429:
                        logger.warning(f"Rate limited by archive.ph (429). Stopping pagination.")
                        break
                    elif not response.ok:
                        logger.error(f"Failed to fetch archive.ph listing page {page+1}: {response.status_code}")
                        break
                    
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    # Check if we have results by looking for the pager
                    pager = soup.select_one("#pager")
                    if pager:
                        pager_text = pager.get_text().strip()
                        # Extract pagination info like "1..20 of 194 urls"
                        pagination_match = re.search(r'(\d+)\.\.(\d+) of (\d+)', pager_text)
                        if pagination_match:
                            start, end, total = map(int, pagination_match.groups())
                            total_items = total
                            logger.info(f"Archive.ph shows {total} total items for this query ({start}-{end} displayed)")
                    
                    # Each row has an id like "row0", "row1", etc.
                    rows = soup.select("div[id^='row']")
                    logger.info(f"Found {len(rows)} rows on page {page+1}")
                    
                    if not rows:
                        logger.info(f"No rows found on page {page+1}")
                        break
                    
                    page_items_found = 0
                    for row_idx, row in enumerate(rows):
                        try:
                            # The archive URL is in the first link in the TEXT-BLOCK
                            text_block = row.select_one(".TEXT-BLOCK")
                            if not text_block:
                                continue
                                
                            # First link contains the archive URL
                            links = text_block.select("a")
                            if len(links) < 2:
                                continue
                            
                            # Get archive URL from first link
                            archive_link = links[0]
                            archive_url = archive_link.get('href')
                            if not archive_url or not archive_url.startswith("https://archive.ph/"):
                                continue
                            
                            # Get original URL from second link
                            original_link = links[1]
                            original_url_raw = original_link.get('href')
                            
                            # Parse the original URL correctly
                            if original_url_raw:
                                if original_url_raw.startswith("https://archive.ph/https://"):
                                    original_url = original_url_raw.replace("https://archive.ph/https://", "https://")
                                elif original_url_raw.startswith("https://archive.ph/http://"):
                                    original_url = original_url_raw.replace("https://archive.ph/http://", "http://")
                                else:
                                    original_url = original_url_raw
                            else:
                                continue
                            
                            # Skip non-indafoto.hu URLs
                            if "indafoto.hu" not in original_url:
                                continue
                            
                            # Skip already archived URLs
                            if original_url in already_archived_urls:
                                continue
                                
                            # The timestamp is in the THUMBS-BLOCK
                            thumbs_block = row.select_one(".THUMBS-BLOCK")
                            if not thumbs_block:
                                continue
                                
                            # Get timestamp div (contains text like "22 Mar 2025 20:48")
                            timestamp_div = thumbs_block.select_one("div[style*='white-space:nowrap']")
                            if not timestamp_div:
                                continue
                                
                            timestamp_str = timestamp_div.get_text().strip()
                            
                            # Parse the timestamp (format: "22 Mar 2025 20:48")
                            try:
                                timestamp = datetime.strptime(timestamp_str, "%d %b %Y %H:%M")
                                
                                # All items are new since we already filtered out existing URLs
                                page_items_found += 1
                                total_items_found += 1
                                results.append((original_url, archive_url, timestamp))
                            except ValueError as e:
                                logger.warning(f"Could not parse timestamp: {timestamp_str}")
                        except Exception as e:
                            logger.error(f"Error parsing archive item in row {row_idx}: {e}")
                    
                    logger.info(f"Found {page_items_found} new items on page {page+1}")
                    
                    # If we've reached the end of results or hit our limit, stop
                    if page_items_found == 0:
                        logger.info(f"No more new results to fetch (found {total_items_found}/{total_items if total_items else 'unknown'} total)")
                        break
                        
                    # Move to the next page only if we found items on this page
                    if page_items_found > 0:
                        offset += 20
                        time.sleep(3)  # Increased sleep time to be more respectful to the server
                    else:
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing archive.ph page {page+1}: {e}")
                    break
                
        except Exception as e:
            logger.error(f"Error fetching archive.ph listings: {e}")
            
        logger.info(f"Total NEW archive.ph items found: {len(results)}")
        return results

    def update_archives_from_listings(self):
        """
        Fetch archive listings from both services and update our database.
        This handles both URLs we submitted and URLs archived by others.
        """
        try:
            # Get archive.ph listings
            ph_listings = self.fetch_archive_ph_listings()
            logger.info(f"Found {len(ph_listings)} NEW archive.ph listings to add")
            
            # Get archive.org listings
            org_listings = self.fetch_archive_org_listings()
            logger.info(f"Found {len(org_listings)} NEW archive.org listings to add")
            
            # Process all listings
            total_updated = 0
            
            # Process archive.ph listings
            for original_url, archive_url, timestamp in ph_listings:
                self.update_archive_from_listing(original_url, archive_url, 'archive.ph', timestamp)
                total_updated += 1
                
            # Process archive.org listings
            for original_url, archive_url, timestamp in org_listings:
                self.update_archive_from_listing(original_url, archive_url, 'archive.org', timestamp)
                total_updated += 1
                
            logger.info(f"Added {total_updated} NEW entries from archive listings")
            
        except Exception as e:
            logger.error(f"Error updating archives from listings: {e}")
            
    def update_archive_from_listing(self, url, archive_url, service, timestamp):
        """Update the archive_submissions table with data from archive.org or archive.ph listings."""
        try:
            # Convert timestamp to string format if it's a datetime object
            if isinstance(timestamp, datetime):
                submission_date = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            else:
                submission_date = str(timestamp)
                
            # Determine the URL type
            url_type = self._determine_url_type(url)
                
            # First check if the is_archived column exists
            self.cursor.execute("PRAGMA table_info(archive_submissions)")
            columns = self.cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if 'is_archived' in column_names and 'type' in column_names and url_type:
                # Use the is_archived column and type
                self.cursor.execute("""
                    INSERT OR REPLACE INTO archive_submissions 
                    (url, archive_url, archive_service, submission_date, status, is_archived, type) 
                    VALUES (?, ?, ?, ?, 'success', 1, ?)
                """, (url, archive_url, service, submission_date, url_type))
            elif 'is_archived' in column_names:
                # Fallback to the old schema with is_archived but without type
                self.cursor.execute("""
                    INSERT OR REPLACE INTO archive_submissions 
                    (url, archive_url, archive_service, submission_date, status, is_archived) 
                    VALUES (?, ?, ?, ?, 'success', 1)
                """, (url, archive_url, service, submission_date))
            else:
                # Fallback to the old schema without is_archived and type
                self.cursor.execute("""
                    INSERT OR REPLACE INTO archive_submissions 
                    (url, archive_url, archive_service, submission_date, status) 
                    VALUES (?, ?, ?, ?, 'success')
                """, (url, archive_url, service, submission_date))
                
            self.conn.commit()
            logger.info(f"Added new successful submission for {url} in {service}")
        except Exception as e:
            logger.error(f"Error updating archive from listing for {url}: {e}")

    def submit_to_archive_org(self, url):
        """Submit URL to archive.org."""
        try:
            archive_url = f"https://web.archive.org/save/{url}"
            response = self.session.get(archive_url, timeout=60)
            return response.ok
        except Exception as e:
            logger.error(f"Failed to submit to archive.org: {url} - {e}")
            return False

    def submit_to_archive_ph(self, url):
        """Submit URL to archive.ph."""
        try:
            data = {'url': url}
            response = self.session.post('https://archive.ph/submit/', 
                                  data=data, 
                                  headers=HEADERS,
                                  timeout=60)
            return response.ok
        except Exception as e:
            logger.error(f"Failed to submit to archive.ph: {url} - {e}")
            return False

    def fetch_author_archives(self, author_username):
        """
        Fetch archives specifically for a given author from both archive services.
        
        Args:
            author_username: The username part of the author URL (e.g., 'felsmann')
            
        Returns:
            Number of archived entries found and added to the database
        """
        total_updated = 0
        try:
            logger.info(f"Fetching archives for author: {author_username}")
            
            # Fetch from archive.ph with author-specific pattern
            # Limit to 5 pages to avoid rate limiting (429 errors)
            ph_listings = self.fetch_archive_ph_listings(author_pattern=author_username, max_pages=5)
            logger.info(f"Found {len(ph_listings)} archive.ph listings for author {author_username}")
            
            # Update database with the listings
            for original_url, archive_url, timestamp in ph_listings:
                try:
                    self.update_archive_from_listing(original_url, archive_url, 'archive.ph', timestamp)
                    total_updated += 1
                except Exception as e:
                    logger.error(f"Error updating listing for {original_url}: {e}")
            
            # Fetch from archive.org with author-specific pattern
            org_url = f"indafoto.hu/{author_username}"
            org_listings = self.fetch_archive_org_listings(domain=org_url)
            logger.info(f"Found {len(org_listings)} archive.org listings for author {author_username}")
            
            # Update database with the listings
            for original_url, archive_url, timestamp in org_listings:
                try:
                    self.update_archive_from_listing(original_url, archive_url, 'archive.org', timestamp)
                    total_updated += 1
                except Exception as e:
                    logger.error(f"Error updating listing for {original_url}: {e}")
            
            logger.info(f"Updated {total_updated} archive entries for author {author_username}")
                
        except Exception as e:
            logger.error(f"Error fetching author archives for {author_username}: {e}")
            
        return total_updated

    def process_pending_authors(self):
        """Find and submit author pages that haven't been archived."""
        try:
            # Get all unique author URLs from both images and author_crawl tables
            self.cursor.execute("""
                SELECT DISTINCT author_url 
                FROM (
                    SELECT author_url FROM images
                    UNION
                    SELECT author_url FROM author_crawl
                    WHERE author_url IS NOT NULL
                )
                WHERE author_url NOT IN (
                    SELECT url FROM archive_submissions
                )
                ORDER BY author_url
            """)
            
            author_urls = self.cursor.fetchall()
            logger.info(f"Found {len(author_urls)} unarchived author URLs")
            
            for (author_url,) in author_urls:
                # First, try to fetch existing archives for this author
                author_match = re.search(r'indafoto\.hu/([^/]+)', author_url)
                if author_match:
                    author_username = author_match.group(1)
                    archives_found = self.fetch_author_archives(author_username)
                    logger.info(f"Found and added {archives_found} existing archives for {author_url}")
                
                # Check if already in archive.org
                archived_org, archive_org_url = self.check_archive_org(author_url)
                if archived_org:
                    # Update our database that it's already archived
                    self.update_submission_status(author_url, 'success', 'archive.org', archive_org_url)
                    logger.info(f"Author URL already in archive.org: {author_url} -> {archive_org_url}")
                else:
                    # Not archived, submit it
                    success = self.submit_to_archive_org(author_url)
                    if success:
                        self.update_submission_status(author_url, 'pending', 'archive.org')
                        logger.info(f"Successfully submitted author URL to archive.org: {author_url}")
                    else:
                        self.update_submission_status(author_url, 'failed', 'archive.org')
                        logger.error(f"Failed to submit author URL to archive.org: {author_url}")
                
                # Also check author details page
                details_url = author_url + "/details"
                
                # Check if details page is already in archive.org
                archived_org, archive_org_url = self.check_archive_org(details_url)
                if archived_org:
                    # Update our database that it's already archived
                    self.update_submission_status(details_url, 'success', 'archive.org', archive_org_url)
                    logger.info(f"Details URL already in archive.org: {details_url} -> {archive_org_url}")
                else:
                    # Not archived, submit it
                    success = self.submit_to_archive_org(details_url)
                    if success:
                        self.update_submission_status(details_url, 'pending', 'archive.org')
                        logger.info(f"Successfully submitted details URL to archive.org: {details_url}")
                    else:
                        self.update_submission_status(details_url, 'failed', 'archive.org')
                        logger.error(f"Failed to submit details URL to archive.org: {details_url}")
                
                # Sleep to avoid rate limiting
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error processing pending authors: {e}")

    def process_pending_images(self):
        """Find and submit image pages based on sampling rate."""
        try:
            # Get authors and their image counts
            self.cursor.execute("""
                SELECT 
                    i.author, 
                    COUNT(*) as total_images,
                    SUM(CASE WHEN a.is_archived > 0 THEN 1 ELSE 0 END) as archived_images
                FROM images i
                LEFT JOIN (
                    SELECT url, MAX(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as is_archived
                    FROM archive_submissions
                    GROUP BY url
                ) a ON i.page_url = a.url
                GROUP BY i.author
            """)
            
            for author, total_images, archived_images in self.cursor.fetchall():
                try:
                    archived_images = archived_images or 0
                    target_archives = int(total_images * ARCHIVE_SAMPLE_RATE)
                    
                    if archived_images < target_archives:
                        # Get unarchived images for this author
                        self.cursor.execute("""
                            SELECT i.page_url
                            FROM images i
                            LEFT JOIN (
                                SELECT url, MAX(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as is_archived
                                FROM archive_submissions
                                GROUP BY url
                            ) a ON i.page_url = a.url
                            WHERE i.author = ? AND (a.url IS NULL OR a.is_archived = 0)
                            ORDER BY RANDOM()
                            LIMIT ?
                        """, (author, target_archives - archived_images))
                        
                        for (page_url,) in self.cursor.fetchall():
                            try:
                                # Check current archive status
                                archived_org, _ = self.check_archive_org(page_url)
                                archived_ph, _ = self.check_archive_ph(page_url)
                                
                                if not archived_org:
                                    try:
                                        if self.submit_to_archive_org(page_url):
                                            logger.info(f"Submitted image to archive.org: {page_url}")
                                            self.update_submission_status(page_url, 'pending', 'archive.org')
                                    except Exception as org_e:
                                        logger.error(f"Error submitting to archive.org for {page_url}: {org_e}")
                                
                                if not archived_ph:
                                    try:
                                        if self.submit_to_archive_ph(page_url):
                                            logger.info(f"Submitted image to archive.ph: {page_url}")
                                            self.update_submission_status(page_url, 'pending', 'archive.ph')
                                    except Exception as ph_e:
                                        logger.error(f"Error submitting to archive.ph for {page_url}: {ph_e}")
                                
                                time.sleep(2.5)  # Rate limiting
                            except Exception as img_e:
                                logger.error(f"Error processing image {page_url}: {img_e}")
                                time.sleep(2.5)  # Continue to the next image
                except Exception as author_e:
                    logger.error(f"Error processing author {author}: {author_e}")

        except Exception as e:
            logger.error(f"Error processing pending images: {e}")

    def verify_pending_submissions(self):
        """Check status of pending submissions."""
        try:
            # Update archives from both services' listings
            self.update_archives_from_listings()
            
            # Get submissions that are pending and were submitted more than 5 minutes ago
            self.cursor.execute("""
                SELECT url, submission_date, archive_service
                FROM archive_submissions
                WHERE status = 'pending'
                AND datetime(submission_date) <= datetime('now', '-5 minutes')
            """)
            
            for url, submission_date, service in self.cursor.fetchall():
                # Check the appropriate archive
                if service == 'archive.org':
                    archived, archive_url = self.check_archive_org(url)
                else:  # archive.ph
                    archived, archive_url = self.check_archive_ph(url)
                
                if archived:
                    self.update_submission_status(url, 'success', service, archive_url=archive_url)
                    logger.info(f"Verified successful archive: {url} -> {archive_url}")
                else:
                    # Update retry count and potentially mark as failed
                    self.cursor.execute("""
                        UPDATE archive_submissions
                        SET retry_count = retry_count + 1,
                            status = CASE WHEN retry_count >= 2 THEN 'failed' ELSE 'pending' END,
                            last_attempt = datetime('now')
                        WHERE url = ? AND archive_service = ?
                    """, (url, service))
                    self.conn.commit()
                
                time.sleep(1)  # Rate limiting for API calls
                
        except Exception as e:
            logger.error(f"Error verifying pending submissions: {e}")

    def update_submission_status(self, url, status, service=None, archive_url=None):
        """Update or insert archive submission status."""
        try:
            # Ensure database schema is correct before proceeding
            try:
                # First check if record exists with this URL and service
                self.cursor.execute("""
                    SELECT id FROM archive_submissions 
                    WHERE url = ? AND archive_service = ?
                """, (url, service))
            except sqlite3.OperationalError as e:
                if 'no such column: archive_service' in str(e):
                    # The column doesn't exist yet, ensure schema
                    logger.warning("archive_service column not found, updating schema")
                    self._ensure_db_schema()
                    # Continue with updated schema
                else:
                    # Rethrow other errors
                    raise
            
            # Try again with updated schema
            self.cursor.execute("""
                SELECT id FROM archive_submissions 
                WHERE url = ? AND archive_service = ?
            """, (url, service))
            
            existing_id = self.cursor.fetchone()
            
            # Determine URL type
            url_type = self._determine_url_type(url)
            
            if existing_id:
                # Update existing record
                if url_type:
                    self.cursor.execute("""
                        UPDATE archive_submissions
                        SET status = ?,
                            archive_url = COALESCE(?, archive_url),
                            last_attempt = datetime('now'),
                            type = ?
                        WHERE id = ?
                    """, (status, archive_url, url_type, existing_id[0]))
                else:
                    # Don't update type if we can't determine it
                    self.cursor.execute("""
                        UPDATE archive_submissions
                        SET status = ?,
                            archive_url = COALESCE(?, archive_url),
                            last_attempt = datetime('now')
                        WHERE id = ?
                    """, (status, archive_url, existing_id[0]))
            else:
                # Ensure service has a value
                if service is None:
                    logger.warning(f"Service parameter is None for URL {url}, defaulting to 'archive.org'")
                    service = 'archive.org'
                    
                # Check if is_archived and error columns exist
                self.cursor.execute("PRAGMA table_info(archive_submissions)")
                columns = self.cursor.fetchall()
                column_names = [col[1] for col in columns]
                
                if 'is_archived' in column_names and 'type' in column_names and 'error' in column_names:
                    # Insert with is_archived column, type, and error
                    if url_type:
                        self.cursor.execute("""
                            INSERT INTO archive_submissions 
                            (url, submission_date, status, archive_url, archive_service, retry_count, is_archived, type, error)
                            VALUES (?, datetime('now'), ?, ?, ?, 0, CASE WHEN ? = 'success' THEN 1 ELSE 0 END, ?, NULL)
                        """, (url, status, archive_url, service, status, url_type))
                    else:
                        # Insert without type if we can't determine it
                        self.cursor.execute("""
                            INSERT INTO archive_submissions 
                            (url, submission_date, status, archive_url, archive_service, retry_count, is_archived, error)
                            VALUES (?, datetime('now'), ?, ?, ?, 0, CASE WHEN ? = 'success' THEN 1 ELSE 0 END, NULL)
                        """, (url, status, archive_url, service, status))
                elif 'is_archived' in column_names and 'type' in column_names:
                    # Insert with is_archived column and type
                    if url_type:
                        self.cursor.execute("""
                            INSERT INTO archive_submissions 
                            (url, submission_date, status, archive_url, archive_service, retry_count, is_archived, type)
                            VALUES (?, datetime('now'), ?, ?, ?, 0, CASE WHEN ? = 'success' THEN 1 ELSE 0 END, ?)
                        """, (url, status, archive_url, service, status, url_type))
                    else:
                        # Insert without type if we can't determine it
                        self.cursor.execute("""
                            INSERT INTO archive_submissions 
                            (url, submission_date, status, archive_url, archive_service, retry_count, is_archived)
                            VALUES (?, datetime('now'), ?, ?, ?, 0, CASE WHEN ? = 'success' THEN 1 ELSE 0 END)
                        """, (url, status, archive_url, service, status))
                elif 'is_archived' in column_names:
                    # Insert with is_archived column
                    self.cursor.execute("""
                        INSERT INTO archive_submissions 
                        (url, submission_date, status, archive_url, archive_service, retry_count, is_archived)
                        VALUES (?, datetime('now'), ?, ?, ?, 0, CASE WHEN ? = 'success' THEN 1 ELSE 0 END)
                    """, (url, status, archive_url, service, status))
                else:
                    # Fallback to inserting without is_archived
                    self.cursor.execute("""
                        INSERT INTO archive_submissions 
                        (url, submission_date, status, archive_url, archive_service, retry_count)
                        VALUES (?, datetime('now'), ?, ?, ?, 0)
                    """, (url, status, archive_url, service))
            
            self.conn.commit()
        except sqlite3.IntegrityError as e:
            logger.warning(f"SQLite integrity error when updating {url} for {service}: {e}")
            # Try one more time with a direct approach
            try:
                # Ensure service has a value
                if service is None:
                    logger.warning(f"Service parameter is None for URL {url}, defaulting to 'archive.org'")
                    service = 'archive.org'
                    
                url_type = self._determine_url_type(url)
                if url_type:
                    self.cursor.execute("""
                        UPDATE archive_submissions
                        SET status = ?,
                            archive_url = COALESCE(?, archive_url),
                            last_attempt = datetime('now'),
                            type = ?,
                            archive_service = ?
                        WHERE url = ? AND (archive_service = ? OR archive_service IS NULL)
                    """, (status, archive_url, url_type, service, url, service))
                else:
                    # Don't update type if we can't determine it
                    self.cursor.execute("""
                        UPDATE archive_submissions
                        SET status = ?,
                            archive_url = COALESCE(?, archive_url),
                            last_attempt = datetime('now'),
                            archive_service = ?
                        WHERE url = ? AND (archive_service = ? OR archive_service IS NULL)
                    """, (status, archive_url, service, url, service))
                self.conn.commit()
                logger.info(f"Successfully updated existing record for {url} on {service}")
            except Exception as inner_e:
                logger.error(f"Final error updating submission for {url} on {service}: {inner_e}")
                self.conn.rollback()
        except Exception as e:
            logger.error(f"Error updating submission status for {url} on {service}: {e}")
            self.conn.rollback()

    def process_marked_images(self):
        """Find and submit pages for marked images that haven't been archived."""
        try:
            # Get marked images that haven't been archived
            self.cursor.execute("""
                SELECT DISTINCT i.page_url, i.author_url
                FROM images i
                JOIN marked_images m ON i.id = m.image_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM archive_submissions
                    WHERE url = i.page_url AND status = 'success'
                )
            """)
            
            marked_images = self.cursor.fetchall()
            logger.info(f"Found {len(marked_images)} unarchived marked images")
            
            for page_url, author_url in marked_images:
                try:
                    # Check if already in archive.org
                    archived_org, archive_org_url = self.check_archive_org(page_url)
                    archived_ph, archive_ph_url = self.check_archive_ph(page_url)
                    
                    # Submit to archive.org if needed
                    if not archived_org:
                        try:
                            if self.submit_to_archive_org(page_url):
                                logger.info(f"Submitted marked image to archive.org: {page_url}")
                                self.update_submission_status(page_url, 'pending', 'archive.org')
                        except Exception as org_e:
                            logger.error(f"Error submitting marked image to archive.org for {page_url}: {org_e}")
                    
                    # Submit to archive.ph if needed
                    if not archived_ph:
                        try:
                            if self.submit_to_archive_ph(page_url):
                                logger.info(f"Submitted marked image to archive.ph: {page_url}")
                                self.update_submission_status(page_url, 'pending', 'archive.ph')
                        except Exception as ph_e:
                            logger.error(f"Error submitting marked image to archive.ph for {page_url}: {ph_e}")
                    
                    # # Also submit author page if available
                    # if author_url:
                    #     try:
                    #         # Check author page
                    #         archived_org_author, _ = self.check_archive_org(author_url)
                    #         archived_ph_author, _ = self.check_archive_ph(author_url)
                            
                    #         if not archived_org_author:
                    #             if self.submit_to_archive_org(author_url):
                    #                 logger.info(f"Submitted author page to archive.org: {author_url}")
                    #                 self.update_submission_status(author_url, 'pending', 'archive.org')
                            
                    #         if not archived_ph_author:
                    #             if self.submit_to_archive_ph(author_url):
                    #                 logger.info(f"Submitted author page to archive.ph: {author_url}")
                    #                 self.update_submission_status(author_url, 'pending', 'archive.ph')
                            
                    #         # Submit author details page
                    #         details_url = f"{author_url}/details"
                    #         archived_org_details, _ = self.check_archive_org(details_url)
                    #         archived_ph_details, _ = self.check_archive_ph(details_url)
                            
                    #         if not archived_org_details:
                    #             if self.submit_to_archive_org(details_url):
                    #                 logger.info(f"Submitted author details to archive.org: {details_url}")
                    #                 self.update_submission_status(details_url, 'pending', 'archive.org')
                            
                    #         if not archived_ph_details:
                    #             if self.submit_to_archive_ph(details_url):
                    #                 logger.info(f"Submitted author details to archive.ph: {details_url}")
                    #                 self.update_submission_status(details_url, 'pending', 'archive.ph')
                    #     except Exception as author_e:
                    #         logger.error(f"Error processing author pages for {author_url}: {author_e}")
                    
                    time.sleep(2.5)  # Rate limiting
                except Exception as img_e:
                    logger.error(f"Error processing marked image {page_url}: {img_e}")
                    time.sleep(2.5)  # Continue to the next image if there's an error
                
        except Exception as e:
            logger.error(f"Error processing marked images: {e}")

    def process_favorite_authors(self):
        """Process images from favorite authors in batches."""
        try:
            # Get favorite authors with unarchived images, biased toward authors with lowest percentage archived
            self.cursor.execute("""
                WITH author_stats AS (
                    SELECT 
                        i.author,
                        COUNT(*) as total_images,
                        SUM(CASE WHEN EXISTS (
                            SELECT 1 FROM archive_submissions 
                            WHERE url = i.page_url AND status = 'success'
                        ) THEN 1 ELSE 0 END) as archived_images,
                        COUNT(*) - SUM(CASE WHEN EXISTS (
                            SELECT 1 FROM archive_submissions 
                            WHERE url = i.page_url AND status = 'success'
                        ) THEN 1 ELSE 0 END) as unarchived_count
                    FROM images i
                    GROUP BY i.author
                )
                SELECT 
                    fa.author_name,
                    ats.total_images,
                    ats.archived_images,
                    ats.unarchived_count,
                    CASE 
                        WHEN ats.total_images = 0 THEN 0
                        ELSE ats.archived_images * 100.0 / ats.total_images 
                    END as archived_percentage
                FROM favorite_authors fa
                JOIN author_stats ats ON fa.author_name = ats.author
                WHERE ats.unarchived_count > 0
                ORDER BY RANDOM() / (100.0 - CASE 
                                         WHEN ats.total_images = 0 THEN 0
                                         ELSE ats.archived_images * 100.0 / ats.total_images 
                                     END + 0.1)
                LIMIT 1
            """)
            
            author = self.cursor.fetchone()
            if not author:
                return
                
            author_name, total_images, archived_images, unarchived_count, archived_percentage = author
            logger.info(f"Processing favorite author: {author_name} (total: {total_images}, archived: {archived_images}, unarchived: {unarchived_count}, percentage: {archived_percentage:.1f}%)")
            
            # Get unarchived images for this author in batches
            self.cursor.execute("""
                SELECT i.page_url, i.author_url
                FROM images i
                WHERE i.author = ? 
                AND NOT EXISTS (
                    SELECT 1 FROM archive_submissions
                    WHERE url = i.page_url AND status = 'success'
                )
                ORDER BY RANDOM()
                LIMIT 60
            """, (author_name,))
            
            images = self.cursor.fetchall()
            if not images:
                return
                
            logger.info(f"Found {len(images)} unarchived images for favorite author {author_name} in this batch.")
            
            for page_url, author_url in images:
                try:
                    # Check if already in archive.org
                    archived_org, archive_org_url = self.check_archive_org(page_url)
                    
                    # Submit to archive.org if needed
                    if not archived_org:
                        try:
                            if self.submit_to_archive_org(page_url):
                                logger.info(f"Submitted favorite author image to archive.org: {page_url}")
                                self.update_submission_status(page_url, 'pending', 'archive.org')
                        except Exception as org_e:
                            logger.error(f"Error submitting to archive.org for {page_url}: {org_e}")
                    
                    # Check and submit to archive.ph if needed
                    try:
                        archived_ph, archive_ph_url = self.check_archive_ph(page_url)
                        if not archived_ph:
                            if self.submit_to_archive_ph(page_url):
                                logger.info(f"Submitted favorite author image to archive.ph: {page_url}")
                                self.update_submission_status(page_url, 'pending', 'archive.ph')
                    except Exception as ph_e:
                        logger.error(f"Error submitting to archive.ph for {page_url}: {ph_e}")
                    
                    time.sleep(2)  # Rate limiting
                except Exception as img_e:
                    logger.error(f"Error processing favorite author image {page_url}: {img_e}")
                    time.sleep(2)  # Continue to the next image if there's an error
                
        except Exception as e:
            logger.error(f"Error processing favorite authors: {e}")

    def process_archived_urls(self):
        """
        Process existing archive.org and archive.ph URLs.
        This fetches and adds entries from both archive services.
        """
        try:
            # Fetch archive.org listings
            logger.info("Fetching archive.org listings...")
            org_listings = self.fetch_archive_org_listings()
            logger.info(f"Found {len(org_listings)} NEW archive.org listings to add")

            # Process archive.org listings
            archived_count = 0
            for original_url, archive_url, timestamp in org_listings:
                self.update_archive_from_listing(original_url, archive_url, 'archive.org', timestamp)
                archived_count += 1
                if archived_count % 100 == 0:
                    logger.info(f"Processed {archived_count} NEW archive.org listings")

            # Fetch archive.ph listings
            logger.info("Fetching archive.ph listings...")
            ph_listings = self.fetch_archive_ph_listings()
            logger.info(f"Found {len(ph_listings)} NEW archive.ph listings to add")

            # Process archive.ph listings
            for original_url, archive_url, timestamp in ph_listings:
                self.update_archive_from_listing(original_url, archive_url, 'archive.ph', timestamp)
                archived_count += 1
                if archived_count % 100 == 0:
                    logger.info(f"Processed {archived_count} total NEW listings")
            
            logger.info(f"Finished processing {archived_count} NEW archived URLs")
        except Exception as e:
            logger.error(f"Error processing archived URLs: {e}")

    def fix_missing_type_categorizations(self):
        """
        Find and fix records with missing or potentially incorrect type categorizations.
        This should be run periodically to ensure correct categorization.
        """
        try:
            logger.info("Starting to fix missing or incorrect URL type categorizations")
            
            # Check if the type column exists
            self.cursor.execute("PRAGMA table_info(archive_submissions)")
            columns = self.cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if 'type' not in column_names:
                logger.warning("Type column doesn't exist yet, ensuring schema first")
                self._ensure_db_schema()
                # If column was just added, the default update should have already run
                return
            
            # Get all URLs without a type or with a default type
            self.cursor.execute("""
                SELECT id, url 
                FROM archive_submissions
                WHERE type IS NULL OR type = ''
            """)
            
            records = self.cursor.fetchall()
            if not records:
                logger.info("No records with missing type categorization found")
                return
                
            logger.info(f"Found {len(records)} records with missing type categorization")
            
            # Process each record
            updates = 0
            errors = 0
            for record_id, url in records:
                try:
                    url_type = self._determine_url_type(url)
                    if url_type:
                        self.cursor.execute("""
                            UPDATE archive_submissions
                            SET type = ?
                            WHERE id = ?
                        """, (url_type, record_id))
                        updates += 1
                    else:
                        # If type can't be determined, set a default
                        self.cursor.execute("""
                            UPDATE archive_submissions
                            SET type = 'unknown'
                            WHERE id = ?
                        """, (record_id,))
                        updates += 1
                except Exception as e:
                    logger.error(f"Error processing record ID {record_id}, URL '{url}': {e}")
                    errors += 1
                    # Continue with next record
                    continue
            
            # Also check for records that might have incorrect 'image_page' defaults
            self.cursor.execute("""
                SELECT id, url 
                FROM archive_submissions
                WHERE type = 'image_page' AND url NOT LIKE '%/image/%'
            """)
            
            potential_wrong_types = self.cursor.fetchall()
            if potential_wrong_types:
                logger.info(f"Found {len(potential_wrong_types)} records with potentially incorrect 'image_page' categorization")
                
                for record_id, url in potential_wrong_types:
                    try:
                        url_type = self._determine_url_type(url)
                        if url_type and url_type != 'image_page':
                            self.cursor.execute("""
                                UPDATE archive_submissions
                                SET type = ?
                                WHERE id = ?
                            """, (url_type, record_id))
                            updates += 1
                    except Exception as e:
                        logger.error(f"Error processing potential wrong type record ID {record_id}, URL '{url}': {e}")
                        errors += 1
                        # Continue with next record
                        continue
            
            self.conn.commit()
            logger.info(f"Updated type categorization for {updates} records, encountered {errors} errors")
            
        except Exception as e:
            logger.error(f"Error fixing type categorizations: {e}")
            self.conn.rollback()

    def run(self):
        """Main loop for the archive submitter."""
        logger.info("Starting archive submitter service")
        
        # Run the fix for missing type categorizations on startup
        self.fix_missing_type_categorizations()
        
        while True:
            try:
                logger.info("Processing pending authors...")
                self.process_pending_authors()
                time.sleep(TASK_INTERVAL)  # Short break between tasks

                logger.info("Processing marked images...")
                self.process_marked_images()
                time.sleep(TASK_INTERVAL)  # Short break between tasks
                
                logger.info("Processing favorite authors...")
                self.process_favorite_authors()
                time.sleep(TASK_INTERVAL)  # Short break between tasks
                
                logger.info("Processing pending images...")
                self.process_pending_images()
                time.sleep(TASK_INTERVAL)  # Short break between tasks
                
                logger.info("Verifying pending submissions...")
                self.verify_pending_submissions()
                
                logger.info("Processing archived URLs...")
                self.process_archived_urls()
                
                # Run the type categorization fix periodically
                self.fix_missing_type_categorizations()
                
                logger.info(f"Sleeping for {CHECK_INTERVAL} seconds...")
                time.sleep(CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(CHECK_INTERVAL)

    def close(self):
        """Close database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indafoto Archive Submitter')
    parser.add_argument('--no-update-check', action='store_true',
                       help='Skip checking for updates')
    parser.add_argument('--fetch-archived', action='store_true', help='Only fetch existing archives without submitting new pages')
    parser.add_argument('--fetch-author', help='Fetch archives for a specific author username')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)

    if not args.no_update_check:
        check_for_updates(__file__)

    submitter = ArchiveSubmitter()
    try:
        if args.fetch_author:
            logger.info(f"Fetching archives for author: {args.fetch_author}")
            submitter.fetch_author_archives(args.fetch_author)
        elif args.fetch_archived:
            logger.info("Fetching and processing archived URLs...")
            submitter.process_archived_urls()
        else:
            submitter.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
    finally:
        submitter.close() 