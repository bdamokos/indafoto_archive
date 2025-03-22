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
        
        # Ensure the archive_service column exists
        self._ensure_db_schema()
        
    def _ensure_db_schema(self):
        """Ensure the database schema is up to date with any new columns."""
        try:
            # Check if archive_service column exists
            self.cursor.execute("PRAGMA table_info(archive_submissions)")
            columns = self.cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if 'archive_service' not in column_names:
                # Add the column
                logger.info("Adding archive_service column to archive_submissions table")
                self.cursor.execute("""
                    ALTER TABLE archive_submissions
                    ADD COLUMN archive_service TEXT
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
                
            # Check if the composite index exists
            self.cursor.execute("PRAGMA index_list(archive_submissions)")
            indexes = self.cursor.fetchall()
            index_names = [idx[1] for idx in indexes]
            
            if 'idx_archive_submissions_url_service' not in index_names:
                # Create the composite unique index
                logger.info("Creating composite index for url and archive_service")
                self.cursor.execute("""
                    CREATE UNIQUE INDEX idx_archive_submissions_url_service 
                    ON archive_submissions(url, archive_service)
                """)
                self.conn.commit()
                logger.info("Successfully created composite index")
                
        except Exception as e:
            logger.error(f"Error ensuring database schema: {e}")
            self.conn.rollback()
        
    def check_archive_org(self, url):
        """Check if URL is already archived on archive.org using CDX API."""
        try:
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
                        return True, f"https://web.archive.org/web/{timestamp}/{url}"
            return False, None
        except Exception as e:
            logger.error(f"Failed to check archive.org for {url}: {e}")
            return False, None

    def check_archive_ph(self, url):
        """Check if URL is already archived on archive.ph using Memento TimeMap."""
        try:
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
                            return True, f"https://archive.ph/{url}"
            return False, None
        except Exception as e:
            logger.error(f"Failed to check archive.ph for {url}: {e}")
            return False, None

    def fetch_archive_ph_listings(self, domain="indafoto.hu", max_pages=10, author_pattern=None):
        """
        Fetch and parse the archive.ph listings for a domain or specific author pattern.
        
        Args:
            domain: The domain to search archives for
            max_pages: Maximum number of pages to fetch (20 items per page)
            author_pattern: Optional specific author URL pattern (e.g., 'https://indafoto.hu/felsmann/*')
            
        Returns:
            A list of tuples (original_url, archive_url, timestamp)
        """
        results = []
        offset = 0
        
        try:
            # Get the most recent archive we've already recorded
            self.cursor.execute("""
                SELECT MIN(submission_date) 
                FROM archive_submissions 
                WHERE archive_service = 'archive.ph' AND status = 'success'
            """)
            newest_timestamp = self.cursor.fetchone()[0]
            if newest_timestamp:
                newest_date = datetime.strptime(newest_timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
            else:
                newest_date = None
                
            # Track if we've seen any items newer than our newest record
            found_new_items = True
            
            for page in range(max_pages):
                if not found_new_items:
                    break
                    
                logger.info(f"Fetching archive.ph listing page {page+1}/{max_pages} (offset={offset})")
                
                # Build the URL based on whether we're searching for a specific author pattern or domain
                url = f"https://archive.ph/offset={offset}/" if offset > 0 else "https://archive.ph/"
                if author_pattern:
                    logger.info(f"Using author-specific pattern: {author_pattern}")
                    url += f"https://{domain}/{author_pattern}/*"
                else:
                    url += domain
                
                try:
                    response = self.session.get(url, headers=HEADERS, timeout=60)
                    if response.status_code == 429:
                        logger.warning(f"Rate limited by archive.ph (429). Stopping pagination.")
                        break
                    elif not response.ok:
                        logger.error(f"Failed to fetch archive.ph listing page {page+1}: {response.status_code}")
                        break
                    
                    soup = BeautifulSoup(response.text, "html.parser")
                    
                    # Each row has an id like "row0", "row1", etc.
                    # Each row contains a link to the archived page and the original URL
                    rows = soup.select("div[id^='row']")
                    
                    if not rows:
                        logger.info(f"No more items found on page {page+1}")
                        break
                    
                    for row in rows:
                        try:
                            # The title and archive URL are in the first link in the TEXT-BLOCK
                            text_block = row.select_one(".TEXT-BLOCK")
                            if not text_block:
                                continue
                                
                            archive_link = text_block.select_one("a[href^='https://archive.ph/']")
                            if not archive_link:
                                continue
                                
                            archive_url = archive_link.get('href')
                            
                            # The original URL is in the second link
                            original_link = text_block.select("a")[1] if len(text_block.select("a")) > 1 else None
                            if not original_link:
                                continue
                                
                            original_url = original_link.get('href').replace('https://archive.ph/https://', 'https://')
                            
                            # Skip non-indafoto.hu URLs
                            if "indafoto.hu" not in original_url:
                                continue
                                
                            # The timestamp is in the THUMBS-BLOCK
                            thumbs_block = row.select_one(".THUMBS-BLOCK")
                            if not thumbs_block:
                                continue
                                
                            timestamp_div = thumbs_block.select_one("div[style*='white-space:nowrap']")
                            if not timestamp_div:
                                continue
                                
                            timestamp_str = timestamp_div.get_text().strip()
                                
                            # Parse the timestamp (format: "22 Mar 2025 21:00")
                            try:
                                timestamp = datetime.strptime(timestamp_str, "%d %b %Y %H:%M")
                                    
                                # If we have a newest date and this item is older, we can stop
                                if newest_date and timestamp < newest_date:
                                    continue
                                    
                                found_new_items = True
                                    
                                results.append((original_url, archive_url, timestamp))
                            except ValueError:
                                logger.warning(f"Could not parse timestamp: {timestamp_str}")
                        except Exception as e:
                            logger.error(f"Error parsing archive item: {e}")
                    
                    # Move to the next page
                    offset += 20
                    time.sleep(3)  # Increased sleep time to be more respectful to the server
                except Exception as e:
                    logger.error(f"Error processing archive.ph page {page+1}: {e}")
                    break
                
        except Exception as e:
            logger.error(f"Error fetching archive.ph listings: {e}")
            
        return results
        
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
            # Get the most recent archive we've already recorded
            self.cursor.execute("""
                SELECT MIN(submission_date) 
                FROM archive_submissions 
                WHERE archive_service = 'archive.org' AND status = 'success'
            """)
            newest_timestamp = self.cursor.fetchone()[0]
            if newest_timestamp:
                newest_date = datetime.strptime(newest_timestamp.split('.')[0], '%Y-%m-%d %H:%M:%S')
            else:
                newest_date = None
            
            cdx_url = f"https://web.archive.org/cdx/search/cdx?url={domain}/*&output=json&limit={limit}"
            logger.info(f"Fetching archive.org CDX data for {domain}")
            
            response = self.session.get(cdx_url, timeout=120)  # Longer timeout as this can be slow
            if not response.ok:
                logger.error(f"Failed to fetch archive.org CDX data: {response.status_code}")
                return results
                
            data = response.json()
            
            # Skip the header row
            if len(data) <= 1:
                logger.info("No archive.org data found")
                return results
                
            header = data[0]
            timestamp_idx = header.index("timestamp")
            original_idx = header.index("original")
            
            for row in data[1:]:
                timestamp_str = row[timestamp_idx]
                original_url = row[original_idx]
                
                # Parse the timestamp (format: YYYYMMDDhhmmss)
                try:
                    timestamp = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")
                    
                    # If we have a newest date and this item is older, skip it
                    if newest_date and timestamp < newest_date:
                        continue
                        
                    archive_url = f"https://web.archive.org/web/{timestamp_str}/{original_url}"
                    results.append((original_url, archive_url, timestamp))
                except ValueError:
                    logger.warning(f"Could not parse timestamp: {timestamp_str}")
            
        except Exception as e:
            logger.error(f"Error fetching archive.org listings: {e}")
            
        return results

    def update_archives_from_listings(self):
        """
        Fetch archive listings from both services and update our database.
        This handles both URLs we submitted and URLs archived by others.
        """
        try:
            # Get archive.ph listings
            ph_listings = self.fetch_archive_ph_listings()
            logger.info(f"Found {len(ph_listings)} archive.ph listings")
            
            # Get archive.org listings
            org_listings = self.fetch_archive_org_listings()
            logger.info(f"Found {len(org_listings)} archive.org listings")
            
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
                
            logger.info(f"Updated {total_updated} entries from archive listings")
            
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
                
            # First check if the is_archived column exists
            self.cursor.execute("PRAGMA table_info(archive_submissions)")
            columns = self.cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            if 'is_archived' in column_names:
                # Use the is_archived column
                self.cursor.execute("""
                    INSERT OR REPLACE INTO archive_submissions 
                    (url, archive_url, archive_service, submission_date, status, is_archived) 
                    VALUES (?, ?, ?, ?, 'success', 1)
                """, (url, archive_url, service, submission_date))
            else:
                # Fallback to the old schema without is_archived
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

    def fetch_author_archives(self, author_url):
        """
        Fetch archives specifically for a given author from both archive services.
        This extracts the author username from the URL and searches for specific patterns.
        
        Args:
            author_url: The full author URL (e.g., 'https://indafoto.hu/felsmann')
            
        Returns:
            Number of archived entries found and added to the database
        """
        total_updated = 0
        try:
            # Extract author username from URL
            author_match = re.search(r'indafoto\.hu/([^/]+)', author_url)
            if not author_match:
                logger.error(f"Could not extract author username from URL: {author_url}")
                return 0
                
            author_username = author_match.group(1)
            logger.info(f"Fetching archives for author: {author_username}")
            
            # Fetch from archive.ph with author-specific pattern
            # For archive.ph, we need to search with the pattern in the right format
            # Limit to 5 pages to avoid rate limiting (429 errors)
            ph_listings = self.fetch_archive_ph_listings(domain=f"indafoto.hu/{author_username}", max_pages=5)
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
            logger.error(f"Error fetching author archives for {author_url}: {e}")
            
        return total_updated

    def process_pending_authors(self):
        """Find and submit author pages that haven't been archived."""
        try:
            # Get unique authors that haven't been archived
            self.cursor.execute("""
                SELECT DISTINCT i.author_url 
                FROM images i 
                LEFT JOIN (
                    SELECT url, MAX(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as is_archived
                    FROM archive_submissions
                    GROUP BY url
                ) a ON i.author_url = a.url 
                WHERE i.author_url IS NOT NULL 
                AND (a.url IS NULL OR a.is_archived = 0)
            """)
            
            author_urls = self.cursor.fetchall()
            logger.info(f"Found {len(author_urls)} unarchived author pages")
            
            for (author_url,) in author_urls:
                # First, try to fetch existing archives for this author
                archives_found = self.fetch_author_archives(author_url)
                logger.info(f"Found and added {archives_found} existing archives for {author_url}")
                
                # Check if already in archive.org
                archived_org, archive_org_url = self.check_archive_org(author_url)
                archived_ph, archive_ph_url = self.check_archive_ph(author_url)
                
                # Submit to archive.org if needed
                if not archived_org:
                    if self.submit_to_archive_org(author_url):
                        logger.info(f"Submitted to archive.org: {author_url}")
                        self.update_submission_status(author_url, 'pending', 'archive.org')
                
                # Submit to archive.ph if needed
                if not archived_ph:
                    if self.submit_to_archive_ph(author_url):
                        logger.info(f"Submitted to archive.ph: {author_url}")
                        self.update_submission_status(author_url, 'pending', 'archive.ph')
                
                # Also submit author details page
                details_url = f"{author_url}/details"
                archived_org_details, _ = self.check_archive_org(details_url)
                archived_ph_details, _ = self.check_archive_ph(details_url)
                
                if not archived_org_details:
                    if self.submit_to_archive_org(details_url):
                        logger.info(f"Submitted details to archive.org: {details_url}")
                        self.update_submission_status(details_url, 'pending', 'archive.org')
                
                if not archived_ph_details:
                    if self.submit_to_archive_ph(details_url):
                        logger.info(f"Submitted details to archive.ph: {details_url}")
                        self.update_submission_status(details_url, 'pending', 'archive.ph')
                
                time.sleep(2.5)  # Rate limiting
                
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
                        # Check current archive status
                        archived_org, _ = self.check_archive_org(page_url)
                        archived_ph, _ = self.check_archive_ph(page_url)
                        
                        if not archived_org:
                            if self.submit_to_archive_org(page_url):
                                logger.info(f"Submitted image to archive.org: {page_url}")
                                self.update_submission_status(page_url, 'pending', 'archive.org')
                        
                        if not archived_ph:
                            if self.submit_to_archive_ph(page_url):
                                logger.info(f"Submitted image to archive.ph: {page_url}")
                                self.update_submission_status(page_url, 'pending', 'archive.ph')
                        
                        time.sleep(2.5)  # Rate limiting

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
            # First check if a record already exists
            self.cursor.execute("""
                SELECT id FROM archive_submissions 
                WHERE url = ? AND archive_service = ?
            """, (url, service))
            
            existing_id = self.cursor.fetchone()
            
            if existing_id:
                # Update existing record
                self.cursor.execute("""
                    UPDATE archive_submissions
                    SET status = ?,
                        archive_url = COALESCE(?, archive_url),
                        last_attempt = datetime('now')
                    WHERE id = ?
                """, (status, archive_url, existing_id[0]))
            else:
                # Insert new record
                self.cursor.execute("""
                    INSERT INTO archive_submissions 
                    (url, submission_date, status, archive_url, archive_service, retry_count)
                    VALUES (?, datetime('now'), ?, ?, ?, 0)
                """, (url, status, archive_url, service))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating submission status for {url}: {e}")
            self.conn.rollback()

    def process_marked_images(self):
        """Find and submit pages for marked images that haven't been archived."""
        try:
            # Get marked images that haven't been archived
            self.cursor.execute("""
                SELECT DISTINCT i.page_url, i.author_url
                FROM images i
                JOIN marked_images m ON i.id = m.image_id
                LEFT JOIN (
                    SELECT url, MAX(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as is_archived
                    FROM archive_submissions
                    GROUP BY url
                ) a ON i.page_url = a.url
                WHERE a.url IS NULL OR a.is_archived = 0
            """)
            
            marked_images = self.cursor.fetchall()
            logger.info(f"Found {len(marked_images)} unarchived marked images")
            
            for page_url, author_url in marked_images:
                # Check if already in archive.org
                archived_org, archive_org_url = self.check_archive_org(page_url)
                archived_ph, archive_ph_url = self.check_archive_ph(page_url)
                
                # Submit to archive.org if needed
                if not archived_org:
                    if self.submit_to_archive_org(page_url):
                        logger.info(f"Submitted marked image to archive.org: {page_url}")
                        self.update_submission_status(page_url, 'pending', 'archive.org')
                
                # Submit to archive.ph if needed
                if not archived_ph:
                    if self.submit_to_archive_ph(page_url):
                        logger.info(f"Submitted marked image to archive.ph: {page_url}")
                        self.update_submission_status(page_url, 'pending', 'archive.ph')
                
                # Also submit author page if available
                if author_url:
                    # Check author page
                    archived_org_author, _ = self.check_archive_org(author_url)
                    archived_ph_author, _ = self.check_archive_ph(author_url)
                    
                    if not archived_org_author:
                        if self.submit_to_archive_org(author_url):
                            logger.info(f"Submitted author page to archive.org: {author_url}")
                            self.update_submission_status(author_url, 'pending', 'archive.org')
                    
                    if not archived_ph_author:
                        if self.submit_to_archive_ph(author_url):
                            logger.info(f"Submitted author page to archive.ph: {author_url}")
                            self.update_submission_status(author_url, 'pending', 'archive.ph')
                    
                    # Submit author details page
                    details_url = f"{author_url}/details"
                    archived_org_details, _ = self.check_archive_org(details_url)
                    archived_ph_details, _ = self.check_archive_ph(details_url)
                    
                    if not archived_org_details:
                        if self.submit_to_archive_org(details_url):
                            logger.info(f"Submitted author details to archive.org: {details_url}")
                            self.update_submission_status(details_url, 'pending', 'archive.org')
                    
                    if not archived_ph_details:
                        if self.submit_to_archive_ph(details_url):
                            logger.info(f"Submitted author details to archive.ph: {details_url}")
                            self.update_submission_status(details_url, 'pending', 'archive.ph')
                
                time.sleep(2.5)  # Rate limiting
                
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
                        SUM(CASE WHEN a.is_archived > 0 THEN 1 ELSE 0 END) as archived_images,
                        COUNT(*) - SUM(CASE WHEN a.is_archived > 0 THEN 1 ELSE 0 END) as unarchived_count
                    FROM images i
                    LEFT JOIN (
                        SELECT url, MAX(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as is_archived
                        FROM archive_submissions
                        GROUP BY url
                    ) a ON i.page_url = a.url
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
                LEFT JOIN (
                    SELECT url, MAX(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as is_archived
                    FROM archive_submissions
                    GROUP BY url
                ) a ON i.page_url = a.url
                WHERE i.author = ? 
                AND (a.url IS NULL OR a.is_archived = 0)
                ORDER BY RANDOM()
                LIMIT 60
            """, (author_name,))
            
            images = self.cursor.fetchall()
            if not images:
                return
                
            logger.info(f"Found {len(images)} unarchived images for favorite author {author_name} in this batch.")
            
            for page_url, author_url in images:
                # Check if already in archive.org
                archived_org, archive_org_url = self.check_archive_org(page_url)
                archived_ph, archive_ph_url = self.check_archive_ph(page_url)
                
                # Submit to archive.org if needed
                if not archived_org:
                    if self.submit_to_archive_org(page_url):
                        logger.info(f"Submitted favorite author image to archive.org: {page_url}")
                        self.update_submission_status(page_url, 'pending', 'archive.org')
                
                # Submit to archive.ph if needed
                if not archived_ph:
                    if self.submit_to_archive_ph(page_url):
                        logger.info(f"Submitted favorite author image to archive.ph: {page_url}")
                        self.update_submission_status(page_url, 'pending', 'archive.ph')
                
                time.sleep(5)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error processing favorite authors: {e}")

    def run(self):
        """Main loop for the archive submitter."""
        logger.info("Starting archive submitter service")
        
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
    args = parser.parse_args()
    
    if not args.no_update_check:
        check_for_updates(__file__)

    submitter = ArchiveSubmitter()
    try:
        if args.fetch_archived:
            logger.info("Fetching existing archives...")
            # Fetch archive.org listings
            org_listings = submitter.fetch_archive_org_listings()
            logger.info(f"Found {len(org_listings)} archive.org listings")

            # Process archive.org listings
            for original_url, archive_url, timestamp in org_listings:
                submitter.update_archive_from_listing(original_url, archive_url, 'archive.org', timestamp)

            # Get archive.ph listings
            ph_listings = submitter.fetch_archive_ph_listings()
            logger.info(f"Found {len(ph_listings)} archive.ph listings")

            # Process archive.ph listings
            for original_url, archive_url, timestamp in ph_listings:
                submitter.update_archive_from_listing(original_url, archive_url, 'archive.ph', timestamp)
            
            logger.info("Finished fetching archives.")
        else:
            submitter.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
    finally:
        submitter.close() 