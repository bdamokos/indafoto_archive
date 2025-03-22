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
CHECK_INTERVAL = 10  # 1 minute between full cycles
TASK_INTERVAL = 5  # 5 seconds between different task types
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15'
}

class ArchiveSubmitter:
    def __init__(self):
        self.conn = sqlite3.connect(DB_FILE)
        self.cursor = self.conn.cursor()
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
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
            response = self.session.get(timemap_url, timeout=60)
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
                                  timeout=60)
            return response.ok
        except Exception as e:
            logger.error(f"Failed to submit to archive.ph: {url} - {e}")
            return False

    def process_pending_authors(self):
        """Find and submit author pages that haven't been archived."""
        try:
            # Get unique authors that haven't been archived
            self.cursor.execute("""
                SELECT DISTINCT i.author_url 
                FROM images i 
                LEFT JOIN archive_submissions a ON i.author_url = a.url 
                WHERE i.author_url IS NOT NULL 
                AND (a.id IS NULL OR (a.status = 'failed' AND a.retry_count < 3))
            """)
            
            author_urls = self.cursor.fetchall()
            logger.info(f"Found {len(author_urls)} unarchived author pages")
            
            for (author_url,) in author_urls:
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
                SELECT i.author, COUNT(*) as total_images,
                       SUM(CASE WHEN a.status = 'success' THEN 1 ELSE 0 END) as archived_images
                FROM images i
                LEFT JOIN archive_submissions a ON i.page_url = a.url
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
                        LEFT JOIN archive_submissions a ON i.page_url = a.url
                        WHERE i.author = ? AND (a.id IS NULL OR a.status = 'failed')
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
            # Get submissions that are pending and were submitted more than 5 minutes ago
            self.cursor.execute("""
                SELECT url, submission_date, type
                FROM archive_submissions
                WHERE status = 'pending'
                AND datetime(submission_date) <= datetime('now', '-5 minutes')
            """)
            
            for url, submission_date, url_type in self.cursor.fetchall():
                # Check both archives
                archived_org, archive_org_url = self.check_archive_org(url)
                archived_ph, archive_ph_url = self.check_archive_ph(url)
                
                if archived_org or archived_ph:
                    archive_url = archive_org_url or archive_ph_url
                    self.update_submission_status(url, 'success', archive_url=archive_url)
                    logger.info(f"Verified successful archive: {url} -> {archive_url}")
                else:
                    # Update retry count and potentially mark as failed
                    self.cursor.execute("""
                        UPDATE archive_submissions
                        SET retry_count = retry_count + 1,
                            status = CASE WHEN retry_count >= 2 THEN 'failed' ELSE 'pending' END,
                            last_attempt = datetime('now')
                        WHERE url = ?
                    """, (url,))
                    self.conn.commit()
                
                time.sleep(1)  # Rate limiting for API calls
                
        except Exception as e:
            logger.error(f"Error verifying pending submissions: {e}")

    def update_submission_status(self, url, status, service=None, archive_url=None):
        """Update or insert archive submission status."""
        try:
            self.cursor.execute("""
                INSERT INTO archive_submissions (url, submission_date, status, archive_url)
                VALUES (?, datetime('now'), ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    status = excluded.status,
                    archive_url = COALESCE(excluded.archive_url, archive_submissions.archive_url),
                    last_attempt = datetime('now')
            """, (url, status, archive_url))
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
                LEFT JOIN archive_submissions a ON i.page_url = a.url
                WHERE (a.id IS NULL OR (a.status = 'failed' AND a.retry_count < 3))
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
                        SUM(CASE WHEN a.status = 'success' THEN 1 ELSE 0 END) as archived_images,
                        COUNT(*) - SUM(CASE WHEN a.status = 'success' THEN 1 ELSE 0 END) as unarchived_count
                    FROM images i
                    LEFT JOIN archive_submissions a ON i.page_url = a.url
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
                LEFT JOIN archive_submissions a ON i.page_url = a.url
                WHERE i.author = ? 
                AND (a.id IS NULL OR (a.status = 'failed' AND a.retry_count < 3))
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indafoto Archive Submitter')
    parser.add_argument('--no-update-check', action='store_true',
                       help='Skip checking for updates')
    args = parser.parse_args()
    
    try:
        # Check for updates unless explicitly disabled
        if not args.no_update_check:
            check_for_updates(__file__)
            
        submitter = ArchiveSubmitter()
        submitter.run()
    except Exception as e:
        logger.critical(f"Unexpected error occurred: {e}", exc_info=True) 