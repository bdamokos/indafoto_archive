#!/usr/bin/env python3
"""
This script redownloads images that are missing their local paths (i.e. they are in the database but not on the disk)
It uses the indafoto.py script to download the images and extract metadata.
If an image cannot be downloaded, it marks it for archiving using the marked_images table.
If on subsequent runs the image still fails but is confirmed to be archived, it will be deleted from the database.
"""


import sqlite3
import logging
from indafoto import download_image, extract_metadata, create_session, check_for_updates, delete_image_data
import os
from tqdm import tqdm
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('redownload_missing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_FILE = "indafoto.db"

def redownload_missing_images():
    """Redownload images that are missing their local paths."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Get all images without local paths
    cursor.execute("""
        SELECT i.id, i.url, i.page_url, i.author,
               (SELECT COUNT(*) FROM marked_images m WHERE m.image_id = i.id) as is_marked,
               (SELECT COUNT(*) FROM archive_submissions a WHERE a.url = i.page_url AND a.status = 'success') as is_archived
        FROM images i
        WHERE i.local_path IS NULL
        ORDER BY i.id DESC
    """)
    
    images = cursor.fetchall()
    if not images:
        logger.info("No images found without local paths")
        return
        
    logger.info(f"Found {len(images)} images without local paths")
    
    # Create a session to reuse for all requests
    session = create_session()
    
    # Keep track of results
    success_count = 0
    failed_count = 0
    marked_for_archive_count = 0
    deleted_count = 0
    
    try:
        for image in tqdm(images, desc="Redownloading missing images", colour='yellow'):
            image_id, url, page_url, author, is_marked, is_archived = image
            
            # If marked for archive previously and archived successfully, delete it
            if is_marked and is_archived > 0:
                logger.info(f"Image {image_id} was marked and has been archived. Deleting from database.")
                delete_image_data(conn, cursor, image_id)
                deleted_count += 1
                continue
            
            try:
                # Get fresh metadata from the page
                metadata = extract_metadata(page_url, session=session)
                if not metadata:
                    logger.error(f"Failed to get metadata for {page_url}")
                    
                    # Mark for archive if not already marked
                    if not is_marked:
                        cursor.execute("""
                            INSERT INTO marked_images (image_id, marked_date)
                            VALUES (?, ?)
                        """, (image_id, datetime.now().isoformat()))
                        conn.commit()
                        logger.info(f"Marked image {image_id} for archiving")
                        marked_for_archive_count += 1
                    
                    failed_count += 1
                    continue
                
                # Download the image
                new_path, _ = download_image(url, author, session=session)
                
                if not new_path:
                    logger.error(f"Failed to download {url}")
                    
                    # Mark for archive if not already marked
                    if not is_marked:
                        cursor.execute("""
                            INSERT INTO marked_images (image_id, marked_date)
                            VALUES (?, ?)
                        """, (image_id, datetime.now().isoformat()))
                        conn.commit()
                        logger.info(f"Marked image {image_id} for archiving")
                        marked_for_archive_count += 1
                    
                    failed_count += 1
                    continue
                
                # Update the database with the new path
                cursor.execute("""
                    UPDATE images 
                    SET local_path = ?
                    WHERE id = ?
                """, (new_path, image_id))
                
                conn.commit()
                success_count += 1
                
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                
                # Mark for archive if not already marked
                if not is_marked:
                    cursor.execute("""
                        INSERT INTO marked_images (image_id, marked_date)
                        VALUES (?, ?)
                    """, (image_id, datetime.now().isoformat()))
                    conn.commit()
                    logger.info(f"Marked image {image_id} for archiving")
                    marked_for_archive_count += 1
                
                failed_count += 1
                continue
                
    finally:
        # Clean up the session
        session.close()
    
    # Print summary
    logger.info("\nRedownload Summary:")
    logger.info(f"Total images processed: {len(images)}")
    logger.info(f"Successfully downloaded: {success_count}")
    logger.info(f"Failed: {failed_count}")
    logger.info(f"Newly marked for archive: {marked_for_archive_count}")
    logger.info(f"Deleted (archived): {deleted_count}")
    
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indafoto Missing Files Recovery Tool')
    parser.add_argument('--no-update-check', action='store_true',
                       help='Skip checking for updates')
    args = parser.parse_args()
    
    try:
        # Check for updates unless explicitly disabled
        if not args.no_update_check:
            check_for_updates(__file__)
            
        redownload_missing_images()
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True) 