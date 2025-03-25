#!/usr/bin/env python3
"""
This script fixes stale NFS file handles by redownloading the affected images.
It uses the indafoto.py script to download the images and extract metadata.
"""

import sqlite3
import logging
from indafoto import download_image, extract_metadata, create_session, delete_image_data, get_high_res_url
import os
from tqdm import tqdm
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fix_stale_files.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_FILE = "indafoto.db"

def fix_stale_files():
    """Fix stale NFS files by redownloading them."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Get all images that might have stale handles
    cursor.execute("""
        SELECT i.id, i.url, i.page_url, i.local_path, i.author
        FROM images i
        WHERE i.local_path IS NOT NULL
        ORDER BY i.author, i.id
    """)
    
    images = cursor.fetchall()
    if not images:
        logger.info("No images found to check")
        return
        
    logger.info(f"Found {len(images)} images to check")
    
    # Create a session to reuse for all requests
    session = create_session()
    
    # Keep track of results
    fixed_count = 0
    failed_count = 0
    
    try:
        for image in tqdm(images, desc="Checking for stale files", colour='yellow'):
            image_id, url, page_url, local_path, author = image
            
            try:
                # Try to access the file to check if it's stale
                try:
                    with open(local_path, 'rb') as f:
                        f.seek(0)
                except (OSError, IOError) as e:
                    if 'Stale NFS file handle' in str(e):
                        logger.warning(f"Found stale NFS file handle: {local_path}")
                        
                        # Get fresh metadata
                        metadata = extract_metadata(page_url, session=session)
                        if not metadata:
                            logger.error(f"Failed to get metadata for {page_url}")
                            failed_count += 1
                            continue
                        
                        # Try to get high-res version
                        download_url = get_high_res_url(url, session=session)
                        
                        # Download the image
                        new_path, new_hash = download_image(download_url, author, session=session)
                        
                        if not new_path:
                            logger.error(f"Failed to download {url}")
                            failed_count += 1
                            continue
                        
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
                        fixed_count += 1
                        logger.info(f"Fixed stale file: {local_path}")
                        
            except Exception as e:
                logger.error(f"Error processing {url}: {e}")
                failed_count += 1
                continue
                
    finally:
        # Clean up the session
        session.close()
    
    # Print summary
    logger.info("\nFix Summary:")
    logger.info(f"Total images checked: {len(images)}")
    logger.info(f"Fixed stale files: {fixed_count}")
    logger.info(f"Failed to fix: {failed_count}")
    
    conn.close()

if __name__ == "__main__":
    fix_stale_files() 