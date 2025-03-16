#!/usr/bin/env python3
import os
import sqlite3
import logging
from PIL import Image
import argparse
from pathlib import Path
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('verify_downloads.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_FILE = "indafoto.db"

def verify_jpeg(file_path):
    """Verify if a file is a valid JPEG image."""
    try:
        with Image.open(file_path) as img:
            if img.format != 'JPEG':
                return False, f"Not a JPEG file (format: {img.format})"
            # Force load the image to verify it's not corrupted
            img.load()
            return True, None
    except Exception as e:
        return False, str(e)

def delete_image_data(conn, cursor, image_id):
    """Delete all database entries related to an image."""
    try:
        # Delete from image_collections
        cursor.execute("DELETE FROM image_collections WHERE image_id = ?", (image_id,))
        
        # Delete from image_albums
        cursor.execute("DELETE FROM image_albums WHERE image_id = ?", (image_id,))
        
        # Delete from image_tags
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

def process_corrupted_file(conn, cursor, file_path, image_id, author, auto_confirm=None):
    """Process a corrupted file with user confirmation."""
    if auto_confirm is True:
        logger.info(f"Auto-confirming deletion for {file_path}")
        return True
    elif auto_confirm is False:
        logger.info(f"Auto-skipping deletion for {file_path}")
        return False
    
    while True:
        response = input(f"\nCorrupted file found: {file_path}\nAuthor: {author}\nDelete? [y]es/[n]o/[a]ll from this author/[q]uit: ").lower()
        
        if response == 'y':
            return True
        elif response == 'n':
            return False
        elif response == 'a':
            return 'all_author'
        elif response == 'q':
            raise KeyboardInterrupt
        else:
            print("Invalid response. Please try again.")

def verify_downloads():
    """Main function to verify all downloaded images."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Get all images grouped by author
    cursor.execute("""
        SELECT i.id, i.local_path, i.author, i.title
        FROM images i
        ORDER BY i.author, i.id
    """)
    
    images = cursor.fetchall()
    total_images = len(images)
    logger.info(f"Found {total_images} images to verify")
    
    corrupted_count = 0
    deleted_count = 0
    author_auto_confirm = defaultdict(lambda: None)
    
    try:
        for i, (image_id, file_path, author, title) in enumerate(images, 1):
            if not file_path or not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                continue
                
            logger.info(f"[{i}/{total_images}] Verifying {file_path}")
            is_valid, error = verify_jpeg(file_path)
            
            if not is_valid:
                corrupted_count += 1
                logger.error(f"Corrupted file: {file_path}")
                logger.error(f"Error: {error}")
                
                # Check if we have an auto-confirm setting for this author
                auto_confirm = author_auto_confirm[author]
                
                try:
                    should_delete = process_corrupted_file(conn, cursor, file_path, image_id, author, auto_confirm)
                    
                    if should_delete == 'all_author':
                        author_auto_confirm[author] = True
                        should_delete = True
                    
                    if should_delete:
                        # Delete file
                        try:
                            os.remove(file_path)
                            logger.info(f"Deleted file: {file_path}")
                        except Exception as e:
                            logger.error(f"Failed to delete file {file_path}: {e}")
                            continue
                        
                        # Delete database entries
                        if delete_image_data(conn, cursor, image_id):
                            deleted_count += 1
                            logger.info(f"Deleted database entries for image ID {image_id}")
                        
                except KeyboardInterrupt:
                    logger.info("\nVerification interrupted by user")
                    break
    
    except KeyboardInterrupt:
        logger.info("\nVerification interrupted by user")
    finally:
        # Print summary
        logger.info("\nVerification Summary:")
        logger.info(f"Total images checked: {i}")
        logger.info(f"Corrupted files found: {corrupted_count}")
        logger.info(f"Files deleted: {deleted_count}")
        
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Verify downloaded images and clean up corrupted ones.')
    args = parser.parse_args()
    
    try:
        verify_downloads()
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True) 