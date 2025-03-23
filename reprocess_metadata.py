#!/usr/bin/env python3
"""
This script reprocesses metadata for images that are missing tags, collections, and albums.
It fetches fresh metadata from the original page and updates the database with the new information.
"""

import sqlite3
import logging
import argparse
from tqdm import tqdm
from datetime import datetime
import os
import sys

# Import necessary functions from indafoto.py
from indafoto import (
    extract_metadata, 
    create_session, 
    check_for_updates, 
    init_db,
    calculate_file_hash,
    logger as indafoto_logger
)

# Configure Windows console for UTF-8 support if on Windows
if sys.platform.startswith('win'):
    try:
        import ctypes
        # Set console code page to UTF-8
        if ctypes.windll.kernel32.GetConsoleOutputCP() != 65001:
            ctypes.windll.kernel32.SetConsoleOutputCP(65001)
            ctypes.windll.kernel32.SetConsoleCP(65001)
        # For Python 3.6+ in Windows, ensure stdout/stderr use UTF-8
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
        sys.stderr.reconfigure(encoding='utf-8', errors='backslashreplace')
    except Exception as e:
        print(f"Warning: Could not configure Windows console for UTF-8: {e}")
        print("Some characters may not display correctly.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reprocess_metadata.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Leverage indafoto's DB_FILE constant by reusing it from the module
from indafoto import DB_FILE

def process_metadata_without_download(image_id, page_url, conn, cursor, session=None):
    """
    Process metadata for an image without redownloading the image file itself.
    
    This function reuses the metadata extraction and processing logic from indafoto.py
    but skips the download step.
    
    Args:
        image_id: Database ID of the image to process
        page_url: URL of the image page to fetch metadata from
        conn: SQLite database connection
        cursor: Database cursor
        session: Requests session to use (optional)
    
    Returns:
        tuple (success, stats) where:
            success: Boolean indicating if the operation was successful
            stats: Dictionary containing metadata statistics
    """
    # We expect a session to be provided - don't create a new one as it defeats the purpose of session pooling
    if not session:
        logger.warning("No session provided to process_metadata_without_download, creating a new one")
        session = create_session()
    
    tags_count = 0
    collections_count = 0
    albums_count = 0
    success = False
    
    try:
        # Get fresh metadata from the page
        metadata = extract_metadata(page_url, session=session)
        
        if not metadata:
            logger.error(f"Failed to extract metadata for {page_url}")
            return False, {"error": "Failed to extract metadata"}
        
        # Process collections
        if metadata.get('collections'):
            logger.info(f"Found {len(metadata['collections'])} collections for image:")
            for i, collection in enumerate(metadata.get('collections')):
                # if i == 0:  # Log only the first collection to avoid log spam
                #     logger.info(f"  - Collection: {collection['title']} (ID: {collection['id']})")
                
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO collections (collection_id, title, url, is_public)
                        VALUES (?, ?, ?, ?)
                    """, (collection['id'], collection['title'], collection['url'], collection['is_public']))
                    
                    cursor.execute("SELECT id FROM collections WHERE collection_id = ?", (collection['id'],))
                    collection_db_id = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        INSERT OR IGNORE INTO image_collections (image_id, collection_id)
                        VALUES (?, ?)
                    """, (image_id, collection_db_id))
                    
                    collections_count += 1
                except Exception as e:
                    logger.error(f"Error processing collection {collection.get('id')}: {e}")
        
        # Process albums
        if metadata.get('albums'):
            logger.info(f"Found {len(metadata['albums'])} albums for image")
            for i, album in enumerate(metadata.get('albums')):
                # if i == 0:  # Log only the first album to avoid log spam
                #     logger.info(f"  - Album: {album['title']} (ID: {album['id']})")
                
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO albums (album_id, title, url, is_public)
                        VALUES (?, ?, ?, ?)
                    """, (album['id'], album['title'], album['url'], album['is_public']))
                    
                    cursor.execute("SELECT id FROM albums WHERE album_id = ?", (album['id'],))
                    album_db_id = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        INSERT OR IGNORE INTO image_albums (image_id, album_id)
                        VALUES (?, ?)
                    """, (image_id, album_db_id))
                    
                    albums_count += 1
                except Exception as e:
                    logger.error(f"Error processing album {album.get('id')}: {e}")
        
        # Process tags
        if metadata.get('tags'):
            logger.info(f"Found {len(metadata['tags'])} tags for image")
            for i, tag in enumerate(metadata.get('tags')):
                # if i == 0:  # Log only the first tag to avoid log spam
                #     logger.info(f"  - Tag: {tag['name']} (Count: {tag['count']})")
                
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO tags (name, count)
                        VALUES (?, ?)
                    """, (tag['name'], tag['count']))
                    
                    cursor.execute("SELECT id FROM tags WHERE name = ?", (tag['name'],))
                    tag_db_id = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        INSERT OR IGNORE INTO image_tags (image_id, tag_id)
                        VALUES (?, ?)
                    """, (image_id, tag_db_id))
                    
                    tags_count += 1
                except Exception as e:
                    logger.error(f"Error processing tag {tag.get('name')}: {e}")
        
        # Update the last modified timestamp for this image's metadata
        cursor.execute("""
            UPDATE images
            SET metadata_updated = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), image_id))
        
        conn.commit()
        success = True
        
    except Exception as e:
        logger.error(f"Error processing image ID {image_id}: {e}")
        success = False
    
    return success, {
        "tags_count": tags_count,
        "collections_count": collections_count,
        "albums_count": albums_count
    }

def reprocess_missing_metadata(batch_size=100, concurrent_sessions=5):
    """
    Reprocess metadata for images that don't have tags, collections, or albums.
    
    Args:
        batch_size: Number of images to process in each batch to avoid memory issues
        concurrent_sessions: Number of concurrent HTTP sessions to use
    """
    # Use the init_db function from indafoto.py to ensure all tables exist
    conn = init_db()
    cursor = conn.cursor()
    
    # Add metadata_updated column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE images ADD COLUMN metadata_updated TEXT")
        logger.info("Added metadata_updated column to images table")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    # Find images that don't have any entries in any of the junction tables
    # (no tags, no collections, no albums)
    cursor.execute("""
        SELECT i.id, i.page_url, i.title, i.author
        FROM images i
        WHERE NOT EXISTS (
            SELECT 1 FROM image_tags WHERE image_id = i.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM image_collections WHERE image_id = i.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM image_albums WHERE image_id = i.id
        )
    """)
    
    images = cursor.fetchall()
    if not images:
        logger.info("No images found without metadata")
        conn.close()
        return
    
    total_images = len(images)
    logger.info(f"Found {total_images} images without tags, collections, or albums")
    
    # Create a session pool for better HTTP connection reuse
    logger.info(f"Creating a pool of {concurrent_sessions} HTTP sessions")
    session_pool = []
    for i in range(concurrent_sessions):
        session = create_session()
        session_pool.append(session)
    
    # Track results
    success_count = 0
    failed_count = 0
    total_tags = 0
    total_collections = 0
    total_albums = 0
    
    try:
        for i in range(0, total_images, batch_size):
            batch = images[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(total_images + batch_size - 1)//batch_size}")
            
            for idx, image in enumerate(tqdm(batch, desc="Reprocessing metadata", colour='green')):
                image_id, page_url, title, author = image
                
                if not page_url:
                    logger.warning(f"Image ID {image_id} has no page URL, skipping")
                    failed_count += 1
                    continue
                
                # Use session from pool in a round-robin fashion
                session = session_pool[idx % concurrent_sessions]
                
                logger.info(f"Fetching metadata for image ID: {image_id})")
                # Process metadata for this image using our helper function
                success, stats = process_metadata_without_download(image_id, page_url, conn, cursor, session)
                
                if success:
                    success_count += 1
                    total_tags += stats.get("tags_count", 0)
                    total_collections += stats.get("collections_count", 0)
                    total_albums += stats.get("albums_count", 0)
                else:
                    failed_count += 1
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    finally:
        # Clean up all sessions
        for session in session_pool:
            try:
                session.close()
            except:
                pass
    
    # Print summary
    logger.info("\nMetadata Reprocessing Summary:")
    logger.info(f"Total images processed: {success_count + failed_count}")
    logger.info(f"Successfully updated: {success_count}")
    logger.info(f"Failed: {failed_count}")
    logger.info(f"Collections added: {total_collections}")
    logger.info(f"Albums added: {total_albums}")
    logger.info(f"Tags added: {total_tags}")
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Indafoto Metadata Reprocessing Tool')
    parser.add_argument('--no-update-check', action='store_true',
                        help='Skip checking for updates')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of images to process in each batch')
    parser.add_argument('--concurrent-sessions', type=int, default=5,
                        help='Number of concurrent HTTP sessions to use')
    args = parser.parse_args()
    
    try:
        # Check for updates unless explicitly disabled
        if not args.no_update_check:
            check_for_updates(__file__)
            
        reprocess_missing_metadata(batch_size=args.batch_size, concurrent_sessions=args.concurrent_sessions)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True) 