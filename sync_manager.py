import sqlite3
import os
import hashlib
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SyncManager:
    def __init__(self, db_path: str):
        """Initialize the sync manager with the path to the indafoto database."""
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        self.cursor = self.conn.cursor()
        self._init_sync_tables()
        
    def _init_sync_tables(self):
        """Initialize the sync-related tables in the database."""
        # Create sync_operations table to track sync sessions
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id TEXT UNIQUE,
                start_time TEXT,
                end_time TEXT,
                status TEXT,
                source_db_path TEXT,
                total_files INTEGER,
                processed_files INTEGER,
                error_count INTEGER
            )
        """)
        
        # Create sync_files table to track individual files in sync operations
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id TEXT,
                image_id INTEGER,
                file_path TEXT,
                file_hash TEXT,
                status TEXT,
                error_message TEXT,
                FOREIGN KEY (operation_id) REFERENCES sync_operations(operation_id),
                FOREIGN KEY (image_id) REFERENCES images(id)
            )
        """)
        
        # Create sync_export_lists table to track export lists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_export_lists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_id TEXT,
                list_id TEXT UNIQUE,
                created_time TEXT,
                total_files INTEGER,
                processed_files INTEGER,
                status TEXT,
                FOREIGN KEY (operation_id) REFERENCES sync_operations(operation_id)
            )
        """)
        
        # Create sync_export_files table to track files in export lists
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_export_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                list_id TEXT,
                image_id INTEGER,
                export_path TEXT,
                status TEXT,
                FOREIGN KEY (list_id) REFERENCES sync_export_lists(list_id),
                FOREIGN KEY (image_id) REFERENCES images(id)
            )
        """)
        
        self.conn.commit()
        
    def analyze_remote_db(self, remote_db_path: str) -> Tuple[Set[str], Set[str]]:
        """
        Analyze a remote database and return sets of:
        1. Hashes we are missing
        2. URLs we are missing (based on metadata)
        
        Args:
            remote_db_path: Path to the remote database file
            
        Returns:
            Tuple of (missing_hashes, missing_urls)
        """
        missing_hashes = set()
        missing_urls = set()
        
        try:
            # Connect to remote database
            remote_conn = sqlite3.connect(remote_db_path)
            remote_cursor = remote_conn.cursor()
            
            # Get all hashes from remote database
            remote_cursor.execute("""
                SELECT sha256_hash, url, page_url 
                FROM images 
                WHERE sha256_hash IS NOT NULL
            """)
            
            # Get our local hashes for comparison
            self.cursor.execute("""
                SELECT sha256_hash, url, page_url 
                FROM images 
                WHERE sha256_hash IS NOT NULL
            """)
            
            local_hashes = {row[0] for row in self.cursor.fetchall()}
            local_urls = {row[1] for row in self.cursor.fetchall()}
            local_page_urls = {row[2] for row in self.cursor.fetchall()}
            
            # Compare hashes and URLs
            for hash_value, url, page_url in remote_cursor.fetchall():
                if hash_value not in local_hashes:
                    missing_hashes.add(hash_value)
                if url not in local_urls and page_url not in local_page_urls:
                    missing_urls.add(url)
            
            remote_conn.close()
            
        except Exception as e:
            logger.error(f"Error analyzing remote database: {e}")
            
        return missing_hashes, missing_urls
        
    def create_export_list(self, missing_hashes: Set[str], missing_urls: Set[str]) -> str:
        """
        Create an export list for files that need to be shared.
        
        Args:
            missing_hashes: Set of hashes that are missing
            missing_urls: Set of URLs that are missing
            
        Returns:
            The ID of the created export list
        """
        list_id = hashlib.sha256(str(datetime.now()).encode()).hexdigest()[:16]
        
        try:
            # Create export list record
            self.cursor.execute("""
                INSERT INTO sync_export_lists (
                    list_id, created_time, total_files, processed_files, status
                ) VALUES (?, ?, ?, ?, ?)
            """, (list_id, datetime.now().isoformat(), 
                  len(missing_hashes) + len(missing_urls), 0, 'pending'))
            
            # Add files to export list
            for hash_value in missing_hashes:
                self.cursor.execute("""
                    INSERT INTO sync_export_files (
                        list_id, image_id, export_path, status
                    ) VALUES (?, ?, ?, ?)
                """, (list_id, None, f"hash_{hash_value}", 'pending'))
                
            for url in missing_urls:
                self.cursor.execute("""
                    INSERT INTO sync_export_files (
                        list_id, image_id, export_path, status
                    ) VALUES (?, ?, ?, ?)
                """, (list_id, None, f"url_{url}", 'pending'))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error creating export list: {e}")
            self.conn.rollback()
            raise
            
        return list_id
        
    def process_imported_files(self, import_dir: str, list_id: str) -> bool:
        """
        Process imported files from a sync operation.
        
        Args:
            import_dir: Directory containing imported files
            list_id: ID of the export list being processed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import_dir_path = Path(import_dir)
            if not import_dir_path.exists():
                raise ValueError(f"Import directory {import_dir} does not exist")
                
            # Get files to process from the export list
            self.cursor.execute("""
                SELECT id, export_path, status 
                FROM sync_export_files 
                WHERE list_id = ?
            """, (list_id,))
            
            files_to_process = self.cursor.fetchall()
            
            for file_id, export_path, status in files_to_process:
                if status == 'completed':
                    continue
                    
                # Process the file based on its type (hash or URL)
                if export_path.startswith('hash_'):
                    hash_value = export_path[5:]
                    self._process_hash_file(import_dir_path, hash_value, file_id)
                elif export_path.startswith('url_'):
                    url = export_path[4:]
                    self._process_url_file(import_dir_path, url, file_id)
                    
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error processing imported files: {e}")
            self.conn.rollback()
            return False
            
    def _process_hash_file(self, import_dir: Path, hash_value: str, file_id: int):
        """Process a file imported by hash."""
        try:
            # Find the file in the import directory
            matching_files = list(import_dir.glob(f"*_{hash_value}.*"))
            if not matching_files:
                logger.warning(f"No file found for hash {hash_value}")
                self._update_file_status(file_id, 'error', 'File not found')
                return
                
            if len(matching_files) > 1:
                logger.warning(f"Multiple files found for hash {hash_value}")
                self._update_file_status(file_id, 'error', 'Multiple files found')
                return
                
            file_path = matching_files[0]
            
            # Verify the hash
            with open(file_path, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()
                
            if file_hash != hash_value:
                logger.error(f"Hash mismatch for file {file_path}")
                self._update_file_status(file_id, 'error', 'Hash mismatch')
                return
                
            # Get metadata from remote database
            self.cursor.execute("""
                SELECT * FROM images WHERE sha256_hash = ?
            """, (hash_value,))
            
            metadata = self.cursor.fetchone()
            if not metadata:
                logger.error(f"No metadata found for hash {hash_value}")
                self._update_file_status(file_id, 'error', 'No metadata found')
                return
                
            # Create the target directory structure
            target_dir = Path("images") / metadata[6]  # author field
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Move the file to its final location
            target_path = target_dir / file_path.name
            file_path.rename(target_path)
            
            # Update the database
            self.cursor.execute("""
                UPDATE images 
                SET local_path = ?
                WHERE sha256_hash = ?
            """, (str(target_path), hash_value))
            
            self._update_file_status(file_id, 'completed')
            
        except Exception as e:
            logger.error(f"Error processing hash file {hash_value}: {e}")
            self._update_file_status(file_id, 'error', str(e))
            
    def _process_url_file(self, import_dir: Path, url: str, file_id: int):
        """Process a file imported by URL."""
        try:
            # Find the file in the import directory
            matching_files = list(import_dir.glob(f"*_{url.replace('/', '_')}.*"))
            if not matching_files:
                logger.warning(f"No file found for URL {url}")
                self._update_file_status(file_id, 'error', 'File not found')
                return
                
            if len(matching_files) > 1:
                logger.warning(f"Multiple files found for URL {url}")
                self._update_file_status(file_id, 'error', 'Multiple files found')
                return
                
            file_path = matching_files[0]
            
            # Get metadata from remote database
            self.cursor.execute("""
                SELECT * FROM images WHERE url = ?
            """, (url,))
            
            metadata = self.cursor.fetchone()
            if not metadata:
                logger.error(f"No metadata found for URL {url}")
                self._update_file_status(file_id, 'error', 'No metadata found')
                return
                
            # Create the target directory structure
            target_dir = Path("images") / metadata[6]  # author field
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Move the file to its final location
            target_path = target_dir / file_path.name
            file_path.rename(target_path)
            
            # Update the database
            self.cursor.execute("""
                UPDATE images 
                SET local_path = ?
                WHERE url = ?
            """, (str(target_path), url))
            
            self._update_file_status(file_id, 'completed')
            
        except Exception as e:
            logger.error(f"Error processing URL file {url}: {e}")
            self._update_file_status(file_id, 'error', str(e))
            
    def _update_file_status(self, file_id: int, status: str, error_message: str = None):
        """Update the status of a file in the sync_export_files table."""
        try:
            self.cursor.execute("""
                UPDATE sync_export_files 
                SET status = ?, error_message = ?
                WHERE id = ?
            """, (status, error_message, file_id))
            
            # Update the export list status
            self.cursor.execute("""
                UPDATE sync_export_lists 
                SET processed_files = (
                    SELECT COUNT(*) 
                    FROM sync_export_files 
                    WHERE list_id = (
                        SELECT list_id 
                        FROM sync_export_files 
                        WHERE id = ?
                    ) AND status = 'completed'
                )
                WHERE list_id = (
                    SELECT list_id 
                    FROM sync_export_files 
                    WHERE id = ?
                )
            """, (file_id, file_id))
            
        except Exception as e:
            logger.error(f"Error updating file status: {e}")
            raise
            
    def cleanup_sync_files(self, list_id: str):
        """Clean up temporary files created during sync."""
        try:
            # Get all files from the export list
            self.cursor.execute("""
                SELECT export_path 
                FROM sync_export_files 
                WHERE list_id = ?
            """, (list_id,))
            
            files = self.cursor.fetchall()
            
            # Delete temporary files
            for export_path in files:
                if export_path.startswith('hash_'):
                    hash_value = export_path[5:]
                    for file in Path("temp_sync").glob(f"*_{hash_value}.*"):
                        file.unlink()
                elif export_path.startswith('url_'):
                    url = export_path[4:]
                    for file in Path("temp_sync").glob(f"*_{url.replace('/', '_')}.*"):
                        file.unlink()
                        
            # Update the export list status
            self.cursor.execute("""
                UPDATE sync_export_lists 
                SET status = 'completed'
                WHERE list_id = ?
            """, (list_id,))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error cleaning up sync files: {e}")
            self.conn.rollback()
            raise 