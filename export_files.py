import sqlite3
import os
import shutil
from pathlib import Path
from typing import List, Dict
import logging
from sync_manager import SyncManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileExporter:
    def __init__(self, db_path: str):
        """Initialize the file exporter with the path to the indafoto database."""
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        self.cursor = self.conn.cursor()
        self.sync_manager = SyncManager(db_path)
        
    def prepare_export(self, list_id: str, output_dir: str, dry_run: bool = False) -> bool:
        """
        Prepare files for export based on an export list.
        
        Args:
            list_id: ID of the export list
            output_dir: Directory to store exported files
            dry_run: If True, only prepare export list without copying files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            output_path = Path(output_dir)
            if not dry_run:
                output_path.mkdir(parents=True, exist_ok=True)
            
            # Get files to export
            self.cursor.execute("""
                SELECT id, export_path, status 
                FROM sync_export_files 
                WHERE list_id = ? AND status = 'pending'
            """, (list_id,))
            
            files_to_export = self.cursor.fetchall()
            
            for file_id, export_path, status in files_to_export:
                if export_path.startswith('hash_'):
                    hash_value = export_path[5:]
                    success = self._export_by_hash(hash_value, output_path, file_id, dry_run)
                elif export_path.startswith('url_'):
                    url = export_path[4:]
                    success = self._export_by_url(url, output_path, file_id, dry_run)
                    
                if not success:
                    logger.error(f"Failed to export {export_path}")
                    
            return True
            
        except Exception as e:
            logger.error(f"Error preparing export: {e}")
            return False
            
    def _export_by_hash(self, hash_value: str, output_dir: Path, file_id: int, dry_run: bool = False) -> bool:
        """Export a file by its hash value."""
        try:
            # Get file information from database
            self.cursor.execute("""
                SELECT local_path, url 
                FROM images 
                WHERE sha256_hash = ?
            """, (hash_value,))
            
            result = self.cursor.fetchone()
            if not result:
                logger.error(f"No file found for hash {hash_value}")
                self._update_export_status(file_id, 'error', 'File not found in database')
                return False
                
            local_path, url = result
            if not local_path:
                logger.error(f"No local path for hash {hash_value}")
                self._update_export_status(file_id, 'error', 'No local path')
                return False
                
            # Check if source file exists
            source_path = Path(local_path)
            if not source_path.exists():
                logger.error(f"Source file not found: {local_path}")
                self._update_export_status(file_id, 'error', 'Source file not found')
                return False
                
            # Create a unique filename for export
            export_name = f"{source_path.stem}_{hash_value}{source_path.suffix}"
            target_path = output_dir / export_name
            
            if dry_run:
                logger.info(f"Would export {source_path} to {target_path}")
                self._update_export_status(file_id, 'pending', 'Dry run - file would be exported')
                return True
                
            # Copy file to export directory
            shutil.copy2(source_path, target_path)
            
            # Verify the copy
            if not target_path.exists():
                logger.error(f"Failed to copy file to {target_path}")
                self._update_export_status(file_id, 'error', 'Copy failed')
                return False
                
            self._update_export_status(file_id, 'completed')
            return True
            
        except Exception as e:
            logger.error(f"Error exporting file by hash {hash_value}: {e}")
            self._update_export_status(file_id, 'error', str(e))
            return False
            
    def _export_by_url(self, url: str, output_dir: Path, file_id: int, dry_run: bool = False) -> bool:
        """Export a file by its URL."""
        try:
            # Get file information from database
            self.cursor.execute("""
                SELECT local_path, sha256_hash 
                FROM images 
                WHERE url = ?
            """, (url,))
            
            result = self.cursor.fetchall()
            if not result:
                logger.error(f"No file found for URL {url}")
                self._update_export_status(file_id, 'error', 'File not found in database')
                return False
                
            # Handle multiple matches (should be rare)
            if len(result) > 1:
                logger.warning(f"Multiple files found for URL {url}, using first match")
                
            local_path, hash_value = result[0]
            if not local_path:
                logger.error(f"No local path for URL {url}")
                self._update_export_status(file_id, 'error', 'No local path')
                return False
                
            # Check if source file exists
            source_path = Path(local_path)
            if not source_path.exists():
                logger.error(f"Source file not found: {local_path}")
                self._update_export_status(file_id, 'error', 'Source file not found')
                return False
                
            # Create a unique filename for export
            export_name = f"{source_path.stem}_{url.replace('/', '_')}{source_path.suffix}"
            target_path = output_dir / export_name
            
            if dry_run:
                logger.info(f"Would export {source_path} to {target_path}")
                self._update_export_status(file_id, 'pending', 'Dry run - file would be exported')
                return True
                
            # Copy file to export directory
            shutil.copy2(source_path, target_path)
            
            # Verify the copy
            if not target_path.exists():
                logger.error(f"Failed to copy file to {target_path}")
                self._update_export_status(file_id, 'error', 'Copy failed')
                return False
                
            self._update_export_status(file_id, 'completed')
            return True
            
        except Exception as e:
            logger.error(f"Error exporting file by URL {url}: {e}")
            self._update_export_status(file_id, 'error', str(e))
            return False
            
    def _update_export_status(self, file_id: int, status: str, error_message: str = None):
        """Update the status of a file in the sync_export_files table."""
        try:
            self.cursor.execute("""
                UPDATE sync_export_files 
                SET status = ?, error_message = ?
                WHERE id = ?
            """, (status, error_message, file_id))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error updating export status: {e}")
            self.conn.rollback()
            raise

def main():
    """Main function to handle command line arguments and run the export."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Export files for sync')
    parser.add_argument('--db', required=True, help='Path to indafoto database')
    parser.add_argument('--list-id', required=True, help='ID of the export list')
    parser.add_argument('--output-dir', required=True, help='Directory to store exported files')
    parser.add_argument('--dry-run', action='store_true', help='Perform dry run without copying files')
    
    args = parser.parse_args()
    
    exporter = FileExporter(args.db)
    success = exporter.prepare_export(args.list_id, args.output_dir, args.dry_run)
    
    if success:
        if args.dry_run:
            logger.info("Dry run completed successfully - no files were copied")
        else:
            logger.info(f"Export completed successfully to {args.output_dir}")
    else:
        logger.error("Export failed")
        exit(1)

if __name__ == '__main__':
    main() 