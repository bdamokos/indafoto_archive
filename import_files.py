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

class FileImporter:
    def __init__(self, db_path: str):
        """Initialize the file importer with the path to the indafoto database."""
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        self.cursor = self.conn.cursor()
        self.sync_manager = SyncManager(db_path)
        
    def import_files(self, import_dir: str, list_id: str, dry_run: bool = False) -> bool:
        """
        Import files from a sync operation.
        
        Args:
            import_dir: Directory containing imported files
            list_id: ID of the export list being processed
            dry_run: If True, only verify files without importing
            
        Returns:
            True if successful, False otherwise
        """
        try:
            import_path = Path(import_dir)
            if not import_path.exists():
                logger.error(f"Import directory {import_dir} does not exist")
                return False
                
            if dry_run:
                logger.info("Dry run - verifying files without importing")
                return self._verify_import_files(import_path, list_id)
                
            # Process the files using the sync manager
            success = self.sync_manager.process_imported_files(import_dir, list_id)
            
            if success:
                # Clean up temporary files
                self.sync_manager.cleanup_sync_files(list_id)
                
            return success
            
        except Exception as e:
            logger.error(f"Error importing files: {e}")
            return False
            
    def _verify_import_files(self, import_dir: Path, list_id: str) -> bool:
        """Verify imported files without importing them."""
        try:
            # Get files to verify from the export list
            self.cursor.execute("""
                SELECT id, export_path 
                FROM sync_export_files 
                WHERE list_id = ?
            """, (list_id,))
            
            files_to_verify = self.cursor.fetchall()
            success = True
            
            for file_id, export_path in files_to_verify:
                if export_path.startswith('hash_'):
                    hash_value = export_path[5:]
                    if not self._verify_hash_file(import_dir, hash_value, file_id):
                        success = False
                elif export_path.startswith('url_'):
                    url = export_path[4:]
                    if not self._verify_url_file(import_dir, url, file_id):
                        success = False
                        
            return success
            
        except Exception as e:
            logger.error(f"Error verifying import files: {e}")
            return False
            
    def _verify_hash_file(self, import_dir: Path, hash_value: str, file_id: int) -> bool:
        """Verify a file imported by hash."""
        try:
            # Find the file in the import directory
            matching_files = list(import_dir.glob(f"*_{hash_value}.*"))
            if not matching_files:
                logger.warning(f"No file found for hash {hash_value}")
                self._update_file_status(file_id, 'error', 'File not found')
                return False
                
            if len(matching_files) > 1:
                logger.warning(f"Multiple files found for hash {hash_value}")
                self._update_file_status(file_id, 'error', 'Multiple files found')
                return False
                
            file_path = matching_files[0]
            logger.info(f"Found file for hash {hash_value}: {file_path}")
            self._update_file_status(file_id, 'pending', 'Dry run - file would be imported')
            return True
            
        except Exception as e:
            logger.error(f"Error verifying hash file {hash_value}: {e}")
            self._update_file_status(file_id, 'error', str(e))
            return False
            
    def _verify_url_file(self, import_dir: Path, url: str, file_id: int) -> bool:
        """Verify a file imported by URL."""
        try:
            # Find the file in the import directory
            matching_files = list(import_dir.glob(f"*_{url.replace('/', '_')}.*"))
            if not matching_files:
                logger.warning(f"No file found for URL {url}")
                self._update_file_status(file_id, 'error', 'File not found')
                return False
                
            if len(matching_files) > 1:
                logger.warning(f"Multiple files found for URL {url}")
                self._update_file_status(file_id, 'error', 'Multiple files found')
                return False
                
            file_path = matching_files[0]
            logger.info(f"Found file for URL {url}: {file_path}")
            self._update_file_status(file_id, 'pending', 'Dry run - file would be imported')
            return True
            
        except Exception as e:
            logger.error(f"Error verifying URL file {url}: {e}")
            self._update_file_status(file_id, 'error', str(e))
            return False
            
    def verify_import(self, list_id: str) -> Dict[str, int]:
        """
        Verify the results of an import operation.
        
        Args:
            list_id: ID of the export list being processed
            
        Returns:
            Dictionary with counts of successful and failed imports
        """
        try:
            # Get import statistics
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed
                FROM sync_export_files 
                WHERE list_id = ?
            """, (list_id,))
            
            result = self.cursor.fetchone()
            if not result:
                return {'total': 0, 'completed': 0, 'failed': 0}
                
            total, completed, failed = result
            return {
                'total': total,
                'completed': completed,
                'failed': failed
            }
            
        except Exception as e:
            logger.error(f"Error verifying import: {e}")
            return {'total': 0, 'completed': 0, 'failed': 0}
            
    def get_failed_imports(self, list_id: str) -> List[Dict]:
        """
        Get details of failed imports.
        
        Args:
            list_id: ID of the export list being processed
            
        Returns:
            List of dictionaries containing details of failed imports
        """
        try:
            self.cursor.execute("""
                SELECT export_path, error_message 
                FROM sync_export_files 
                WHERE list_id = ? AND status = 'error'
            """, (list_id,))
            
            failed_imports = []
            for export_path, error_message in self.cursor.fetchall():
                failed_imports.append({
                    'export_path': export_path,
                    'error_message': error_message
                })
                
            return failed_imports
            
        except Exception as e:
            logger.error(f"Error getting failed imports: {e}")
            return []
            
    def _update_file_status(self, file_id: int, status: str, error_message: str = None):
        """Update the status of a file in the sync_export_files table."""
        try:
            self.cursor.execute("""
                UPDATE sync_export_files 
                SET status = ?, error_message = ?
                WHERE id = ?
            """, (status, error_message, file_id))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error updating file status: {e}")
            self.conn.rollback()
            raise

def main():
    """Main function to handle command line arguments and run the import."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import files from sync')
    parser.add_argument('--db', required=True, help='Path to indafoto database')
    parser.add_argument('--list-id', required=True, help='ID of the export list')
    parser.add_argument('--import-dir', help='Directory containing imported files')
    parser.add_argument('--verify', action='store_true', help='Verify import results')
    parser.add_argument('--dry-run', action='store_true', help='Perform dry run without importing')
    
    args = parser.parse_args()
    
    importer = FileImporter(args.db)
    
    if args.verify:
        # Just verify the import results
        stats = importer.verify_import(args.list_id)
        print(f"Import statistics:")
        print(f"Total files: {stats['total']}")
        print(f"Successfully imported: {stats['completed']}")
        print(f"Failed imports: {stats['failed']}")
        
        if stats['failed'] > 0:
            failed_imports = importer.get_failed_imports(args.list_id)
            print("\nFailed imports:")
            for failed in failed_imports:
                print(f"- {failed['export_path']}: {failed['error_message']}")
    else:
        if not args.import_dir:
            parser.error("--import-dir is required when not using --verify")
            
        # Run the import or dry run
        success = importer.import_files(args.import_dir, args.list_id, args.dry_run)
        
        if success:
            if args.dry_run:
                logger.info("Dry run completed successfully - no files were imported")
            else:
                logger.info("Import completed successfully")
        else:
            logger.error("Import failed")
            exit(1)

if __name__ == '__main__':
    main() 