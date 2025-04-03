import sqlite3
import os
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
import logging
from sync_manager import SyncManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SyncAnalyzer:
    def __init__(self, db_path: str):
        """Initialize the sync analyzer with the path to the indafoto database."""
        self.db_path = Path(db_path)
        self.conn = sqlite3.connect(str(self.db_path))
        self.cursor = self.conn.cursor()
        self.sync_manager = SyncManager(db_path)
        
    def analyze_remote_db(self, remote_db_path: str, dry_run: bool = False) -> Tuple[str, Dict[str, int]]:
        """
        Analyze a remote database and create an export list.
        
        Args:
            remote_db_path: Path to the remote database file
            dry_run: If True, only analyze without creating export list
            
        Returns:
            Tuple of (list_id, stats) where stats is a dictionary of statistics
        """
        try:
            # Analyze the remote database
            missing_hashes, missing_urls = self.sync_manager.analyze_remote_db(remote_db_path)
            
            # Calculate statistics
            stats = {
                'total_files': len(missing_hashes) + len(missing_urls),
                'hash_files': len(missing_hashes),
                'url_files': len(missing_urls),
                'completed': 0,
                'failed': 0
            }
            
            if dry_run:
                logger.info("Dry run - would create export list with:")
                logger.info(f"- Missing hashes: {len(missing_hashes)}")
                logger.info(f"- Missing URLs: {len(missing_urls)}")
                return None, stats
            
            # Create export list
            list_id = self.sync_manager.create_export_list(missing_hashes, missing_urls)
            
            # Log results
            logger.info(f"Analysis complete:")
            logger.info(f"- Missing hashes: {len(missing_hashes)}")
            logger.info(f"- Missing URLs: {len(missing_urls)}")
            logger.info(f"- Export list ID: {list_id}")
            
            return list_id, stats
            
        except Exception as e:
            logger.error(f"Error analyzing remote database: {e}")
            raise
            
    def get_export_list_stats(self, list_id: str) -> Dict[str, int]:
        """
        Get statistics about an export list.
        
        Args:
            list_id: ID of the export list
            
        Returns:
            Dictionary with counts of different file types
        """
        try:
            # Get statistics
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN export_path LIKE 'hash_%' THEN 1 ELSE 0 END) as hash_files,
                    SUM(CASE WHEN export_path LIKE 'url_%' THEN 1 ELSE 0 END) as url_files,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failed
                FROM sync_export_files 
                WHERE list_id = ?
            """, (list_id,))
            
            result = self.cursor.fetchone()
            if not result:
                return {
                    'total': 0,
                    'hash_files': 0,
                    'url_files': 0,
                    'completed': 0,
                    'failed': 0
                }
                
            total, hash_files, url_files, completed, failed = result
            return {
                'total': total,
                'hash_files': hash_files,
                'url_files': url_files,
                'completed': completed,
                'failed': failed
            }
            
        except Exception as e:
            logger.error(f"Error getting export list statistics: {e}")
            return {
                'total': 0,
                'hash_files': 0,
                'url_files': 0,
                'completed': 0,
                'failed': 0
            }
            
    def get_export_list_details(self, list_id: str) -> List[Dict]:
        """
        Get detailed information about files in an export list.
        
        Args:
            list_id: ID of the export list
            
        Returns:
            List of dictionaries containing file details
        """
        try:
            self.cursor.execute("""
                SELECT id, export_path, status, error_message 
                FROM sync_export_files 
                WHERE list_id = ?
            """, (list_id,))
            
            files = []
            for file_id, export_path, status, error_message in self.cursor.fetchall():
                files.append({
                    'id': file_id,
                    'export_path': export_path,
                    'status': status,
                    'error_message': error_message
                })
                
            return files
            
        except Exception as e:
            logger.error(f"Error getting export list details: {e}")
            return []

def main():
    """Main function to handle command line arguments and run the analysis."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze remote database for sync')
    parser.add_argument('--db', required=True, help='Path to local indafoto database')
    parser.add_argument('--remote-db', help='Path to remote indafoto database')
    parser.add_argument('--list-id', help='ID of export list to analyze (optional)')
    parser.add_argument('--dry-run', action='store_true', help='Perform dry run without making changes')
    
    args = parser.parse_args()
    
    analyzer = SyncAnalyzer(args.db)
    
    if args.list_id:
        # Analyze existing export list
        stats = analyzer.get_export_list_stats(args.list_id)
        print(f"Export list statistics:")
        print(f"Total files: {stats['total']}")
        print(f"Hash-based files: {stats['hash_files']}")
        print(f"URL-based files: {stats['url_files']}")
        print(f"Completed: {stats['completed']}")
        print(f"Failed: {stats['failed']}")
        
        if stats['failed'] > 0:
            files = analyzer.get_export_list_details(args.list_id)
            print("\nFailed files:")
            for file in files:
                if file['status'] == 'error':
                    print(f"- {file['export_path']}: {file['error_message']}")
    else:
        if not args.remote_db:
            parser.error("--remote-db is required when not using --list-id")
            
        # Analyze remote database and create new export list
        list_id, stats = analyzer.analyze_remote_db(args.remote_db, args.dry_run)
        
        if args.dry_run:
            print(f"Dry run results:")
            print(f"Total files to export: {stats['total']}")
            print(f"Hash-based files: {stats['hash_files']}")
            print(f"URL-based files: {stats['url_files']}")
        else:
            print(f"Created export list with ID: {list_id}")

if __name__ == '__main__':
    main() 