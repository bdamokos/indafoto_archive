import sqlite3
import shutil
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import time

class DatabaseSync:
    def __init__(self, local_db_path: str, remote_db_path: str):
        self.local_db_path = Path(local_db_path)
        self.remote_db_path = Path(remote_db_path)
        
    def sync_remote_db(self) -> bool:
        """
        Sync remote database to local temporary location and return path.
        Returns path to synced database if successful, None otherwise.
        """
        try:
            # Create temp directory for synced database
            temp_dir = Path("temp_sync")
            temp_dir.mkdir(exist_ok=True)
            
            # Copy remote database to temp location
            temp_db_path = temp_dir / "remote_indafoto.db"
            shutil.copy2(self.remote_db_path, temp_db_path)
            
            return str(temp_db_path)
            
        except Exception as e:
            print(f"Error syncing remote database: {e}")
            return None
            
    def get_remote_image_hashes(self, remote_db_path: str) -> Dict[str, str]:
        """Get all image hashes from remote database"""
        hashes = {}
        try:
            conn = sqlite3.connect(remote_db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, sha256_hash, url 
                FROM images 
                WHERE sha256_hash IS NOT NULL
            """)
            
            for row in cursor.fetchall():
                image_id, hash_value, url = row
                hashes[hash_value] = {
                    'id': image_id,
                    'url': url
                }
                
            conn.close()
            return hashes
            
        except Exception as e:
            print(f"Error getting remote hashes: {e}")
            return {}
            
    def get_remote_image_metadata(self, remote_db_path: str, image_id: int) -> Dict[str, Any]:
        """Get full metadata for an image from remote database"""
        try:
            conn = sqlite3.connect(remote_db_path)
            cursor = conn.cursor()
            
            # Get base image data
            cursor.execute("""
                SELECT * FROM images WHERE id = ?
            """, [image_id])
            
            columns = [description[0] for description in cursor.description]
            image_data = dict(zip(columns, cursor.fetchone()))
            
            # Get tags
            cursor.execute("""
                SELECT t.name 
                FROM tags t
                JOIN image_tags it ON t.id = it.tag_id
                WHERE it.image_id = ?
            """, [image_id])
            
            image_data['tags'] = [row[0] for row in cursor.fetchall()]
            
            # Get albums
            cursor.execute("""
                SELECT a.album_id, a.title, a.url, a.is_public
                FROM albums a
                JOIN image_albums ia ON a.id = ia.album_id
                WHERE ia.image_id = ?
            """, [image_id])
            
            image_data['albums'] = [
                {
                    'album_id': row[0],
                    'title': row[1],
                    'url': row[2],
                    'is_public': bool(row[3])
                }
                for row in cursor.fetchall()
            ]
            
            # Get collections
            cursor.execute("""
                SELECT c.collection_id, c.title, c.url, c.is_public
                FROM collections c
                JOIN image_collections ic ON c.id = ic.collection_id
                WHERE ic.image_id = ?
            """, [image_id])
            
            image_data['collections'] = [
                {
                    'collection_id': row[0],
                    'title': row[1],
                    'url': row[2],
                    'is_public': bool(row[3])
                }
                for row in cursor.fetchall()
            ]
            
            conn.close()
            return image_data
            
        except Exception as e:
            print(f"Error getting remote metadata: {e}")
            return None
            
    def cleanup_temp_db(self, temp_db_path: str):
        """Clean up temporary database file"""
        try:
            Path(temp_db_path).unlink()
        except Exception as e:
            print(f"Error cleaning up temp database: {e}")
            
    def get_last_sync_time(self, remote_source: str) -> float:
        """Get timestamp of last successful sync for a remote source"""
        try:
            conn = sqlite3.connect(self.local_db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT MAX(sync_date) 
                FROM sync_records 
                WHERE remote_source = ?
            """, [remote_source])
            
            result = cursor.fetchone()[0]
            conn.close()
            
            if result:
                return datetime.fromisoformat(result).timestamp()
            return 0
            
        except Exception as e:
            print(f"Error getting last sync time: {e}")
            return 0 