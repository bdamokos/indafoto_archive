import os
import hashlib
import shutil
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from .database import Image, Tag, Album, Collection, SyncRecord

class SyncManager:
    def __init__(self, db_session: Session, image_root: str):
        self.db = db_session
        self.image_root = Path(image_root)
        
    def calculate_file_hash(self, filepath: Path) -> str:
        """Calculate SHA256 hash of a file"""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
        
    def get_image_by_hash(self, sha256_hash: str) -> Optional[Image]:
        """Find image by SHA256 hash"""
        return self.db.query(Image).filter(Image.sha256_hash == sha256_hash).first()
        
    def get_image_by_url(self, url: str) -> Optional[Image]:
        """Find image by URL"""
        return self.db.query(Image).filter(Image.url == url).first()
        
    def process_synced_image(self, 
                           remote_path: str,
                           remote_source: str,
                           metadata: Dict[str, Any]) -> bool:
        """
        Process an image that was synced via Syncthing.
        Returns True if successful, False otherwise.
        """
        try:
            # Find the synced file
            synced_file = Path(remote_path)
            if not synced_file.exists():
                raise FileNotFoundError(f"Synced file not found: {remote_path}")
                
            # Calculate hash
            file_hash = self.calculate_file_hash(synced_file)
            
            # Check if we already have this image
            existing_image = self.get_image_by_hash(file_hash)
            if existing_image:
                # Create sync record for existing image
                sync_record = SyncRecord(
                    image_id=existing_image.id,
                    remote_source=remote_source,
                    remote_path=remote_path,
                    status='success',
                    sync_date=datetime.utcnow()
                )
                self.db.add(sync_record)
                self.db.commit()
                return True
                
            # Create new image record
            image = Image(
                url=metadata['url'],
                sha256_hash=file_hash,
                title=metadata['title'],
                author=metadata['author'],
                author_url=metadata['author_url'],
                license=metadata['license'],
                camera_make=metadata['camera_make'],
                camera_model=metadata['camera_model'],
                focal_length=metadata['focal_length'],
                aperture=metadata['aperture'],
                shutter_speed=metadata['shutter_speed'],
                taken_date=metadata['taken_date'],
                page_url=metadata['page_url'],
                upload_date=metadata['upload_date'],
                description=metadata['description'],
                metadata_updated=metadata['metadata_updated']
            )
            
            # Create author directory structure
            author_dir = self.image_root / image.author
            author_dir.mkdir(exist_ok=True)
            
            # Calculate subfolder (same logic as indafoto.py)
            file_count = len(list(author_dir.glob('*')))
            subfolder = str(file_count // 1000 * 1000)
            subfolder_path = author_dir / subfolder
            subfolder_path.mkdir(exist_ok=True)
            
            # Move file to final location
            final_path = subfolder_path / f"{file_hash}.jpg"
            shutil.move(str(synced_file), str(final_path))
            
            # Verify hash after move
            if self.calculate_file_hash(final_path) != file_hash:
                raise ValueError("Hash verification failed after move")
                
            # Set local path
            image.local_path = str(final_path.relative_to(self.image_root))
            
            # Add to database
            self.db.add(image)
            self.db.flush()  # Get image ID
            
            # Process tags
            for tag_name in metadata.get('tags', []):
                tag = self.db.query(Tag).filter(Tag.name == tag_name).first()
                if not tag:
                    tag = Tag(name=tag_name)
                    self.db.add(tag)
                    self.db.flush()
                image.tags.append(tag)
                
            # Process albums
            for album_data in metadata.get('albums', []):
                album = self.db.query(Album).filter(
                    Album.album_id == album_data['album_id']
                ).first()
                if not album:
                    album = Album(**album_data)
                    self.db.add(album)
                    self.db.flush()
                image.albums.append(album)
                
            # Process collections
            for collection_data in metadata.get('collections', []):
                collection = self.db.query(Collection).filter(
                    Collection.collection_id == collection_data['collection_id']
                ).first()
                if not collection:
                    collection = Collection(**collection_data)
                    self.db.add(collection)
                    self.db.flush()
                image.collections.append(collection)
                
            # Create sync record
            sync_record = SyncRecord(
                image_id=image.id,
                remote_source=remote_source,
                remote_path=remote_path,
                status='success',
                sync_date=datetime.utcnow()
            )
            self.db.add(sync_record)
            
            # Commit all changes
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            # Create failed sync record if we have an image ID
            if 'image' in locals():
                sync_record = SyncRecord(
                    image_id=image.id,
                    remote_source=remote_source,
                    remote_path=remote_path,
                    status='failed',
                    error_message=str(e),
                    sync_date=datetime.utcnow()
                )
                self.db.add(sync_record)
                self.db.commit()
            return False
            
    def get_sync_progress(self, remote_source: str) -> Dict[str, Any]:
        """Get sync progress for a remote source"""
        total = self.db.query(SyncRecord).filter(
            SyncRecord.remote_source == remote_source
        ).count()
        
        completed = self.db.query(SyncRecord).filter(
            SyncRecord.remote_source == remote_source,
            SyncRecord.status == 'success'
        ).count()
        
        failed = self.db.query(SyncRecord).filter(
            SyncRecord.remote_source == remote_source,
            SyncRecord.status == 'failed'
        ).count()
        
        return {
            'total': total,
            'completed': completed,
            'failed': failed,
            'pending': total - completed - failed
        } 