import os
import argparse
from pathlib import Path
from typing import Dict, Any
import json
from tqdm import tqdm
import time

from .database import init_db
from .sync_manager import SyncManager
from .syncthing_manager import SyncthingManager
from .db_sync import DatabaseSync

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON file"""
    with open(config_path) as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description='Indafoto Archive Sync Tool')
    parser.add_argument('--config', required=True, help='Path to config file')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Initialize database
    db_session = init_db(config['database_path'])
    
    # Initialize managers
    sync_manager = SyncManager(db_session, config['image_root'])
    syncthing = SyncthingManager(
        api_key=config['syncthing_api_key'],
        host=config['syncthing_host'],
        port=config['syncthing_port']
    )
    
    # Process each remote source
    for source in config['remote_sources']:
        print(f"\nProcessing remote source: {source['name']}")
        
        # Initialize database sync
        db_sync = DatabaseSync(
            local_db_path=config['database_path'],
            remote_db_path=source['remote_db_path']
        )
        
        # Check if we need to sync database
        last_sync = db_sync.get_last_sync_time(source['name'])
        current_time = time.time()
        
        if current_time - last_sync < source['remote_db_sync_interval']:
            print(f"Database sync not needed (last sync: {last_sync})")
        else:
            print("Syncing remote database...")
            temp_db_path = db_sync.sync_remote_db()
            if not temp_db_path:
                print("Failed to sync remote database")
                continue
                
            try:
                # Get all remote image hashes
                remote_hashes = db_sync.get_remote_image_hashes(temp_db_path)
                print(f"Found {len(remote_hashes)} images in remote database")
                
                # Add folder to Syncthing if not already added
                if not syncthing.get_folder_status(source['folder_id']):
                    if not syncthing.add_folder(
                        folder_id=source['folder_id'],
                        path=source['local_path'],
                        type="receiveonly"
                    ):
                        print(f"Failed to add folder {source['folder_id']}")
                        continue
                        
                # Wait for initial sync
                print("Waiting for initial sync...")
                if not syncthing.wait_for_sync(source['folder_id']):
                    print("Sync timed out")
                    continue
                    
                # Get list of files
                files = syncthing.get_folder_files(source['folder_id'])
                
                # Process each file
                for file in tqdm(files, desc="Processing files"):
                    # Skip if already processed
                    if sync_manager.get_sync_progress(source['name'])['completed'] > 0:
                        print(f"Files from {source['name']} already processed")
                        break
                        
                    # Get file hash
                    file_path = Path(source['local_path']) / file['name']
                    file_hash = sync_manager.calculate_file_hash(file_path)
                    
                    # Check if we have this image in remote database
                    if file_hash not in remote_hashes:
                        print(f"Image {file['name']} not found in remote database")
                        continue
                        
                    # Get metadata from remote database
                    image_id = remote_hashes[file_hash]['id']
                    metadata = db_sync.get_remote_image_metadata(temp_db_path, image_id)
                    if not metadata:
                        print(f"Failed to get metadata for {file['name']}")
                        continue
                        
                    # Process the image
                    success = sync_manager.process_synced_image(
                        remote_path=str(file_path),
                        remote_source=source['name'],
                        metadata=metadata
                    )
                    
                    if not success:
                        print(f"Failed to process {file['name']}")
                        
                # Show final progress
                progress = sync_manager.get_sync_progress(source['name'])
                print(f"\nSync progress for {source['name']}:")
                print(f"Total: {progress['total']}")
                print(f"Completed: {progress['completed']}")
                print(f"Failed: {progress['failed']}")
                print(f"Pending: {progress['pending']}")
                
                # Remove folder from Syncthing
                if syncthing.remove_folder(source['folder_id']):
                    print(f"Removed folder {source['folder_id']} from Syncthing")
                else:
                    print(f"Failed to remove folder {source['folder_id']}")
                    
            finally:
                # Clean up temporary database
                db_sync.cleanup_temp_db(temp_db_path)

if __name__ == "__main__":
    main() 