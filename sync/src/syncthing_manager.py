from typing import Optional, Dict, Any
import os
from pathlib import Path
from syncthing import Syncthing
import time

class SyncthingManager:
    def __init__(self, api_key: str, host: str = "localhost", port: int = 8384):
        self.client = Syncthing(api_key, host=host, port=port)
        
    def wait_for_sync(self, folder_id: str, timeout: int = 3600) -> bool:
        """
        Wait for a folder to complete syncing.
        Returns True if sync completed successfully, False if timed out.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.client.folder_status(folder_id)
            if status['state'] == 'idle':
                return True
            time.sleep(5)
        return False
        
    def get_folder_status(self, folder_id: str) -> Dict[str, Any]:
        """Get current status of a folder"""
        return self.client.folder_status(folder_id)
        
    def get_folder_files(self, folder_id: str) -> Dict[str, Any]:
        """Get list of files in a folder"""
        return self.client.folder_files(folder_id)
        
    def add_folder(self, 
                  folder_id: str,
                  path: str,
                  type: str = "sendreceive",
                  rescan_interval: int = 3600) -> bool:
        """Add a new folder to sync"""
        try:
            self.client.add_folder(
                folder_id=folder_id,
                path=path,
                type=type,
                rescan_interval=rescan_interval
            )
            return True
        except Exception as e:
            print(f"Error adding folder: {e}")
            return False
            
    def remove_folder(self, folder_id: str) -> bool:
        """Remove a folder from sync"""
        try:
            self.client.remove_folder(folder_id)
            return True
        except Exception as e:
            print(f"Error removing folder: {e}")
            return False
            
    def get_device_id(self) -> Optional[str]:
        """Get the current device ID"""
        try:
            return self.client.system_status()['myID']
        except Exception:
            return None
            
    def get_connected_devices(self) -> Dict[str, Any]:
        """Get list of connected devices"""
        return self.client.system_connections() 