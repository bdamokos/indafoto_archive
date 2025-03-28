# Indafoto Archive Sync Tool

This tool helps synchronize Indafoto archives between different instances using Syncthing for file transfer and maintaining database consistency.

## Features

- Uses Syncthing for efficient file transfer
- Maintains database consistency with proper relationships (tags, albums, collections)
- Tracks sync progress and provenance
- Verifies file integrity using SHA256 hashes
- Handles duplicate detection
- Maintains the same folder structure as the original indafoto.py

## Prerequisites

- Python 3.8+
- Syncthing installed and running
- SQLite3

## Installation

1. Clone this repository
2. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. Copy `config.example.json` to `config.json`
2. Edit `config.json` with your settings:
   - `database_path`: Path to your SQLite database
   - `image_root`: Root directory for image storage
   - `syncthing_api_key`: Your Syncthing API key
   - `syncthing_host`: Syncthing host (default: localhost)
   - `syncthing_port`: Syncthing port (default: 8384)
   - `remote_sources`: List of remote sources to sync from

## Usage

1. Start Syncthing on both machines
2. On the source machine:
   - Share the folder containing images and metadata
   - Note the folder ID
3. On the destination machine:
   - Add the remote folder ID to `config.json`
   - Run the sync tool:
     ```bash
     python -m sync.src.main --config config.json
     ```

## How it Works

1. The tool uses Syncthing to receive files from the remote source
2. For each image:
   - Verifies the file hash
   - Checks for duplicates
   - Moves the file to the proper location in the archive
   - Updates the database with metadata and relationships
   - Records the sync operation
3. The process is atomic - if anything fails, the database is rolled back

## Error Handling

- Failed syncs are recorded in the `sync_records` table
- Duplicate files are detected and handled gracefully
- File integrity is verified using SHA256 hashes
- Database operations are wrapped in transactions

## Database Schema

The tool adds a new table to track sync operations:

```sql
CREATE TABLE sync_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    image_id INTEGER,
    remote_source TEXT,
    sync_date DATETIME,
    remote_path TEXT,
    status TEXT,
    error_message TEXT,
    FOREIGN KEY (image_id) REFERENCES images (id)
);
```
