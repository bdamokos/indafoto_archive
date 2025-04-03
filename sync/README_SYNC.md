# Indafoto Sync System

This system allows users to sync their Indafoto archives by exchanging missing files and metadata. The sync process is designed to be efficient and reliable, with support for resuming interrupted transfers and verifying file integrity.

## Sync Process Overview

The sync process consists of the following steps:

1. **Exchange Database Files**: Users exchange their `indafoto.db` files to compare metadata.
2. **Analyze Differences**: Each user analyzes the remote database to identify missing files.
3. **Create Export Lists**: Lists of files to be shared are created and exchanged.
4. **Export Files**: Users prepare the requested files for sharing.
5. **Import Files**: Users import the received files and update their databases.
6. **Cleanup**: Temporary files are cleaned up.

## Scripts

The sync system consists of several scripts:

### `analyze_sync.py`

Analyzes a remote database and creates an export list of files to be shared.

```bash
# Analyze remote database and create export list
python analyze_sync.py --db /path/to/local/indafoto.db --remote-db /path/to/remote/indafoto.db

# View statistics of an existing export list
python analyze_sync.py --db /path/to/local/indafoto.db --list-id <list_id>

# Dry run - analyze without creating export list
python analyze_sync.py --db /path/to/local/indafoto.db --remote-db /path/to/remote/indafoto.db --dry-run
```

### `export_files.py`

Exports files based on an export list.

```bash
# Export files
python export_files.py --db /path/to/indafoto.db --list-id <list_id> --output-dir /path/to/export/dir

# Dry run - prepare export list without copying files
python export_files.py --db /path/to/indafoto.db --list-id <list_id> --output-dir /path/to/export/dir --dry-run
```

### `import_files.py`

Imports files from a sync operation.

```bash
# Import files
python import_files.py --db /path/to/indafoto.db --list-id <list_id> --import-dir /path/to/import/dir

# Verify import results
python import_files.py --db /path/to/indafoto.db --list-id <list_id> --verify

# Dry run - verify files without importing
python import_files.py --db /path/to/indafoto.db --list-id <list_id> --import-dir /path/to/import/dir --dry-run
```

## Database Schema

The sync system uses several additional tables in the Indafoto database:

### `sync_operations`

Tracks sync sessions:
- `id`: Primary key
- `operation_id`: Unique identifier for the sync operation
- `start_time`: When the operation started
- `end_time`: When the operation ended
- `status`: Current status
- `source_db_path`: Path to the source database
- `total_files`: Total number of files to process
- `processed_files`: Number of files processed
- `error_count`: Number of errors encountered

### `sync_files`

Tracks individual files in sync operations:
- `id`: Primary key
- `operation_id`: Reference to sync_operations
- `image_id`: Reference to images table
- `file_path`: Path to the file
- `file_hash`: SHA-256 hash of the file
- `status`: Current status
- `error_message`: Error message if any

### `sync_export_lists`

Tracks export lists:
- `id`: Primary key
- `operation_id`: Reference to sync_operations
- `list_id`: Unique identifier for the export list
- `created_time`: When the list was created
- `total_files`: Total number of files in the list
- `processed_files`: Number of files processed
- `status`: Current status

### `sync_export_files`

Tracks files in export lists:
- `id`: Primary key
- `list_id`: Reference to sync_export_lists
- `image_id`: Reference to images table
- `export_path`: Path to export the file
- `status`: Current status

## Best Practices

1. **Backup Your Database**: Always backup your database before starting a sync operation.
2. **Verify File Integrity**: The system verifies file hashes during import, but it's good practice to verify manually as well.
3. **Monitor Progress**: Use the verification options in the scripts to monitor progress and identify any issues.
4. **Clean Up**: Always run the cleanup step after a successful sync to remove temporary files.
5. **Resume Support**: If a sync operation is interrupted, you can resume it by using the same list ID.
6. **Use Dry Run**: Always perform a dry run before executing actual operations to verify everything is correct.

## Error Handling

The system includes comprehensive error handling:
- File hash verification
- Database integrity checks
- Transaction rollback on errors
- Detailed error logging
- Status tracking for each file

## Security Considerations

1. **File Verification**: All files are verified using SHA-256 hashes.
2. **Database Integrity**: Foreign key constraints ensure database integrity.
3. **Transaction Safety**: All database operations use transactions for safety.
4. **Cleanup**: Temporary files are automatically cleaned up after successful operations.
5. **Dry Run**: The dry run feature allows testing operations without making changes. 