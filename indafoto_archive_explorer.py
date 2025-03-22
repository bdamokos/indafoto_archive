from flask import Flask, render_template, request, jsonify, send_file, abort, redirect, url_for
from werkzeug.utils import secure_filename
import sqlite3
import os
from datetime import datetime
import math
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import threading
import queue
import json
import niquests as requests
import logging
import time
from datetime import datetime, date
import signal
import sys
import re
from urllib.parse import unquote
import argparse
from indafoto import init_db as indafoto_init_db, get_banned_authors, ban_author, unban_author, cleanup_banned_author_content, check_for_updates

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('indafoto_explorer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
DB_FILE = "indafoto.db"
ITEMS_PER_PAGE = 24  # Number of items to show per page
SITE_DELETION_DATE = date(2025, 4, 1)  # Site deletion date
ARCHIVE_RATE_LIMIT = 5  # Seconds between archive submissions
ARCHIVE_QUEUE_FILE = "archive_queue.json"

# Headers for archive requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
}


# Global archive queue and submitter
archive_queue = queue.Queue()
archive_submitter = None










def get_db():
    """Create a database connection."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def get_archive_url(archive_url, original_url):
    """Handle special case of archive.ph submission URLs.
    
    Args:
        archive_url: The URL from the database (may be a submission URL)
        original_url: The original URL that was archived
        
    Returns:
        The actual archive URL to use
    """
    if not archive_url:
        return None
        
    # If it's just the archive.ph submission URL without parameters
    if archive_url == 'https://archive.ph/submit/':
        # Use the original URL to construct the archive URL
        return f'https://archive.ph/{original_url}'
        
    # If it's an archive.ph submission URL with parameters
    if archive_url.startswith('https://archive.ph/submit'):
        # Extract the original URL from the submission URL
        try:
            # The submission URL format is: https://archive.ph/submit?url=ORIGINAL_URL
            parsed = urlparse(archive_url)
            query_params = parse_qs(parsed.query)
            if 'url' in query_params:
                original_url = query_params['url'][0]
        except:
            # If we can't parse the submission URL, use the provided original_url
            pass
            
        # Construct the actual archive URL
        return f'https://archive.ph/{original_url}'
        
    return archive_url

def init_db():
    """Initialize the database with additional tables needed for the explorer."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Create table for marking important images
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS marked_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        image_id INTEGER,
        marked_date TEXT,
        FOREIGN KEY (image_id) REFERENCES images (id)
    )
    """)
    
    # Create table for image notes
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS image_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        image_id INTEGER,
        note TEXT,
        created_date TEXT,
        updated_date TEXT,
        FOREIGN KEY (image_id) REFERENCES images (id)
    )
    """)
    
    # Create table for favorite authors
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS favorite_authors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        author_name TEXT UNIQUE,
        added_date TEXT,
        priority INTEGER DEFAULT 0,
        last_processed_date TEXT
    )
    """)
    
    # Migrate existing notes to the new table
    try:
        cursor.execute("""
            INSERT INTO image_notes (image_id, note, created_date, updated_date)
            SELECT image_id, note, marked_date, marked_date
            FROM marked_images
            WHERE note IS NOT NULL AND note != ''
        """)
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Table might not exist yet
    
    # Remove note column from marked_images if it exists
    try:
        cursor.execute("ALTER TABLE marked_images DROP COLUMN note")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column might not exist
    
    conn.commit()
    conn.close()

@app.route('/')
def index():
    with get_db() as db:
        # Get total counts
        total_images = db.execute('SELECT COUNT(*) as count FROM images').fetchone()['count']
        total_authors = db.execute('SELECT COUNT(DISTINCT author) as count FROM images').fetchone()['count']
        total_collections = db.execute('SELECT COUNT(*) as count FROM collections').fetchone()['count']
        total_albums = db.execute('SELECT COUNT(*) as count FROM albums').fetchone()['count']
        total_tags = db.execute('SELECT COUNT(*) as count FROM tags').fetchone()['count']

        # Get all authors with their image counts
        all_authors = db.execute('''
            SELECT author as name, COUNT(*) as count 
            FROM images 
            GROUP BY author 
            ORDER BY count DESC
        ''').fetchall()

        # Get top tags
        top_tags = db.execute('''
            SELECT t.name, COUNT(it.image_id) as usage_count
            FROM tags t
            JOIN image_tags it ON t.id = it.tag_id
            GROUP BY t.id, t.name
            ORDER BY usage_count DESC
            LIMIT 20
        ''').fetchall()

        stats = {
            'total_images': total_images,
            'total_authors': total_authors,
            'total_collections': total_collections,
            'total_albums': total_albums,
            'total_tags': total_tags,
            'all_authors': all_authors,
            'top_tags': top_tags
        }
    return render_template('index.html', stats=stats)

@app.route('/images')
def browse_images():
    """Browse all images with filtering and pagination."""
    page = int(request.args.get('page', 1))
    author = request.args.get('author')
    tag = request.args.get('tag')
    collection = request.args.get('collection')
    album = request.args.get('album')
    marked = request.args.get('marked', '').lower() == 'true'
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Build the query based on filters
    query = """
        SELECT DISTINCT i.*, 
               CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END as is_marked,
               n.note, n.created_date as note_created_date, n.updated_date as note_updated_date
        FROM images i
        LEFT JOIN marked_images m ON i.id = m.image_id
        LEFT JOIN image_notes n ON i.id = n.image_id
    """
    params = []
    where_clauses = []
    
    if tag:
        query += """
            JOIN image_tags it ON i.id = it.image_id
            JOIN tags t ON it.tag_id = t.id
        """
        where_clauses.append("t.name = ?")
        params.append(tag)
    
    if collection:
        query += """
            JOIN image_collections ic ON i.id = ic.image_id
            JOIN collections c ON ic.collection_id = c.id
        """
        where_clauses.append("c.title = ?")
        params.append(collection)
    
    if album:
        query += """
            JOIN image_albums ia ON i.id = ia.image_id
            JOIN albums a ON ia.album_id = a.id
        """
        where_clauses.append("a.title = ?")
        params.append(album)
    
    if author:
        where_clauses.append("i.author = ?")
        params.append(author)
    
    if marked:
        where_clauses.append("m.id IS NOT NULL")
    
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    # Count total results
    count_query = f"SELECT COUNT(DISTINCT i.id) as count FROM ({query}) as i"
    cursor.execute(count_query, params)
    total_items = cursor.fetchone()['count']
    total_pages = math.ceil(total_items / ITEMS_PER_PAGE)
    
    # Add pagination
    query += " ORDER BY i.id DESC LIMIT ? OFFSET ?"
    params.extend([ITEMS_PER_PAGE, (page - 1) * ITEMS_PER_PAGE])
    
    cursor.execute(query, params)
    images = cursor.fetchall()
    
    # Get available filters
    cursor.execute("SELECT DISTINCT author FROM images ORDER BY author")
    authors = [row['author'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT name FROM tags ORDER BY name")
    tags = [row['name'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT title FROM collections ORDER BY title")
    collections = [row['title'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT title FROM albums ORDER BY title")
    albums = [row['title'] for row in cursor.fetchall()]
    
    conn.close()
    
    return render_template('browse.html',
                         images=images,
                         page=page,
                         total_pages=total_pages,
                         authors=authors,
                         tags=tags,
                         collections=collections,
                         albums=albums,
                         current_filters={
                             'author': author,
                             'tag': tag,
                             'collection': collection,
                             'album': album,
                             'marked': marked
                         })

@app.route('/image/<int:image_id>')
def view_image(image_id):
    """View details of a specific image."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get image details
    cursor.execute("SELECT * FROM images WHERE id = ?", (image_id,))
    image = cursor.fetchone()
    
    if not image:
        abort(404)
    
    # Get collections
    cursor.execute("""
        SELECT c.* 
        FROM collections c
        JOIN image_collections ic ON c.id = ic.collection_id
        WHERE ic.image_id = ?
    """, (image_id,))
    collections = cursor.fetchall()
    
    # Get albums
    cursor.execute("""
        SELECT a.* 
        FROM albums a
        JOIN image_albums ia ON a.id = ia.album_id
        WHERE ia.image_id = ?
    """, (image_id,))
    albums = cursor.fetchall()
    
    # Get tags
    cursor.execute("""
        SELECT t.* 
        FROM tags t
        JOIN image_tags it ON t.id = it.tag_id
        WHERE it.image_id = ?
    """, (image_id,))
    tags = cursor.fetchall()
    
    # Get marked status
    cursor.execute("""
        SELECT marked_date
        FROM marked_images
        WHERE image_id = ?
    """, (image_id,))
    marked = cursor.fetchone()
    
    # Get note if exists
    cursor.execute("""
        SELECT note, created_date, updated_date
        FROM image_notes
        WHERE image_id = ?
    """, (image_id,))
    note = cursor.fetchone()
    
    # Get archive information for the image page
    cursor.execute("""
        SELECT archive_url, status, submission_date
        FROM archive_submissions
        WHERE url = ? AND type = 'image_page'
        ORDER BY submission_date DESC
        LIMIT 1
    """, (image['page_url'],))
    image_archive = cursor.fetchone()
    
    # Get archive information for the author page
    author_archive = None
    author_details_archive = None
    if image['author_url']:
        cursor.execute("""
            SELECT archive_url, status, submission_date
            FROM archive_submissions
            WHERE url = ? AND type = 'author_page'
            ORDER BY submission_date DESC
            LIMIT 1
        """, (image['author_url'],))
        author_archive = cursor.fetchone()

        # Get archive information for author details page using the correct URL
        author_details_url = image['author_url'] + "/details"
        cursor.execute("""
            SELECT archive_url, status, submission_date
            FROM archive_submissions
            WHERE url = ? AND type = 'author_details'
            ORDER BY submission_date DESC
            LIMIT 1
        """, (author_details_url,))
        author_details_archive = cursor.fetchone()
    
    # Process archive URLs to handle submission URLs
    if image_archive:
        image_archive = dict(image_archive)
        image_archive['archive_url'] = get_archive_url(image_archive['archive_url'], image['page_url'])
    
    if author_archive:
        author_archive = dict(author_archive)
        author_archive['archive_url'] = get_archive_url(author_archive['archive_url'], image['author_url'])

    if author_details_archive:
        author_details_archive = dict(author_details_archive)
        author_details_archive['archive_url'] = get_archive_url(author_details_archive['archive_url'], author_details_url)
    
    conn.close()
    
    return render_template('image.html',
                         image=image,
                         collections=collections,
                         albums=albums,
                         tags=tags,
                         marked=marked,
                         note=note,
                         image_archive=image_archive,
                         author_archive=author_archive,
                         author_details_archive=author_details_archive)

@app.route('/api/mark_image', methods=['POST'])
def mark_image():
    """API endpoint to mark/unmark an image as important."""
    data = request.get_json()
    image_id = data.get('image_id')
    marked = data.get('marked', True)
    
    logger.info(f"Marking image {image_id}, marked: {marked}")
    
    if not image_id:
        return jsonify({'error': 'Image ID is required'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if marked:
            logger.info(f"Inserting marked_images record for image {image_id}")
            # First delete any existing records for this image
            cursor.execute("DELETE FROM marked_images WHERE image_id = ?", (image_id,))
            # Then insert the new record
            cursor.execute("""
                INSERT INTO marked_images (image_id, marked_date)
                VALUES (?, ?)
            """, (image_id, datetime.now().isoformat()))
        else:
            logger.info(f"Unmarking image {image_id}")
            # Delete all records for this image
            cursor.execute("DELETE FROM marked_images WHERE image_id = ?", (image_id,))
        
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error in mark_image: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/image_note', methods=['POST'])
def manage_image_note():
    """API endpoint to add, update, or delete a note for an image."""
    data = request.get_json()
    image_id = data.get('image_id')
    note = data.get('note', '')
    action = data.get('action', 'add')  # 'add', 'update', or 'delete'
    
    if not image_id:
        return jsonify({'error': 'Image ID is required'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if action == 'delete':
            cursor.execute("DELETE FROM image_notes WHERE image_id = ?", (image_id,))
        else:
            # Check if note exists
            cursor.execute("SELECT id FROM image_notes WHERE image_id = ?", (image_id,))
            existing_note = cursor.fetchone()
            
            if existing_note:
                if action == 'update':
                    cursor.execute("""
                        UPDATE image_notes 
                        SET note = ?, updated_date = ?
                        WHERE image_id = ?
                    """, (note, datetime.now().isoformat(), image_id))
            else:
                cursor.execute("""
                    INSERT INTO image_notes (image_id, note, created_date, updated_date)
                    VALUES (?, ?, ?, ?)
                """, (image_id, note, datetime.now().isoformat(), datetime.now().isoformat()))
        
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error in manage_image_note: {e}")
        return jsonify({'error': 'An internal error has occurred!'}), 500
    finally:
        conn.close()

@app.route('/api/image_note/<int:image_id>', methods=['GET'])
def get_image_note(image_id):
    """API endpoint to get the note for an image."""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT note, created_date, updated_date 
            FROM image_notes 
            WHERE image_id = ?
        """, (image_id,))
        result = cursor.fetchone()
        
        if result:
            return jsonify({
                'note': result['note'],
                'created_date': result['created_date'],
                'updated_date': result['updated_date']
            })
        return jsonify({'note': None})
    except Exception as e:
        logger.error(f"Error in get_image_note: {e}")
        return jsonify({'error': 'An internal error has occurred!'}), 500
    finally:
        conn.close()

@app.route('/serve_image/<path:image_path>')
def serve_image(image_path):
    """Serve image files with streaming and caching enabled."""
    try:
        # Get the base path for images (indafoto_archive directory)
        base_path = os.path.abspath(os.path.join(os.getcwd(), 'indafoto_archive'))
        
        # URL decode the path first
        decoded_path = unquote(image_path)
        
        # Normalize the path to prevent path traversal
        normalized_path = os.path.normpath(decoded_path)
        
        # Validate the path structure
        if '..' in normalized_path or normalized_path.startswith('/'):
            logger.error(f"Invalid path structure detected: {normalized_path}")
            abort(404)
            
        # Remove indafoto_archive prefix if it exists
        if normalized_path.startswith('indafoto_archive/'):
            normalized_path = normalized_path[len('indafoto_archive/'):]
            
        # Construct the full path and ensure it's within the base directory
        full_path = os.path.join(base_path, normalized_path)
        if not os.path.abspath(full_path).startswith(base_path):
            logger.error(f"Path traversal attempt detected: {full_path}")
            abort(404)
            
        # Check if file exists and is a file (not a directory)
        if not os.path.isfile(full_path):
            logger.error(f"Image file not found: {full_path}")
            abort(404)
            
        # Serve the file with streaming and caching enabled
        return send_file(
            full_path,
            mimetype='image/jpeg',
            conditional=True,  # Enable conditional responses (304 Not Modified)
            etag=True  # Enable ETag support
        )
        
    except Exception as e:
        logger.error(f"Error serving image {image_path}: {e}")
        abort(404)

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files."""
    try:
        base_path = os.path.join(os.getcwd(), 'static')
        safe_filename = secure_filename(filename)
        full_path = os.path.normpath(os.path.join(base_path, safe_filename))
        if not full_path.startswith(base_path):
            abort(404)
        return send_file(full_path)
    except Exception as e:
        abort(404)

@app.route('/author/<path:author_name>')
def author_gallery(author_name):
    """View gallery of a specific author."""
    return redirect(url_for('browse_images', author=author_name))

@app.route('/collection/<int:collection_id>')
def collection_gallery(collection_id):
    """View gallery of a specific collection."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get collection details
    cursor.execute("SELECT * FROM collections WHERE id = ?", (collection_id,))
    collection = cursor.fetchone()
    
    if not collection:
        abort(404)
    
    # Get images in collection
    cursor.execute("""
        SELECT i.*, 
               CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END as is_marked,
               n.note, n.created_date as note_created_date, n.updated_date as note_updated_date
        FROM images i
        JOIN image_collections ic ON i.id = ic.image_id
        LEFT JOIN marked_images m ON i.id = m.image_id
        LEFT JOIN image_notes n ON i.id = n.image_id
        WHERE ic.collection_id = ?
        ORDER BY i.id DESC
    """, (collection_id,))
    images = cursor.fetchall()
    
    # # Log image data for debugging
    # for image in images:
    #     logger.info(f"Image {image['id']}: local_path={image['local_path']}, title={image['title']}")
    
    conn.close()
    
    return render_template('collection.html',
                         collection=collection,
                         images=images)

@app.route('/album/<int:album_id>')
def album_gallery(album_id):
    """View gallery of a specific album."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get album details
    cursor.execute("SELECT * FROM albums WHERE id = ?", (album_id,))
    album = cursor.fetchone()
    
    if not album:
        abort(404)
    
    # Get images in album
    cursor.execute("""
        SELECT i.*, 
               CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END as is_marked,
               n.note, n.created_date as note_created_date, n.updated_date as note_updated_date
        FROM images i
        JOIN image_albums ia ON i.id = ia.image_id
        LEFT JOIN marked_images m ON i.id = m.image_id
        LEFT JOIN image_notes n ON i.id = n.image_id
        WHERE ia.album_id = ?
        ORDER BY i.id DESC
    """, (album_id,))
    images = cursor.fetchall()
    
    conn.close()
    
    return render_template('album.html',
                         album=album,
                         images=images)

@app.route('/tag/<path:tag_name>')
def tag_gallery(tag_name):
    """View gallery of images with a specific tag."""
    return redirect(url_for('browse_images', tag=tag_name))

@app.route('/marked')
def marked_gallery():
    """View gallery of marked images with statistics and notes."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get statistics
    stats = {}
    
    # Total marked images
    cursor.execute("SELECT COUNT(*) as count FROM marked_images")
    stats['total_marked'] = cursor.fetchone()['count']
    
    # Total images (for percentage calculation)
    cursor.execute("SELECT COUNT(*) as count FROM images")
    total_images = cursor.fetchone()['count']
    stats['percentage_marked'] = (stats['total_marked'] / total_images * 100) if total_images > 0 else 0
    
    # Count of different authors
    cursor.execute("""
        SELECT COUNT(DISTINCT i.author) as count
        FROM images i
        JOIN marked_images m ON i.id = m.image_id
    """)
    stats['authors_count'] = cursor.fetchone()['count']
    
    # Count of images with notes
    cursor.execute("SELECT COUNT(*) as count FROM image_notes")
    stats['with_notes'] = cursor.fetchone()['count']
    
    # Get marked images with their notes
    cursor.execute("""
        SELECT i.*, m.marked_date, n.note, n.created_date as note_created_date, n.updated_date as note_updated_date
        FROM images i
        JOIN marked_images m ON i.id = m.image_id
        LEFT JOIN image_notes n ON i.id = n.image_id
        ORDER BY m.marked_date DESC
    """)
    images = cursor.fetchall()
    
    conn.close()
    
    return render_template('marked.html', images=images, stats=stats)

@app.route('/tags')
def browse_tags():
    """View all tags with their statistics."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get all tags with both their original count and current archive count
    cursor.execute("""
        SELECT t.name, t.count as original_count,
               COUNT(DISTINCT it.image_id) as archive_count
        FROM tags t
        LEFT JOIN image_tags it ON t.id = it.tag_id
        GROUP BY t.id
        ORDER BY archive_count DESC, t.count DESC
    """)
    
    tags = [
        {
            'name': row['name'],
            'count': row['original_count'],
            'archive_count': row['archive_count']
        }
        for row in cursor.fetchall()
    ]
    
    conn.close()
    
    return render_template('tags.html', tags=tags)

@app.route('/tag/<path:tag_name>/detail')
def tag_detail(tag_name):
    """View detailed information about a specific tag."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get tag details
    cursor.execute("""
        SELECT t.name, t.count,
               COUNT(DISTINCT it.image_id) as archive_count
        FROM tags t
        LEFT JOIN image_tags it ON t.id = it.tag_id
        WHERE t.name = ?
        GROUP BY t.id
    """, (tag_name,))
    tag = cursor.fetchone()
    
    if not tag:
        abort(404)
    
    # Get related tags (tags that appear together with this tag)
    cursor.execute("""
        SELECT t2.name, COUNT(DISTINCT it2.image_id) as count
        FROM tags t1
        JOIN image_tags it1 ON t1.id = it1.tag_id
        JOIN image_tags it2 ON it1.image_id = it2.image_id
        JOIN tags t2 ON it2.tag_id = t2.id
        WHERE t1.name = ? AND t2.name != ?
        GROUP BY t2.id
        ORDER BY count DESC
        LIMIT 20
    """, (tag_name, tag_name))
    related_tags = cursor.fetchall()
    
    # Get top authors using this tag
    cursor.execute("""
        SELECT i.author as name, COUNT(DISTINCT i.id) as count
        FROM images i
        JOIN image_tags it ON i.id = it.image_id
        JOIN tags t ON it.tag_id = t.id
        WHERE t.name = ?
        GROUP BY i.author
        ORDER BY count DESC
        LIMIT 10
    """, (tag_name,))
    top_authors = cursor.fetchall()
    
    # Get recent images with this tag
    cursor.execute("""
        SELECT i.*
        FROM images i
        JOIN image_tags it ON i.id = it.image_id
        JOIN tags t ON it.tag_id = t.id
        WHERE t.name = ?
        ORDER BY i.id DESC
        LIMIT 12
    """, (tag_name,))
    recent_images = cursor.fetchall()
    
    conn.close()
    
    return render_template('tag_detail.html',
                         tag=tag,
                         related_tags=related_tags,
                         top_authors=top_authors,
                         recent_images=recent_images)

@app.route('/albums')
def browse_albums():
    """View all albums with their first image as thumbnail."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get current page from query parameters
    page = request.args.get('page', 1, type=int)
    items_per_page = ITEMS_PER_PAGE
    
    # Get total count of albums
    cursor.execute("SELECT COUNT(*) FROM albums")
    total_items = cursor.fetchone()[0]
    total_pages = math.ceil(total_items / items_per_page)
    
    # Get paginated albums with their image counts and first image
    cursor.execute("""
        WITH album_stats AS (
            SELECT a.id, a.title, a.url, a.is_public,
                   COUNT(DISTINCT ia.image_id) as image_count,
                   MIN(i.id) as first_image_id  -- Get the earliest image as thumbnail
            FROM albums a
            LEFT JOIN image_albums ia ON a.id = ia.album_id
            LEFT JOIN images i ON ia.image_id = i.id
            GROUP BY a.id
        )
        SELECT 
            as1.*,
            i.local_path as thumbnail_path,
            i.title as thumbnail_title,
            CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END as is_marked,
            n.note, n.created_date as note_created_date, n.updated_date as note_updated_date
        FROM album_stats as1
        LEFT JOIN images i ON as1.first_image_id = i.id
        LEFT JOIN marked_images m ON i.id = m.image_id
        LEFT JOIN image_notes n ON i.id = n.image_id
        ORDER BY as1.image_count DESC, as1.title
        LIMIT ? OFFSET ?
    """, (items_per_page, (page - 1) * items_per_page))
    
    albums = cursor.fetchall()
    
    # Get some statistics
    stats = {
        'total_albums': total_items,
        'total_public': sum(1 for album in albums if album['is_public']),
        'total_private': sum(1 for album in albums if not album['is_public']),
        'total_images': sum(album['image_count'] for album in albums)
    }
    
    conn.close()
    
    return render_template('browse_albums.html',
                         albums=albums,
                         stats=stats,
                         page=page,
                         total_pages=total_pages)

@app.route('/collections')
def browse_collections():
    """View all collections with their first image as thumbnail."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get current page from query parameters
    page = request.args.get('page', 1, type=int)
    items_per_page = ITEMS_PER_PAGE
    
    # Get total count of collections
    cursor.execute("SELECT COUNT(*) FROM collections")
    total_items = cursor.fetchone()[0]
    total_pages = math.ceil(total_items / items_per_page)
    
    # Get paginated collections with their image counts and first image
    cursor.execute("""
        WITH collection_stats AS (
            SELECT c.id, c.title, c.url, c.is_public,
                   COUNT(DISTINCT ic.image_id) as image_count,
                   MIN(i.id) as first_image_id  -- Get the earliest image as thumbnail
            FROM collections c
            LEFT JOIN image_collections ic ON c.id = ic.collection_id
            LEFT JOIN images i ON ic.image_id = i.id
            GROUP BY c.id
        )
        SELECT 
            cs.*,
            i.local_path as thumbnail_path,
            i.title as thumbnail_title,
            CASE WHEN m.id IS NOT NULL THEN 1 ELSE 0 END as is_marked,
            n.note, n.created_date as note_created_date, n.updated_date as note_updated_date
        FROM collection_stats cs
        LEFT JOIN images i ON cs.first_image_id = i.id
        LEFT JOIN marked_images m ON i.id = m.image_id
        LEFT JOIN image_notes n ON i.id = n.image_id
        ORDER BY cs.image_count DESC, cs.title
        LIMIT ? OFFSET ?
    """, (items_per_page, (page - 1) * items_per_page))
    
    collections = cursor.fetchall()
    
    # Get some statistics
    stats = {
        'total_collections': total_items,
        'total_public': sum(1 for collection in collections if collection['is_public']),
        'total_private': sum(1 for collection in collections if not collection['is_public']),
        'total_images': sum(collection['image_count'] for collection in collections)
    }
    
    conn.close()
    
    return render_template('browse_collections.html',
                         collections=collections,
                         stats=stats,
                         page=page,
                         total_pages=total_pages)

@app.route('/api/favorite_authors', methods=['GET'])
def get_favorite_authors():
    """Get list of favorite authors with their status."""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT fa.*, 
                   COUNT(DISTINCT i.id) as total_images,
                   COUNT(DISTINCT CASE WHEN a.status = 'success' THEN i.id END) as archived_images
            FROM favorite_authors fa
            LEFT JOIN images i ON fa.author_name = i.author
            LEFT JOIN archive_submissions a ON i.page_url = a.url
            GROUP BY fa.id
            ORDER BY fa.priority DESC, fa.last_processed_date NULLS FIRST
        """)
        authors = cursor.fetchall()
        
        return jsonify({
            'authors': [dict(author) for author in authors]
        })
    except Exception as e:
        logger.error(f"Error getting favorite authors: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/favorite_authors', methods=['POST'])
def add_favorite_author():
    """Add a new favorite author."""
    data = request.get_json()
    author_name = data.get('author_name')
    priority = data.get('priority', 0)
    
    if not author_name:
        return jsonify({'error': 'Author name is required'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Check if author exists in images table
        cursor.execute("SELECT COUNT(*) FROM images WHERE author = ?", (author_name,))
        if cursor.fetchone()[0] == 0:
            return jsonify({'error': 'Author not found in images table'}), 404
        
        # Add to favorite authors
        cursor.execute("""
            INSERT INTO favorite_authors (author_name, added_date, priority)
            VALUES (?, datetime('now'), ?)
            ON CONFLICT(author_name) DO UPDATE SET
                priority = excluded.priority,
                last_processed_date = NULL  -- Reset last processed date to trigger reprocessing
        """, (author_name, priority))
        
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error adding favorite author: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/favorite_authors/<path:author_name>', methods=['DELETE'])
def remove_favorite_author(author_name):
    """Remove a favorite author."""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM favorite_authors WHERE author_name = ?", (author_name,))
        conn.commit()
        
        if cursor.rowcount == 0:
            return jsonify({'error': 'Author not found in favorites'}), 404
            
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error removing favorite author: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/favorite_authors/<path:author_name>', methods=['PATCH'])
def update_favorite_author(author_name):
    """Update a favorite author's priority."""
    data = request.get_json()
    priority = data.get('priority')
    
    if priority is None:
        return jsonify({'error': 'Priority is required'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE favorite_authors 
            SET priority = ?, last_processed_date = NULL  -- Reset last processed date to trigger reprocessing
            WHERE author_name = ?
        """, (priority, author_name))
        
        conn.commit()
        
        if cursor.rowcount == 0:
            return jsonify({'error': 'Author not found in favorites'}), 404
            
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error updating favorite author: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/authors')
def browse_authors():
    """View all authors with their image counts."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Get all authors with their image counts
    cursor.execute("""
        SELECT author as name, COUNT(*) as count 
        FROM images 
        GROUP BY author 
        ORDER BY count DESC
    """)
    authors = cursor.fetchall()
    
    conn.close()
    
    return render_template('browse_authors.html', authors=authors)

@app.route('/banned_authors')
def banned_authors():
    """Display the banned authors management page."""
    conn = indafoto_init_db()  # Use the init_db from indafoto.py
    cursor_results = get_banned_authors(conn)
    # Convert cursor results to list of dictionaries
    banned_authors = [
        {
            'author': row[1],  # author column
            'reason': row[2],  # reason column
            'banned_date': row[3],  # banned_date column
            'banned_by': row[4]  # banned_by column
        }
        for row in cursor_results
    ]
    conn.close()
    return render_template('banned_authors.html', banned_authors=banned_authors)

@app.route('/api/banned_authors', methods=['POST'])
def add_banned_author():
    """API endpoint to add a new banned author."""
    data = request.get_json()
    if not data or 'author' not in data or 'reason' not in data:
        return jsonify({'success': False, 'error': 'Missing required fields'})
    
    conn = indafoto_init_db()
    try:
        success = ban_author(conn, data['author'], data['reason'], banned_by="admin")
        if success:
            # Clean up existing content if requested
            if data.get('cleanup_existing', False):
                cleanup_banned_author_content(conn, data['author'])
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Author is already banned'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    finally:
        conn.close()

@app.route('/api/banned_authors/<author>', methods=['DELETE'])
def remove_banned_author(author):
    """API endpoint to remove a banned author."""
    conn = indafoto_init_db()
    try:
        success = unban_author(conn, author)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Author is not banned'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    finally:
        conn.close()

@app.route('/api/banned_authors/<author>/cleanup', methods=['POST'])
def cleanup_author_content(author):
    """API endpoint to clean up content from a banned author."""
    conn = indafoto_init_db()
    try:
        success = cleanup_banned_author_content(conn, author)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Failed to clean up content'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})
    finally:
        conn.close()

@app.context_processor
def inject_nav_items():
    return {
        'nav_items': [
            {'url': '/', 'text': 'Home'},
            {'url': '/images', 'text': 'Browse Images'},
            {'url': '/authors', 'text': 'Browse Authors'},
            {'url': '/marked', 'text': 'Marked Images'},
            {'url': '/banned_authors', 'text': 'Banned Authors'}
        ]
    }

def create_app():
    """Create and configure the Flask application."""
    # Ensure the templates directory exists
    templates_dir = Path(__file__).parent / 'templates'
    templates_dir.mkdir(exist_ok=True)
    
    # Initialize the database
    init_db()
    
    # Start archive submitter
    # start_archive_submitter()
    
    # Add template context processors
    @app.context_processor
    def utility_processor():
        return {
            'max': max,
            'min': min,
            'len': len,
            'should_archive': True
        }
    
    return app

def signal_handler(signum, frame):
    """Handle interrupt signals by gracefully shutting down."""
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    
    sys.exit(0)

if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Indafoto Archive Explorer')
    parser.add_argument('--no-update-check', action='store_true',
                       help='Skip checking for updates')
    parser.add_argument('--port', type=int, default=5001,
                       help='Port to run the web server on')
    parser.add_argument('--host', default='0.0.0.0',
                       help='Host to run the web server on')
    parser.add_argument('--debug', action='store_true',
                       help='Run Flask in debug mode')
    args = parser.parse_args()
    
    try:
        # Check for updates unless explicitly disabled
        if not args.no_update_check:
            check_for_updates(__file__)
            
        # Create and configure the application
        app = create_app()
        
        # Register cleanup handler
        import atexit
        
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the Flask development server
        logger.info(f"Starting Flask server on {args.host}:{args.port}...")
        app.run(debug=args.debug, host=args.host, port=args.port)
    except Exception as e:
        logger.error(f"Failed to start server: {e}")
        sys.exit(1)