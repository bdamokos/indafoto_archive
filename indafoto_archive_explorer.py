#TODO: We should have a way to browse the preserved images and see the data we have on them
# We should be able to recreate a view per user and per album, collection, tag
# We should be able to mark images worth preserving and save this info to database with a reference to the image (e.g. the ID of the image)

from flask import Flask, render_template, request, jsonify, send_file, abort, redirect, url_for
import sqlite3
import os
from datetime import datetime
import math
from pathlib import Path

app = Flask(__name__)

# Configuration
DB_FILE = "indafoto.db"
ITEMS_PER_PAGE = 24  # Number of items to show per page

def get_db():
    """Create a database connection."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # This enables column access by name
    return conn

def init_db():
    """Initialize the database with additional tables needed for the explorer."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Create table for marking important images
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS marked_images (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        image_id INTEGER,
        note TEXT,
        marked_date TEXT,
        FOREIGN KEY (image_id) REFERENCES images (id)
    )
    """)
    
    conn.commit()
    conn.close()

@app.route('/')
def index():
    """Home page with statistics and navigation options."""
    conn = get_db()
    cursor = conn.cursor()
    
    # Gather statistics
    stats = {}
    
    # Total images
    cursor.execute("SELECT COUNT(*) as count FROM images")
    stats['total_images'] = cursor.fetchone()['count']
    
    # Total authors
    cursor.execute("SELECT COUNT(DISTINCT author) as count FROM images")
    stats['total_authors'] = cursor.fetchone()['count']
    
    # Total collections
    cursor.execute("SELECT COUNT(*) as count FROM collections")
    stats['total_collections'] = cursor.fetchone()['count']
    
    # Total albums
    cursor.execute("SELECT COUNT(*) as count FROM albums")
    stats['total_albums'] = cursor.fetchone()['count']
    
    # Total tags
    cursor.execute("SELECT COUNT(*) as count FROM tags")
    stats['total_tags'] = cursor.fetchone()['count']
    
    # Most active authors (top 5)
    cursor.execute("""
        SELECT author, COUNT(*) as image_count 
        FROM images 
        GROUP BY author 
        ORDER BY image_count DESC 
        LIMIT 5
    """)
    stats['top_authors'] = cursor.fetchall()
    
    # Most used tags (top 5)
    cursor.execute("""
        SELECT t.name, COUNT(it.image_id) as usage_count
        FROM tags t
        JOIN image_tags it ON t.id = it.tag_id
        GROUP BY t.id
        ORDER BY usage_count DESC
        LIMIT 5
    """)
    stats['top_tags'] = cursor.fetchall()
    
    conn.close()
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
        SELECT DISTINCT i.* 
        FROM images i
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
        query += " JOIN marked_images mi ON i.id = mi.image_id"
    
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
    
    # Get marked status and note
    cursor.execute("""
        SELECT * FROM marked_images
        WHERE image_id = ?
    """, (image_id,))
    marked = cursor.fetchone()
    
    conn.close()
    
    return render_template('image.html',
                         image=image,
                         collections=collections,
                         albums=albums,
                         tags=tags,
                         marked=marked)

@app.route('/api/mark_image', methods=['POST'])
def mark_image():
    """API endpoint to mark/unmark an image as important."""
    data = request.get_json()
    image_id = data.get('image_id')
    note = data.get('note', '')
    marked = data.get('marked', True)
    
    if not image_id:
        return jsonify({'error': 'Image ID is required'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if marked:
            cursor.execute("""
                INSERT OR REPLACE INTO marked_images (image_id, note, marked_date)
                VALUES (?, ?, ?)
            """, (image_id, note, datetime.now().isoformat()))
        else:
            cursor.execute("DELETE FROM marked_images WHERE image_id = ?", (image_id,))
        
        conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/image_file/<path:image_path>')
def serve_image(image_path):
    """Serve image files."""
    try:
        return send_file(image_path)
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
        SELECT i.* 
        FROM images i
        JOIN image_collections ic ON i.id = ic.image_id
        WHERE ic.collection_id = ?
        ORDER BY i.id DESC
    """, (collection_id,))
    images = cursor.fetchall()
    
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
        SELECT i.* 
        FROM images i
        JOIN image_albums ia ON i.id = ia.image_id
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
    cursor.execute("SELECT COUNT(*) as count FROM marked_images WHERE note IS NOT NULL AND note != ''")
    stats['with_notes'] = cursor.fetchone()['count']
    
    # Get marked images with their notes
    cursor.execute("""
        SELECT i.*, m.note, m.marked_date
        FROM images i
        JOIN marked_images m ON i.id = m.image_id
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

def create_app():
    """Create and configure the Flask application."""
    # Ensure the templates directory exists
    templates_dir = Path(__file__).parent / 'templates'
    templates_dir.mkdir(exist_ok=True)
    
    # Initialize the database
    init_db()
    
    # Add template context processors
    @app.context_processor
    def utility_processor():
        return {
            'max': max,
            'min': min
        }
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)