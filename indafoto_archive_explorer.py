#TODO: We should have a way to browse the preserved images and see the data we have on them
# We should be able to recreate a view per user and per album, collection, tag
# We should be able to mark images worth preserving and save this info to database with a reference to the image (e.g. the ID of the image)

from flask import Flask, render_template, request, jsonify, send_file, abort
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
    return browse_images(author=author_name)

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
    return browse_images(tag=tag_name)

@app.route('/marked')
def marked_gallery():
    """View gallery of marked images."""
    return browse_images(marked=True)

def create_app():
    """Create and configure the Flask application."""
    # Ensure the templates directory exists
    templates_dir = Path(__file__).parent / 'templates'
    templates_dir.mkdir(exist_ok=True)
    
    # Initialize the database
    init_db()
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)