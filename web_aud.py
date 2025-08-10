import os
import io
import logging
import threading
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash, send_from_directory
from pydub import AudioSegment
from db import PostgresConnector

# Initialize Flask app
app = Flask(__name__)
# replace app.secret_key = "supersecretkey"
app.secret_key = os.environ.get("SECRET_KEY", "supersecretkey")

# Admin password
ADMIN_PASSWORD = "idrakai"

# Authentication decorator
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated_function

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()  # Also log to console
    ]
)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

from collections import deque

# Cache for text chunks
text_cache = deque()

def cleanup_abandoned_chunks():
    """Reset chunks that have been pending for too long (e.g., 30 minutes)."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            # Reset chunks that have been pending for more than 30 minutes
            # Note: This assumes you have a timestamp column. If not, we'll use a simpler approach
            cursor.execute("""
                UPDATE chunks 
                SET pending = 'no' 
                WHERE pending = 'yes' 
                AND recording != 'done'
                AND (
                    -- If you have a timestamp column, uncomment this:
                    -- updated_at < NOW() - INTERVAL '30 minutes'
                    -- For now, we'll use a simpler approach - reset ALL old pending chunks
                    chunk_id IN (
                        SELECT chunk_id FROM chunks 
                        WHERE pending = 'yes' AND recording != 'done'
                        ORDER BY chunk_id 
                        LIMIT 100  -- Reset up to 100 old pending chunks
                    )
                )
            """)
            
            reset_count = cursor.rowcount
            conn.commit()
            
            if reset_count > 0:
                logging.info(f"✅ Reset {reset_count} abandoned pending chunks.")
                
        PostgresConnector.release_connection(conn)
        return reset_count
        
    except Exception as e:
        logging.error(f"❌ Error in cleanup_abandoned_chunks: {e}", exc_info=True)
        return 0

def fetch_text_chunks():
    """Fetch the next available text chunks and store them in the cache."""
    global text_cache
    try:
        # First, cleanup any abandoned chunks
        cleanup_abandoned_chunks()
        
        conn = PostgresConnector.get_connection()

        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT text, chunk_id FROM chunks 
            WHERE recording != 'done' AND pending <> 'yes'
            ORDER BY chunk_id 
            LIMIT 50 
            FOR UPDATE SKIP LOCKED;""")
            rows = cursor.fetchall()

            if rows:
                text_cache = deque(rows)  # Store fetched chunks in cache
                logging.info(f"✅ Loaded {len(rows)} chunks into cache.")
                
        PostgresConnector.release_connection(conn)        

    except Exception as e:
        logging.error(f"❌ Error in fetch_text_chunks: {e}", exc_info=True)


# --------------------- GET NEXT TEXT FUNCTION ---------------------
def get_next_text():
    """Fetch the next available text chunk from cache, refresh when empty."""
    global text_cache

    if not text_cache:
        fetch_text_chunks()  # Refill the cache when empty

    if text_cache:
        text, chunk_id = text_cache.popleft()  # Fetch from cache
        
        # ✅ NOW mark this specific chunk as pending when actually assigned to user
        try:
            conn = PostgresConnector.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE chunks SET pending = %s WHERE chunk_id = %s;", ("yes", chunk_id))
                conn.commit()
            PostgresConnector.release_connection(conn)
            logging.info(f"✅ Chunk {chunk_id} locked & marked as pending.")
            
        except Exception as e:
            logging.error(f"❌ Error updating chunk {chunk_id}: {e}", exc_info=True)
            
        return text, chunk_id

    logging.info("⚠️ No available chunks found!")
    return None


# --------------------- STORE AUDIO FUNCTION ---------------------
def store_audio_file(audio_file, audio_path):
    """Store uploaded audio file after converting it to WAV format."""
    try:
        logging.info(f"🎵 Starting audio file processing for: {audio_path}")
        
        # Check if audio_file is valid
        if not audio_file:
            raise ValueError("No audio file provided")
            
        # Read the audio file
        audio_data = audio_file.read()
        logging.info(f"📁 Read {len(audio_data)} bytes from uploaded file")
        
        if len(audio_data) == 0:
            raise ValueError("Uploaded audio file is empty")
        
        # Convert using pydub
        audio = AudioSegment.from_file(io.BytesIO(audio_data))
        logging.info(f"🎧 Loaded audio: {len(audio)}ms duration, {audio.frame_rate}Hz, {audio.channels} channels")
        
        # Convert to desired format
        audio = audio.set_channels(1).set_frame_rate(16000)
        logging.info("🔄 Converted audio to mono 16kHz")
        
        # Export to WAV
        audio.export(audio_path, format="wav")
        logging.info(f"✅ Audio file saved successfully at: {audio_path}")
        
        # Verify the file was created
        if os.path.exists(audio_path):
            file_size = os.path.getsize(audio_path)
            logging.info(f"✅ Verified: File exists with size {file_size} bytes")
        else:
            raise FileNotFoundError(f"Failed to create audio file at {audio_path}")
            
    except Exception as e:
        logging.error(f"❌ Error in store_audio_file: {e}", exc_info=True)
        raise  # Re-raise the exception so the caller knows it failed


# --------------------- FLASK ROUTES ---------------------
@app.route("/uploads/<filename>")
def uploaded_file(filename):
    """Serve uploaded audio files."""
    try:
        return send_from_directory(UPLOAD_FOLDER, filename)
    except Exception as e:
        logging.error(f"❌ Error serving file {filename}: {e}", exc_info=True)
        return "File not found", 404

@app.route("/", methods=["GET", "POST"])
def index():
    """Main page to handle recording uploads."""
    try:
        if request.method == "POST":
            logging.info("📨 Received POST request for audio upload")
            
            name = request.form.get("name", "").strip()
            audio_file = request.files.get("audio")
            form_chunk_id = request.form.get("chunk_id")

            logging.info(f"📝 Form data - Name: '{name}', Chunk ID: '{form_chunk_id}', Audio file: {audio_file is not None}")

            if not name or not audio_file:
                error_msg = "⚠️ Name and audio file are required!"
                logging.warning(error_msg)
                return error_msg, 400

            # More lenient session check - only block if trying to submit a different chunk
            last_chunk = session.get("last_chunk")
            form_chunk_id_int = int(form_chunk_id) if form_chunk_id else None
            last_chunk_int = int(last_chunk) if last_chunk else None
            
            logging.info(f"🔍 Session validation - Last: {last_chunk} (type: {type(last_chunk)}), Current: {form_chunk_id} (type: {type(form_chunk_id)})")
            logging.info(f"🔍 Converted - Last: {last_chunk_int}, Current: {form_chunk_id_int}")
            
            if last_chunk_int and last_chunk_int != form_chunk_id_int:
                error_msg = "⚠️ Finish your current recording before requesting a new one!"
                logging.warning(f"{error_msg} Last: {last_chunk_int}, Current: {form_chunk_id_int}")
                return error_msg
            
            # If it's the same chunk, allow the upload (might be a retry or legitimate submission)
            if last_chunk_int == form_chunk_id_int:
                logging.info(f"🔄 Processing upload for chunk {form_chunk_id_int} (session match - OK)")
            else:
                logging.info(f"🆕 Processing upload for new chunk {form_chunk_id_int}")

            # Generate filename with timestamp to prevent collisions
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
            audio_filename = f"{name}_{form_chunk_id}_{timestamp}.wav"
            audio_path = os.path.join(UPLOAD_FOLDER, audio_filename)

            logging.info(f"🎯 Processing audio upload - File: {audio_filename}, Path: {audio_path}")

            try:
                store_audio_file(audio_file, audio_path)
                logging.info("✅ Audio file stored successfully")
            except Exception as e:
                error_msg = f"❌ Failed to store audio file: {str(e)}"
                logging.error(error_msg)
                return error_msg, 500

            # ✅ Run update in background thread instead of Celery
            try:
                thread = threading.Thread(
                    target=update_chunk_in_background_thread,
                    args=(form_chunk_id, audio_path, name),
                    daemon=True  # Thread will die when main program exits
                )
                thread.start()
                logging.info(f"🚀 Background update thread started for chunk {form_chunk_id}")
            except Exception as e:
                logging.error(f"❌ Failed to start background thread: {e}", exc_info=True)
                return f"❌ Failed to process recording: {str(e)}", 500

            # Clear session and redirect
            session.pop("last_chunk", None)
            logging.info("✅ Upload completed successfully, redirecting to index")
            return redirect(url_for("index"))

        # GET request - show next text chunk
        logging.info("📋 GET request - fetching next text chunk")
        entry = get_next_text()
        if entry is None:
            logging.info("🏁 All recordings are done!")
            return "All recordings are done!"

        text, chunk_id = entry
        session["last_chunk"] = chunk_id
        logging.info(f"📄 Serving chunk {chunk_id} to user")

        return render_template("index.html", text=text, chunk_id=chunk_id)

    except Exception as e:
        logging.error(f"❌ Error in index route: {e}", exc_info=True)
        return "❌ Internal Server Error!", 500


# --------------------- DATABASE MANAGEMENT FUNCTIONS ---------------------
def get_all_chunks(page=1, per_page=50, search_query="", status_filter="all"):
    """Fetch chunks with pagination and filtering."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            # Base query
            base_query = "FROM chunks WHERE 1=1"
            params = []
            
            # Add search filter
            if search_query:
                base_query += " AND (text ILIKE %s OR CAST(chunk_id AS TEXT) ILIKE %s OR recorder ILIKE %s)"
                params.extend([f"%{search_query}%", f"%{search_query}%", f"%{search_query}%"])
            
            # Add status filter
            if status_filter == "done":
                base_query += " AND recording = 'done'"
            elif status_filter == "pending":
                base_query += " AND pending = 'yes'"
            elif status_filter == "available":
                base_query += " AND recording != 'done' AND pending != 'yes'"
            
            # Get total count
            cursor.execute(f"SELECT COUNT(*) {base_query}", params)
            total_count = cursor.fetchone()[0]
            
            # Get chunks with pagination
            offset = (page - 1) * per_page
            cursor.execute(f"""
                SELECT chunk_id, text, recording, pending, 
                       CASE WHEN LENGTH(text) > 100 THEN SUBSTRING(text FROM 1 FOR 100) || '...' 
                            ELSE text END as short_text,
                       audio_path, recorder, time, chunk_len
                {base_query} 
                ORDER BY chunk_id 
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            
            chunks = cursor.fetchall()
            
        PostgresConnector.release_connection(conn)
        
        return chunks, total_count
        
    except Exception as e:
        logging.error(f"❌ Error in get_all_chunks: {e}", exc_info=True)
        return [], 0

def add_new_chunk(text):
    """Add a new chunk to the database."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO chunks (text, recording, pending) 
                VALUES (%s, 'not_done', 'no') 
                RETURNING chunk_id
            """, (text,))
            chunk_id = cursor.fetchone()[0]
            conn.commit()
            
        PostgresConnector.release_connection(conn)
        logging.info(f"✅ New chunk added with ID: {chunk_id}")
        return chunk_id
        
    except Exception as e:
        logging.error(f"❌ Error adding new chunk: {e}", exc_info=True)
        return None

def delete_chunk(chunk_id):
    """Delete a chunk from the database."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM chunks WHERE chunk_id = %s", (chunk_id,))
            affected_rows = cursor.rowcount
            conn.commit()
            
        PostgresConnector.release_connection(conn)
        
        if affected_rows > 0:
            logging.info(f"✅ Chunk {chunk_id} deleted successfully")
            return True
        else:
            logging.warning(f"⚠️ No chunk found with ID: {chunk_id}")
            return False
            
    except Exception as e:
        logging.error(f"❌ Error deleting chunk {chunk_id}: {e}", exc_info=True)
        return False

def update_chunk_status(chunk_id, recording_status, pending_status):
    """Update chunk status."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE chunks 
                SET recording = %s, pending = %s 
                WHERE chunk_id = %s
            """, (recording_status, pending_status, chunk_id))
            affected_rows = cursor.rowcount
            conn.commit()
            
        PostgresConnector.release_connection(conn)
        
        if affected_rows > 0:
            logging.info(f"✅ Chunk {chunk_id} status updated")
            return True
        else:
            logging.warning(f"⚠️ No chunk found with ID: {chunk_id}")
            return False
            
    except Exception as e:
        logging.error(f"❌ Error updating chunk {chunk_id}: {e}", exc_info=True)
        return False

def get_database_stats():
    """Get database statistics."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_chunks,
                    COUNT(CASE WHEN recording = 'done' THEN 1 END) as completed_chunks,
                    COUNT(CASE WHEN pending = 'yes' THEN 1 END) as pending_chunks,
                    COUNT(CASE WHEN recording != 'done' AND pending != 'yes' THEN 1 END) as available_chunks
                FROM chunks
            """)
            stats = cursor.fetchone()
            
        PostgresConnector.release_connection(conn)
        
        return {
            'total': stats[0],
            'completed': stats[1],
            'pending': stats[2],
            'available': stats[3]
        }
        
    except Exception as e:
        logging.error(f"❌ Error getting database stats: {e}", exc_info=True)
        return {'total': 0, 'completed': 0, 'pending': 0, 'available': 0}

def get_speaker_stats():
    """Get detailed speaker/recorder statistics."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            # Get speaker statistics
            cursor.execute("""
                SELECT 
                    recorder,
                    COUNT(*) as total_recordings,
                    SUM(chunk_len) as total_characters,
                    AVG(chunk_len) as avg_characters,
                    MIN(time) as first_recording,
                    MAX(time) as last_recording,
                    COUNT(DISTINCT DATE(time)) as recording_days
                FROM chunks 
                WHERE recording = 'done' AND recorder IS NOT NULL AND recorder != ''
                GROUP BY recorder
                ORDER BY total_recordings DESC
            """)
            speaker_stats = cursor.fetchall()
            
            # Get overall statistics
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT recorder) as unique_speakers,
                    COUNT(*) as total_completed,
                    SUM(chunk_len) as total_characters,
                    AVG(chunk_len) as avg_characters_per_chunk,
                    MIN(time) as first_recording,
                    MAX(time) as last_recording
                FROM chunks 
                WHERE recording = 'done' AND recorder IS NOT NULL AND recorder != ''
            """)
            overall_stats = cursor.fetchone()
            
            # Get daily recording statistics
            cursor.execute("""
                SELECT 
                    DATE(time) as recording_date,
                    COUNT(*) as recordings_count,
                    COUNT(DISTINCT recorder) as unique_speakers,
                    SUM(chunk_len) as total_characters
                FROM chunks 
                WHERE recording = 'done' AND recorder IS NOT NULL AND recorder != '' AND time IS NOT NULL
                GROUP BY DATE(time)
                ORDER BY recording_date DESC
                LIMIT 30
            """)
            daily_stats = cursor.fetchall()
            
            # Get top performers by different metrics
            cursor.execute("""
                SELECT recorder, COUNT(*) as count
                FROM chunks 
                WHERE recording = 'done' AND recorder IS NOT NULL AND recorder != ''
                GROUP BY recorder
                ORDER BY count DESC
                LIMIT 10
            """)
            top_by_count = cursor.fetchall()
            
            cursor.execute("""
                SELECT recorder, SUM(chunk_len) as total_chars
                FROM chunks 
                WHERE recording = 'done' AND recorder IS NOT NULL AND recorder != '' AND chunk_len IS NOT NULL
                GROUP BY recorder
                ORDER BY total_chars DESC
                LIMIT 10
            """)
            top_by_characters = cursor.fetchall()
            
        PostgresConnector.release_connection(conn)
        
        return {
            'speaker_stats': speaker_stats,
            'overall_stats': overall_stats,
            'daily_stats': daily_stats,
            'top_by_count': top_by_count,
            'top_by_characters': top_by_characters
        }
        
    except Exception as e:
        logging.error(f"❌ Error getting speaker stats: {e}", exc_info=True)
        return {
            'speaker_stats': [],
            'overall_stats': None,
            'daily_stats': [],
            'top_by_count': [],
            'top_by_characters': []
        }

# --------------------- NEW DATABASE MANAGEMENT ROUTES ---------------------
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    """Admin login page."""
    if request.method == "POST":
        password = request.form.get("password")
        if password == ADMIN_PASSWORD:
            session['admin_logged_in'] = True
            flash("Successfully logged in!", "success")
            return redirect(url_for("admin_dashboard"))
        else:
            flash("Invalid password!", "error")
    
    return render_template("admin_login.html")

@app.route("/admin/logout")
def admin_logout():
    """Admin logout."""
    session.pop('admin_logged_in', None)
    flash("Successfully logged out!", "success")
    return redirect(url_for("admin_login"))

@app.route("/admin")
@admin_required
def admin_dashboard():
    """Admin dashboard with database overview."""
    try:
        stats = get_database_stats()
        return render_template("admin_dashboard.html", stats=stats)
    except Exception as e:
        logging.error(f"❌ Error in admin dashboard: {e}", exc_info=True)
        flash("Error loading dashboard", "error")
        return redirect(url_for("index"))

@app.route("/admin/database", methods=["GET", "POST"])
@admin_required
def database_interface():
    """Database management interface for running SQL queries."""
    query_result = None
    error_message = None
    execution_time = None
    
    # Common pre-built queries
    common_queries = {
        "Show all tables": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';",
        "Chunks overview": "SELECT recording, pending, COUNT(*) as count FROM chunks GROUP BY recording, pending ORDER BY recording, pending;",
        "Recent recordings": "SELECT chunk_id, recorder, time, chunk_len FROM chunks WHERE recording = 'done' ORDER BY time DESC LIMIT 20;",
        "Speaker statistics": "SELECT recorder, COUNT(*) as recordings, SUM(chunk_len) as total_chars FROM chunks WHERE recording = 'done' AND recorder IS NOT NULL GROUP BY recorder ORDER BY recordings DESC LIMIT 10;",
        "Chunks table structure": "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = 'chunks' ORDER BY ordinal_position;",
        "Database size info": "SELECT pg_size_pretty(pg_database_size(current_database())) as database_size;",
        "Pending chunks": "SELECT chunk_id, text, pending FROM chunks WHERE pending = 'yes' LIMIT 10;",
        "Available chunks": "SELECT chunk_id, LEFT(text, 50) || '...' as text_preview FROM chunks WHERE recording != 'done' AND pending != 'yes' LIMIT 10;"
    }
    
    if request.method == "POST":
        sql_query = request.form.get("sql_query", "").strip()
        query_type = request.form.get("query_type", "read_only")
        
        if not sql_query:
            error_message = "Please enter a SQL query"
        else:
            try:
                # Security check for read-only mode
                if query_type == "read_only":
                    # Only allow SELECT statements and some informational queries
                    sql_lower = sql_query.lower().strip()
                    allowed_starts = ['select', 'show', 'describe', 'explain', 'with']
                    
                    if not any(sql_lower.startswith(start) for start in allowed_starts):
                        error_message = "Read-only mode: Only SELECT, SHOW, DESCRIBE, EXPLAIN, and WITH queries are allowed"
                    elif any(keyword in sql_lower for keyword in ['insert', 'update', 'delete', 'drop', 'create', 'alter', 'truncate']):
                        error_message = "Read-only mode: Modification queries are not allowed"
                
                if not error_message:
                    import time
                    start_time = time.time()
                    
                    conn = PostgresConnector.get_connection()
                    with conn.cursor() as cursor:
                        cursor.execute(sql_query)
                        
                        # Get column names
                        columns = [desc[0] for desc in cursor.description] if cursor.description else []
                        
                        # Fetch results (limit to prevent memory issues)
                        if cursor.description:  # Query returns results
                            rows = cursor.fetchmany(1000)  # Limit to 1000 rows
                            if len(rows) == 1000:
                                flash("Results limited to 1000 rows", "info")
                        else:  # Query doesn't return results (e.g., INSERT, UPDATE)
                            rows = []
                            if query_type != "read_only":
                                conn.commit()
                                affected_rows = cursor.rowcount
                                flash(f"Query executed successfully. {affected_rows} rows affected.", "success")
                    
                    PostgresConnector.release_connection(conn)
                    end_time = time.time()
                    execution_time = round((end_time - start_time) * 1000, 2)  # Convert to milliseconds
                    
                    query_result = {
                        'columns': columns,
                        'rows': rows,
                        'row_count': len(rows) if rows else 0
                    }
                    
            except Exception as e:
                error_message = f"Query error: {str(e)}"
                logging.error(f"❌ Database query error: {e}", exc_info=True)
                try:
                    if conn:
                        conn.rollback()
                        PostgresConnector.release_connection(conn)
                except:
                    pass
    
    return render_template("database_interface.html", 
                         query_result=query_result,
                         error_message=error_message,
                         execution_time=execution_time,
                         common_queries=common_queries)

@app.route("/admin/chunks")
@admin_required
def view_chunks():
    """View all chunks with pagination and filtering."""
    try:
        page = int(request.args.get('page', 1))
        search_query = request.args.get('search', '')
        status_filter = request.args.get('status', 'all')
        per_page = 50
        
        chunks, total_count = get_all_chunks(page, per_page, search_query, status_filter)
        
        # Calculate pagination info
        total_pages = (total_count + per_page - 1) // per_page
        has_prev = page > 1
        has_next = page < total_pages
        
        return render_template("view_chunks.html", 
                             chunks=chunks, 
                             page=page, 
                             total_pages=total_pages,
                             has_prev=has_prev,
                             has_next=has_next,
                             search_query=search_query,
                             status_filter=status_filter,
                             total_count=total_count)
                             
    except Exception as e:
        logging.error(f"❌ Error viewing chunks: {e}", exc_info=True)
        flash("Error loading chunks", "error")
        return redirect(url_for("admin_dashboard"))

@app.route("/admin/chunks-with-audio")
@admin_required
def view_chunks_with_audio():
    """View all completed chunks with audio players."""
    try:
        page = int(request.args.get('page', 1))
        search_query = request.args.get('search', '')
        per_page = 25  # Smaller page size due to audio players
        
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            # Base query for completed chunks with audio
            base_query = "FROM chunks WHERE recording = 'done' AND audio_path IS NOT NULL"
            params = []
            
            # Add search filter
            if search_query:
                base_query += " AND (text ILIKE %s OR CAST(chunk_id AS TEXT) ILIKE %s OR recorder ILIKE %s)"
                params.extend([f"%{search_query}%", f"%{search_query}%", f"%{search_query}%"])
            
            # Get total count
            cursor.execute(f"SELECT COUNT(*) {base_query}", params)
            total_count = cursor.fetchone()[0]
            
            # Get chunks with pagination
            offset = (page - 1) * per_page
            cursor.execute(f"""
                SELECT chunk_id, text, recording, pending, audio_path, recorder, time, chunk_len
                {base_query} 
                ORDER BY chunk_id 
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            
            chunks = cursor.fetchall()
            
        PostgresConnector.release_connection(conn)
        
        # Calculate pagination info
        total_pages = (total_count + per_page - 1) // per_page
        has_prev = page > 1
        has_next = page < total_pages
        
        return render_template("view_chunks_with_audio.html", 
                             chunks=chunks, 
                             page=page, 
                             total_pages=total_pages,
                             has_prev=has_prev,
                             has_next=has_next,
                             search_query=search_query,
                             total_count=total_count)
                             
    except Exception as e:
        logging.error(f"❌ Error viewing chunks with audio: {e}", exc_info=True)
        flash("Error loading chunks with audio", "error")
        return redirect(url_for("admin_dashboard"))

@app.route("/admin/chunks/add", methods=["GET", "POST"])
@admin_required
def add_chunk():
    """Add a new chunk."""
    if request.method == "POST":
        text = request.form.get("text", "").strip()
        
        if not text:
            flash("Text is required!", "error")
            return render_template("add_chunk.html")
        
        chunk_id = add_new_chunk(text)
        if chunk_id:
            flash(f"Chunk added successfully with ID: {chunk_id}", "success")
            return redirect(url_for("view_chunks"))
        else:
            flash("Error adding chunk", "error")
    
    return render_template("add_chunk.html")

@app.route("/admin/chunks/delete/<int:chunk_id>", methods=["POST"])
@admin_required
def delete_chunk_route(chunk_id):
    """Delete a specific chunk."""
    try:
        if delete_chunk(chunk_id):
            flash(f"Chunk {chunk_id} deleted successfully", "success")
        else:
            flash(f"Error deleting chunk {chunk_id}", "error")
    except Exception as e:
        logging.error(f"❌ Error in delete route: {e}", exc_info=True)
        flash("Error deleting chunk", "error")
    
    return redirect(url_for("view_chunks"))

@app.route("/admin/chunks/update/<int:chunk_id>", methods=["POST"])
@admin_required
def update_chunk_route(chunk_id):
    """Update chunk status."""
    try:
        recording_status = request.form.get("recording_status")
        pending_status = request.form.get("pending_status")
        
        if update_chunk_status(chunk_id, recording_status, pending_status):
            flash(f"Chunk {chunk_id} updated successfully", "success")
        else:
            flash(f"Error updating chunk {chunk_id}", "error")
            
    except Exception as e:
        logging.error(f"❌ Error in update route: {e}", exc_info=True)
        flash("Error updating chunk", "error")
    
    return redirect(url_for("view_chunks"))

@app.route("/admin/reset_pending", methods=["POST"])
@admin_required
def reset_all_pending_chunks():
    """Reset all pending chunks back to available status."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE chunks 
                SET pending = 'no' 
                WHERE pending = 'yes' AND recording != 'done'
            """)
            reset_count = cursor.rowcount
            conn.commit()
            
        PostgresConnector.release_connection(conn)
        
        if reset_count > 0:
            flash(f"Successfully reset {reset_count} pending chunks to available status", "success")
            logging.info(f"✅ Admin manually reset {reset_count} pending chunks")
        else:
            flash("No pending chunks found to reset", "info")
            
    except Exception as e:
        logging.error(f"❌ Error resetting pending chunks: {e}", exc_info=True)
        flash("Error resetting pending chunks", "error")
    
    return redirect(url_for("admin_dashboard"))

@app.route("/admin/bulk_add", methods=["GET", "POST"])
@admin_required
def bulk_add_chunks():
    """Bulk add chunks from text file or textarea."""
    if request.method == "POST":
        # Check if file was uploaded
        if 'file' in request.files:
            file = request.files['file']
            if file.filename != '':
                try:
                    content = file.read().decode('utf-8')
                    lines = [line.strip() for line in content.split('\n') if line.strip()]
                    
                    added_count = 0
                    for line in lines:
                        if add_new_chunk(line):
                            added_count += 1
                    
                    flash(f"Successfully added {added_count} chunks from file", "success")
                    return redirect(url_for("view_chunks"))
                    
                except Exception as e:
                    flash(f"Error processing file: {str(e)}", "error")
        
        # Check textarea input
        text_input = request.form.get("text_input", "").strip()
        if text_input:
            lines = [line.strip() for line in text_input.split('\n') if line.strip()]
            added_count = 0
            for line in lines:
                if add_new_chunk(line):
                    added_count += 1
            
            flash(f"Successfully added {added_count} chunks", "success")
            return redirect(url_for("view_chunks"))
        
        flash("Please provide text input or upload a file", "error")
    
    return render_template("bulk_add.html")

@app.route("/admin/export_tsv")
@admin_required
def export_tsv():
    """Export only completed chunks data to TSV format."""
    try:
        conn = PostgresConnector.get_connection()
        with conn.cursor() as cursor:
            # Get only completed chunks with their data
            cursor.execute("""
                SELECT chunk_id, text, recording, pending, audio_path, recorder, time, chunk_len
                FROM chunks
                WHERE recording = 'done'
                ORDER BY chunk_id
            """)
            chunks = cursor.fetchall()
        
        PostgresConnector.release_connection(conn)
        
        # Create TSV content
        tsv_lines = []
        # Header row
        headers = ["chunk_id", "text", "recording", "pending", "audio_path", "recorder", "time", "chunk_len"]
        tsv_lines.append("\t".join(headers))
        
        # Data rows
        for chunk in chunks:
            row_data = []
            for item in chunk:
                if item is None:
                    row_data.append("")
                else:
                    # Convert to string and handle tabs/newlines in text
                    item_str = str(item).replace("\t", " ").replace("\n", " ").replace("\r", " ")
                    row_data.append(item_str)
            tsv_lines.append("\t".join(row_data))
        
        tsv_content = "\n".join(tsv_lines)
        
        # Create response with TSV file
        from flask import Response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"completed_recordings_{timestamp}.tsv"
        
        response = Response(
            tsv_content,
            mimetype="text/tab-separated-values",
            headers={"Content-disposition": f"attachment; filename={filename}"}
        )
        
        logging.info(f"✅ TSV export completed: {len(chunks)} completed recordings exported")
        return response
        
    except Exception as e:
        logging.error(f"❌ Error in TSV export: {e}", exc_info=True)
        flash("Error exporting data", "error")
        return redirect(url_for("admin_dashboard"))

@app.route("/admin/stats")
@admin_required
def view_stats():
    """View detailed recording and speaker statistics."""
    try:
        stats = get_speaker_stats()
        basic_stats = get_database_stats()
        return render_template("stats_view.html", 
                             stats=stats, 
                             basic_stats=basic_stats)
    except Exception as e:
        logging.error(f"❌ Error in stats view: {e}", exc_info=True)
        flash("Error loading statistics", "error")
        return redirect(url_for("admin_dashboard"))

# --------------------- DEBUG ROUTES ---------------------
@app.route("/debug/clear_session")
def clear_session():
    """Clear session for debugging purposes."""
    session.clear()
    logging.info("🗑️ Session cleared for debugging")
    return "Session cleared! You can now try recording again. <a href='/'>Go back</a>"

@app.route("/debug/status")
def debug_status():
    """Show current session and system status."""
    try:
        # Get session info
        session_info = dict(session)
        
        # Get recent uploads
        recent_files = []
        if os.path.exists(UPLOAD_FOLDER):
            files = os.listdir(UPLOAD_FOLDER)
            files.sort(key=lambda x: os.path.getmtime(os.path.join(UPLOAD_FOLDER, x)), reverse=True)
            recent_files = files[:5]  # Last 5 files
        
        # Get database stats
        conn = PostgresConnector.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM chunks WHERE recording = 'done'")
        completed_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM chunks WHERE pending = 'yes'")
        pending_count = cursor.fetchone()[0]
        PostgresConnector.release_connection(conn)
        
        return f"""
        <h2>Debug Status</h2>
        <h3>Session Info:</h3>
        <pre>{session_info}</pre>
        
        <h3>Recent Uploads:</h3>
        <ul>
        {"".join([f"<li>{f}</li>" for f in recent_files]) if recent_files else "<li>No uploads found</li>"}
        </ul>
        
        <h3>Database Stats:</h3>
        <ul>
        <li>Completed recordings: {completed_count}</li>
        <li>Pending chunks: {pending_count}</li>
        </ul>
        
        <p><a href="/debug/clear_session">Clear Session</a> | <a href="/">Go Back</a></p>
        """
        
    except Exception as e:
        return f"Error getting status: {e}"

# --------------------- BACKGROUND UPDATE FUNCTION ---------------------
def update_chunk_in_background_thread(chunk_id, audio_path, name):
    """Update chunk status in background thread after recording is completed."""
    try:
        logging.info(f"🔄 Starting background update for chunk {chunk_id}")
        logging.info(f"📁 Audio path: {audio_path}")
        logging.info(f"👤 Recorder: {name}")
        
        # Verify audio file exists
        if not os.path.exists(audio_path):
            raise FileNotFoundError(f"Audio file not found: {audio_path}")
        
        file_size = os.path.getsize(audio_path)
        logging.info(f"✅ Audio file verified: {file_size} bytes")
        
        # Get database connection
        conn = PostgresConnector.get_connection()
        
        with conn.cursor() as cursor:
            # Get text length from database
            try:
                cursor.execute("SELECT text FROM chunks WHERE chunk_id = %s", (chunk_id,))
                text_row = cursor.fetchone()
                chunk_len = len(text_row[0]) if text_row else 0
            except Exception as text_error:
                logging.warning(f"⚠️ Could not get text length for chunk {chunk_id}: {text_error}")
                chunk_len = 0
            
            # Update query with all required fields
            query = """
            UPDATE chunks 
            SET pending = %s,
                recording = %s, 
                audio_path = %s, 
                recorder = %s,
                time = %s,
                chunk_len = %s
            WHERE chunk_id = %s;
            """
            
            values = ("no", "done", audio_path, name, datetime.now(), chunk_len, chunk_id)
            logging.info(f"🗃️ Executing database update for chunk {chunk_id}")
            
            cursor.execute(query, values)
            rows_affected = cursor.rowcount
            conn.commit()

            if rows_affected > 0:
                logging.info(f"✅ Chunk {chunk_id} updated successfully in database ({rows_affected} rows affected)")
            else:
                logging.warning(f"⚠️ No rows affected when updating chunk {chunk_id}")

        PostgresConnector.release_connection(conn)
        logging.info("🔌 Database connection released")

    except Exception as e:
        logging.error(f"❌ Error in background update for chunk {chunk_id}: {e}", exc_info=True)
        try:
            if conn:
                conn.rollback()
                PostgresConnector.release_connection(conn)
        except:
            pass

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    debug = os.environ.get("FLASK_DEBUG", "False").lower() in ("1","true")
    # Option 1: Run without SSL on port 8000 (recommended with nginx)
    app.run(debug=debug, host="0.0.0.0", port=port)
    
    # Option 2: Run with SSL on port 8443 (direct Flask SSL)
    # app.run(debug=True, ssl_context=('cert.pem', 'key.pem'), host="0.0.0.0", port=8443)
    
    # Option 3: Run with SSL on port 443 (requires root privileges)
    # app.run(debug=True, ssl_context=('cert.pem', 'key.pem'), host="0.0.0.0", port=443)

