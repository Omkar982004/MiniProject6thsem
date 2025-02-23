import os
import sqlite3
import hashlib
from flask import Flask, request, jsonify, Response, send_from_directory, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Define paths using Railway's persistent directory.
# If RAILWAY_PERSISTENT_DIR is not set, default to /data.
PERSISTENT_DIR = os.getenv("RAILWAY_PERSISTENT_DIR", "/data")
os.makedirs(PERSISTENT_DIR, exist_ok=True)  # Ensure the base persistent directory exists
print("Persistent storage directory set to:", PERSISTENT_DIR)

# Database path inside persistent storage.
DB_PATH = os.path.join(PERSISTENT_DIR, "file_chunks.db")
print("Database path set to:", DB_PATH)

# Define an assets directory inside persistent storage.
ASSETS_DIR = os.path.join(PERSISTENT_DIR, "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)  # Create the assets folder if it doesn't exist
print("Assets directory set to:", ASSETS_DIR)

# Use ASSETS_DIR to store file-related assets.
UPLOAD_ROOT = os.path.join(ASSETS_DIR, "chunks")
TMP_FOLDER = os.path.join(UPLOAD_ROOT, "tmp")
FOLDERS = ['folder1', 'folder2', 'folder3']  # Used for DFS (chunked files)
NODFS_FOLDER = os.path.join(UPLOAD_ROOT, "nodfs")  # Used for no-DFS (whole file)
CHUNK_SIZE = 1024 * 1024  # 1 MB

# Ensure required directories exist in persistent storage.
os.makedirs(TMP_FOLDER, exist_ok=True)
os.makedirs(NODFS_FOLDER, exist_ok=True)
for folder in FOLDERS:
    os.makedirs(os.path.join(UPLOAD_ROOT, folder), exist_ok=True)

print("Upload root set to:", UPLOAD_ROOT)
print("Temporary folder set to:", TMP_FOLDER)
print("No-DFS folder set to:", NODFS_FOLDER)
for folder in FOLDERS:
    print(f"DFS folder '{folder}' is set up at:", os.path.join(UPLOAD_ROOT, folder))

def get_folder_path(folder):
    path = os.path.join(UPLOAD_ROOT, folder)
    os.makedirs(path, exist_ok=True)
    return path

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Table for DFS pipeline (chunked files)
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            file_hash TEXT,
            total_chunks INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            chunk_order INTEGER,
            chunk_hash TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id)
        )
    ''')
    # Table for no-DFS pipeline (whole files)
    c.execute('''
        CREATE TABLE IF NOT EXISTS files_nodfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            file_hash TEXT,
            file_size INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print("Database initialized (tables created if not existing).")

init_db()

def process_and_chunk_file(file_path, original_filename):
    """
    Reads the file in 1 MB chunks, computes the full file hash and each chunk's hash.
    Writes each chunk (replicated) into all three DFS folders.
    Returns (full_hash, total_chunks, chunk_info) where chunk_info is a list of (chunk_order, chunk_hash).
    """
    base_name, extension = os.path.splitext(original_filename)
    sha_full = hashlib.sha256()
    chunk_info = []
    counter = 1

    with open(file_path, 'rb') as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            sha_full.update(data)
            chunk_hash = hashlib.sha256(data).hexdigest()
            chunk_info.append((counter, chunk_hash))
            chunk_filename = f"{base_name}_chunk{counter}{extension}"
            # Write the chunk into each DFS folder
            for folder in FOLDERS:
                folder_path = get_folder_path(folder)
                with open(os.path.join(folder_path, chunk_filename), 'wb') as cf:
                    cf.write(data)
            counter += 1

    full_hash = sha_full.hexdigest()
    total_chunks = counter - 1
    return full_hash, total_chunks, chunk_info

@app.route('/')
def index():
    return "Hello from Flask..."

@app.route('/upload', methods=['POST'])
def upload_file():
    """DFS upload: Process file into chunks, replicate chunks, record metadata."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Save the file temporarily for processing.
    tmp_file_path = os.path.join(TMP_FOLDER, file.filename)
    file.save(tmp_file_path)
    print(f"File {file.filename} saved temporarily at {tmp_file_path}")

    # Process: chunk file, compute hashes, replicate chunks.
    full_hash, total_chunks, chunk_info = process_and_chunk_file(tmp_file_path, file.filename)
    os.remove(tmp_file_path)  # Remove temporary file.
    print(f"File {file.filename} processed: {total_chunks} chunks created with full hash {full_hash}")

    # Insert file record into DFS database.
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO files (filename, file_hash, total_chunks) VALUES (?, ?, ?)',
              (file.filename, full_hash, total_chunks))
    file_id = c.lastrowid
    for order, chash in chunk_info:
        c.execute('INSERT INTO chunks (file_id, chunk_order, chunk_hash) VALUES (?, ?, ?)',
                  (file_id, order, chash))
    conn.commit()
    conn.close()
    print(f"File metadata for {file.filename} inserted into database with file_id {file_id}")

    return jsonify({'data': 'File uploaded, chunked, and replicated successfully!', 'file_id': file_id})

@app.route('/list', methods=['GET'])
def list_files():
    """Returns a unified list of DFS-uploaded files (with original filename and total chunk count)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, filename, total_chunks FROM files')
    rows = c.fetchall()
    conn.close()
    files = [{'id': row[0], 'filename': row[1], 'total_chunks': row[2]} for row in rows]
    return jsonify({'files': files})

@app.route('/download_chunk', methods=['GET'])
def download_chunk():
    """
    Downloads a single DFS file chunk.
    Expects query parameters: file_id and chunk_order.
    Uses a round-robin strategy to pick the folder.
    """
    file_id = request.args.get('file_id')
    chunk_order = request.args.get('chunk_order')
    if not file_id or not chunk_order:
        return jsonify({'error': 'Missing file_id or chunk_order parameter'}), 400
    try:
        chunk_order = int(chunk_order)
    except ValueError:
        return jsonify({'error': 'Invalid chunk_order parameter'}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT filename FROM files WHERE id=?', (file_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'File not found'}), 404

    filename = row[0]
    base_name, extension = os.path.splitext(filename)
    folder_index = (chunk_order - 1) % len(FOLDERS)
    folder = FOLDERS[folder_index]
    folder_path = get_folder_path(folder)
    chunk_filename = f"{base_name}_chunk{chunk_order}{extension}"
    if not os.path.exists(os.path.join(folder_path, chunk_filename)):
        return jsonify({'error': 'Chunk file not found'}), 404

    return send_from_directory(folder_path, chunk_filename, as_attachment=True)

@app.route('/download', methods=['GET'])
def download_file():
    """
    Streams the merged DFS file by reading chunks sequentially in round-robin order.
    """
    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id parameter'}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, filename, total_chunks FROM files WHERE id=?', (file_id,))
    file_record = c.fetchone()
    conn.close()
    if not file_record:
        return jsonify({'error': 'File not found'}), 404

    file_id, filename, total_chunks = file_record
    base_name, extension = os.path.splitext(filename)

    def generate():
        for order in range(1, total_chunks + 1):
            folder_index = (order - 1) % len(FOLDERS)
            folder = FOLDERS[folder_index]
            chunk_filename = f"{base_name}_chunk{order}{extension}"
            folder_path = get_folder_path(folder)
            chunk_path = os.path.join(folder_path, chunk_filename)
            if os.path.exists(chunk_path):
                with open(chunk_path, 'rb') as cf:
                    yield cf.read()
            else:
                abort(404, description=f"Chunk {order} missing")
    response = Response(generate(), mimetype='application/octet-stream')
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response

@app.route('/db_view', methods=['GET'])
def db_view():
    """
    Returns the contents of the SQLite database (files and chunks tables)
    for backend administrators.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM files')
    files = [dict(row) for row in c.fetchall()]
    c.execute('SELECT * FROM chunks')
    chunks = [dict(row) for row in c.fetchall()]
    # Also include the no-DFS table view.
    c.execute('SELECT * FROM files_nodfs')
    nodfs_files = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({'files': files, 'chunks': chunks, 'files_nodfs': nodfs_files})

# ======================= NO-DFS Endpoints =======================

@app.route('/upload_nodfs', methods=['POST'])
def upload_nodfs():
    """No-DFS upload: Saves the file as a whole in a single folder and records metadata."""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    # Save the file directly in the NODFS_FOLDER.
    save_path = os.path.join(NODFS_FOLDER, file.filename)
    file.save(save_path)
    print(f"File {file.filename} saved to no-DFS folder at {save_path}")

    # Compute file hash and size.
    sha = hashlib.sha256()
    file_size = os.path.getsize(save_path)
    with open(save_path, 'rb') as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            sha.update(data)
    file_hash = sha.hexdigest()
    print(f"File {file.filename} computed hash: {file_hash} and size: {file_size} bytes")

    # Record metadata in files_nodfs table.
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO files_nodfs (filename, file_hash, file_size) VALUES (?, ?, ?)',
              (file.filename, file_hash, file_size))
    file_id = c.lastrowid
    conn.commit()
    conn.close()
    print(f"No-DFS file metadata inserted with file_id {file_id}")

    return jsonify({'data': 'File uploaded successfully (no DFS)!', 'file_id': file_id})

@app.route('/list_nodfs', methods=['GET'])
def list_nodfs():
    """Returns a list of files stored via the no-DFS method."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id, filename, file_size FROM files_nodfs')
    rows = c.fetchall()
    conn.close()
    files = [{'id': row[0], 'filename': row[1], 'file_size': row[2]} for row in rows]
    return jsonify({'files': files})

@app.route('/download_nodfs', methods=['GET'])
def download_nodfs():
    """
    Streams the whole file stored in the NODFS_FOLDER.
    """
    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id parameter'}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT filename FROM files_nodfs WHERE id=?', (file_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'File not found'}), 404

    filename = row[0]
    return send_from_directory(NODFS_FOLDER, filename, as_attachment=True)

if __name__ == '__main__':
    # Use the PORT environment variable (set by Railway) and host '0.0.0.0'
    port = int(os.environ.get("PORT", 5000))
    print("Starting app on port", port)
    app.run(debug=False, host="0.0.0.0", port=port)
