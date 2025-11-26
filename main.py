import mysql.connector
import threading
import time
import os
from flask import Flask, render_template_string, redirect

# --- ⚙️ DATABASE CONFIGURATION ---

# 1. SOURCE (OLD - Wasmer_hub)
SRC_CONFIG = {
    'host': 'db.fr-pari1.bengt.wasmernet.com',
    'port': 10272,
    'user': '2e92a19775bf8000c5e305b1d08d',
    'password': '06922e92-a197-7d33-8000-1faca763e7ea',
    'database': 'Wasmer_hub'
}

# 2. DESTINATION (NEW - Movie_hub)
DST_CONFIG = {
    'host': 'db.fr-pari1.bengt.wasmernet.com',
    'port': 10272,
    'user': '70bf8f01799d80003fa639a0bbf6',
    'password': '069270bf-8f01-7b5f-8000-6742801cae35',
    'database': 'Movie_hub'
}

app = Flask(__name__)
logs = []
status = "Idle"
is_running = False

# --- LOGGING HELPER ---
def log(msg):
    print(msg)
    logs.insert(0, msg)
    if len(logs) > 500: logs.pop()

# --- MIGRATION LOGIC ---
def get_conn(config):
    return mysql.connector.connect(**config)

def run_migration():
    global status, is_running
    is_running = True
    status = "Connecting to Databases..."
    log("🔌 Connecting to Source and Destination...")

    try:
        src = get_conn(SRC_CONFIG)
        dst = get_conn(DST_CONFIG)
        
        src_cur = src.cursor(dictionary=True)
        dst_cur = dst.cursor()
        
        # --- STEP 1: CLEAN DESTINATION ---
        status = "Cleaning Destination DB..."
        log("🧹 Cleaning Destination Database (Truncating tables)...")
        dst_cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        tables = ['movies', 'movie_meta', 'download_links', 'screenshots', 'episodes', 'movie_categories', 'categories']
        for t in tables:
            dst_cur.execute(f"TRUNCATE TABLE {t}")
        dst.commit()
        log("✅ Destination Cleaned.")

        # --- STEP 2: MIGRATE MOVIES (SPLIT & REVERSE) ---
        status = "Migrating Movies..."
        log("🎬 Fetching Movies from Source (Reverse Order)...")
        
        # Fetch ALL movies DESC
        src_cur.execute("SELECT * FROM movies ORDER BY id DESC")
        movies = src_cur.fetchall()
        total_movies = len(movies)
        log(f"📦 Found {total_movies} movies to migrate.")

        id_map = {} # Old_ID -> New_ID
        count = 0

        for m in movies:
            old_id = m['id']
            
            # 1. Insert into 'movies' (Basic Info)
            # Note: Hum naye columns ke liye defaults handle kar rahe hain
            sql_mov = """
                INSERT INTO movies 
                (slug, imdb_id, tmdb_id, youtube_id, title, description, poster_url, release_year, 
                 runtime, status, language, quality_label, audio_label, rating, views, director, cast, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val_mov = (
                m.get('slug'), m.get('imdb_id'), m.get('tmdb_id'), m.get('youtube_id'), m.get('title'), 
                m.get('description'), m.get('poster_url'), m.get('release_year'), m.get('runtime'), 
                'Released', 'Hindi', m.get('quality_label', 'HD'), m.get('audio_label', 'Hindi'), 
                m.get('rating', 0), m.get('views', 0), m.get('director'), m.get('cast'), m.get('created_at')
            )
            dst_cur.execute(sql_mov, val_mov)
            new_id = dst_cur.lastrowid
            id_map[old_id] = new_id

            # 2. Insert into 'movie_meta' (Technical Info - SPLIT)
            # Budget/Revenue purane DB me 'movies' table me hi the
            sql_meta = """
                INSERT INTO movie_meta (movie_id, budget, revenue, verdict, scraped_source)
                VALUES (%s, %s, %s, %s, 'Migration')
            """
            # Check keys exist, else None
            budget = m.get('budget', None)
            revenue = m.get('revenue', None)
            
            # Simple verdict logic demo
            verdict = "Unknown"
            
            dst_cur.execute(sql_meta, (new_id, budget, revenue, verdict))
            
            count += 1
            if count % 100 == 0:
                dst.commit()
                log(f"🚀 Migrated {count}/{total_movies} movies...")

        dst.commit()
        log(f"✅ Movies Migration Complete! ({count} records)")

        # --- STEP 3: MIGRATE DEPENDENTS ---
        
        # Function to migrate child tables
        def migrate_child(table_name, columns, fk_col='movie_id'):
            log(f"🔗 Migrating {table_name}...")
            src_cur.execute(f"SELECT * FROM {table_name}")
            rows = src_cur.fetchall()
            b_data = []
            
            # Build insert query dynamically
            placeholders = ", ".join(["%s"] * len(columns))
            cols_str = ", ".join(columns)
            query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

            for r in rows:
                old_fk = r.get(fk_col)
                if old_fk in id_map:
                    # Replace Old ID with New ID
                    new_fk = id_map[old_fk]
                    row_vals = []
                    for col in columns:
                        if col == fk_col: row_vals.append(new_fk)
                        else: row_vals.append(r.get(col))
                    b_data.append(tuple(row_vals))
            
            if b_data:
                # Batch insert (executemany is faster)
                # Split into chunks of 1000
                chunk_size = 1000
                for i in range(0, len(b_data), chunk_size):
                    dst_cur.executemany(query, b_data[i:i+chunk_size])
                    dst.commit()
                log(f"✅ {table_name}: Moved {len(b_data)} records.")
            else:
                log(f"⚠️ {table_name}: No valid records found.")

        # A. Download Links
        # Ensure columns match your DB Schema
        migrate_child('download_links', ['movie_id', 'label', 'link_url', 'file_size', 'type'])

        # B. Screenshots
        migrate_child('screenshots', ['movie_id', 'image_url'])

        # C. Categories & Relations
        # First Static Categories
        log("📦 Migrating Categories...")
        src_cur.execute("SELECT * FROM categories")
        cats = src_cur.fetchall()
        for c in cats:
            try:
                dst_cur.execute("INSERT INTO categories (category_name, slug) VALUES (%s, %s)", (c['category_name'], c['slug']))
            except: pass # Skip duplicates
        dst.commit()
        
        # Then Relations
        migrate_child('movie_categories', ['movie_id', 'category_id'])

        dst_cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        status = "Completed Successfully"
        log("🎉🎉 MIGRATION COMPLETED SUCCESSFULLY! 🎉🎉")

    except Exception as e:
        status = f"Error: {str(e)}"
        log(f"❌ CRITICAL ERROR: {str(e)}")
    
    finally:
        if 'src' in locals() and src.is_connected(): src.close()
        if 'dst' in locals() and dst.is_connected(): dst.close()
        is_running = False

# --- FLASK UI ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Smart DB Migrator</title>
    <meta http-equiv="refresh" content="2">
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: 'Courier New', monospace; padding: 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        .header { border-bottom: 1px solid #30363d; padding-bottom: 10px; margin-bottom: 20px; }
        .btn { background: #238636; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold; }
        .btn.disabled { background: #555; pointer-events: none; }
        .log-box { background: #010409; border: 1px solid #30363d; padding: 15px; height: 500px; overflow-y: scroll; border-radius: 6px; }
        .log-entry { margin-bottom: 5px; border-bottom: 1px solid #21262d; padding-bottom: 2px; }
        .status { color: #58a6ff; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🐍 Python Smart Migrator</h1>
            <p>Source: <b>Wasmer_hub</b> ➡️ Dest: <b>Movie_hub</b></p>
            <p>Status: <span class="status">{{ status }}</span></p>
            <br>
            {% if not is_running %}
                <a href="/start" class="btn">🚀 START MIGRATION</a>
            {% else %}
                <a href="#" class="btn disabled">⏳ RUNNING...</a>
            {% endif %}
        </div>
        <div class="log-box">
            {% for l in logs %}
                <div class="log-entry"> > {{ l }}</div>
            {% endfor %}
        </div>
    </div>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, logs=logs, status=status, is_running=is_running)

@app.route('/start')
def start():
    global is_running
    if not is_running:
        t = threading.Thread(target=run_migration)
        t.start()
    return redirect('/')

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
