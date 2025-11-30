import mysql.connector
import threading
import time
import os
from flask import Flask, render_template_string, redirect

# -- ⚙️ DATABASE CONFIGURATION --

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

def log(msg):
    print(msg)
    logs.insert(0, msg)
    if len(logs) > 500: logs.pop()

def get_conn(config):
    return mysql.connector.connect(**config)

def run_migration():
    global status, is_running
    is_running = True
    status = "Connecting..."
    log("🔌 Connecting to Source and Destination...")

    try:
        src = get_conn(SRC_CONFIG)
        dst = get_conn(DST_CONFIG)
        
        src_cur = src.cursor(dictionary=True)
        dst_cur = dst.cursor()
        
        # --- PRE-MIGRATION CHECK ---
        src_cur.execute("SELECT COUNT(*) as c FROM movies")
        src_count = src_cur.fetchone()['c']
        log(f"📊 Source contains {src_count} movies.")

        # --- STEP 1: CLEAN DESTINATION ---
        status = "Cleaning Destination DB..."
        log("🧹 Cleaning Destination Tables...")
        dst_cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        tables = ['movies', 'movie_meta', 'download_links', 'screenshots', 'episodes', 'movie_categories', 'categories']
        for t in tables:
            dst_cur.execute(f"TRUNCATE TABLE {t}")
        dst.commit()
        log("✅ Destination Cleaned.")

        # --- STEP 2: MIGRATE MOVIES (Reverse Order) ---
        status = "Migrating Movies..."
        log("🎬 Fetching Movies (Reverse Order)...")
        
        # Fetch ALL movies DESC
        src_cur.execute("SELECT * FROM movies ORDER BY id DESC")
        movies = src_cur.fetchall()
        
        id_map = {} # Old_ID -> New_ID
        count = 0

        # Query prepare kar rahe hain taaki speed fast ho
        sql_mov = """
            INSERT INTO movies 
            (slug, imdb_id, tmdb_id, youtube_id, title, original_title, description, tagline, 
             poster_url, backdrop_url, release_year, release_date, runtime, status, 
             language, country, is_series, quality_label, audio_label, subtitle_label,
             rating, views, director, cast, extra_details, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        sql_meta = """
            INSERT INTO movie_meta (movie_id, budget, revenue, verdict, scraped_source)
            VALUES (%s, %s, %s, 'Unknown', 'Migration')
        """

        for m in movies:
            old_id = m['id']
            
            # Data Mapping (Handle Missing Columns Safely)
            val_mov = (
                m.get('slug'), m.get('imdb_id'), m.get('tmdb_id'), m.get('youtube_id'), 
                m.get('title'), m.get('original_title'), m.get('description'), m.get('tagline'), 
                m.get('poster_url'), m.get('backdrop_url'), m.get('release_year'), m.get('release_date'), 
                m.get('runtime'), m.get('status', 'Released'), m.get('language', 'Hindi'), m.get('country'),
                m.get('is_series', 0), m.get('quality_label', 'HD'), m.get('audio_label', 'Hindi'), m.get('subtitle_label'),
                m.get('rating', 0), m.get('views', 0), m.get('director'), m.get('cast'), 
                m.get('extra_details'), m.get('created_at')
            )
            
            dst_cur.execute(sql_mov, val_mov)
            new_id = dst_cur.lastrowid
            id_map[old_id] = new_id

            # Meta Data Insert
            dst_cur.execute(sql_meta, (new_id, m.get('budget'), m.get('revenue')))
            
            count += 1
            if count % 200 == 0:
                dst.commit()
                log(f"🚀 Migrated {count}/{src_count} movies...")

        dst.commit()
        log(f"✅ Movies Migration Complete! ({count} records)")

        # --- STEP 3: DYNAMIC CHILD TABLES ---
        
        def migrate_child_table(table_name):
            log(f"🔗 Analyzing {table_name}...")
            
            # 1. Get Source Columns
            src_cur.execute(f"SHOW COLUMNS FROM {table_name}")
            src_cols = [c['Field'] for c in src_cur.fetchall() if c['Field'] != 'id']
            
            # 2. Get Dest Columns
            dst_cur.execute(f"SHOW COLUMNS FROM {table_name}")
            dst_cols = [c[0] for c in dst_cur.fetchall() if c[0] != 'id']
            
            # 3. Find Common Columns (Jo dono me hain)
            common_cols = list(set(src_cols) & set(dst_cols))
            
            if not common_cols:
                log(f"⚠️ Skipping {table_name} (No common columns)")
                return

            # Ensure movie_id is present for mapping
            if 'movie_id' not in common_cols:
                log(f"⚠️ Skipping {table_name} (No movie_id found)")
                return

            log(f"📥 Migrating {table_name} (Columns: {', '.join(common_cols)})...")

            # Select Query
            src_cur.execute(f"SELECT {', '.join(common_cols)} FROM {table_name}")
            rows = src_cur.fetchall()
            
            if not rows:
                log(f"ℹ️ {table_name} is empty.")
                return

            # Insert Query
            placeholders = ", ".join(["%s"] * len(common_cols))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(common_cols)}) VALUES ({placeholders})"
            
            batch_data = []
            moved_count = 0
            
            for r in rows:
                old_fk = r['movie_id']
                if old_fk in id_map:
                    # Update ID
                    r['movie_id'] = id_map[old_fk]
                    
                    # Prepare tuple
                    row_vals = tuple(r[c] for c in common_cols)
                    batch_data.append(row_vals)
                    moved_count += 1
            
            # Execute Batch
            chunk_size = 1000
            for i in range(0, len(batch_data), chunk_size):
                dst_cur.executemany(insert_sql, batch_data[i:i+chunk_size])
                dst.commit()
            
            log(f"✅ {table_name}: Moved {moved_count} records.")

        # Migrate Children
        migrate_child_table('download_links')
        migrate_child_table('screenshots')
        migrate_child_table('movie_categories')

        # Categories Static
        log("📦 Migrating Categories...")
        src_cur.execute("SELECT * FROM categories")
        cats = src_cur.fetchall()
        for c in cats:
            try:
                dst_cur.execute("INSERT INTO categories (category_name, slug) VALUES (%s, %s)", (c['category_name'], c['slug']))
            except: pass
        dst.commit()

        # --- FINAL VERIFICATION ---
        dst_cur.execute("SELECT COUNT(*) FROM movies")
        final_count = dst_cur.fetchone()[0]
        
        log("="*30)
        log(f"🏁 FINAL REPORT:")
        log(f"🔹 Source Movies: {src_count}")
        log(f"🔹 Destination Movies: {final_count}")
        
        if src_count == final_count:
            status = "Success (100% Match)"
            log("✅ SUCCESS: Data count matches perfectly!")
        else:
            status = "Warning (Count Mismatch)"
            log("⚠️ WARNING: Some rows might be missing.")
            
        dst_cur.execute("SET FOREIGN_KEY_CHECKS = 1")

    except Exception as e:
        status = f"Error: {str(e)}"
        log(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        if 'src' in locals() and src.is_connected(): src.close()
        if 'dst' in locals() and dst.is_connected(): dst.close()
        is_running = False

# --- FLASK UI ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Safe DB Migrator</title>
    <meta http-equiv="refresh" content="2">
    <style>
        body { background-color: #0d1117; color: #c9d1d9; font-family: 'Courier New', monospace; padding: 20px; }
        .log-box { background: #010409; border: 1px solid #30363d; padding: 15px; height: 600px; overflow-y: scroll; border-radius: 6px; }
        .log-entry { margin-bottom: 5px; border-bottom: 1px solid #21262d; }
        .btn { background: #238636; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block; margin-top: 10px;}
    </style>
</head>
<body>
    <h1>🛡️ 100% Safe DB Migrator</h1>
    <p>Status: <b>{{ status }}</b></p>
    <div class="log-box">
        {% for l in logs %}
            <div class="log-entry"> > {{ l }}</div>
        {% endfor %}
    </div>
    {% if not is_running %}
        <a href="/start" class="btn">🚀 START SECURE MIGRATION</a>
    {% endif %}
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
