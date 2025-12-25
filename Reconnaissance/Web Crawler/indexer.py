import sqlite3, time, os, sys, config
from datetime import datetime


# Configuration
BATCH_SIZE = 5000
STATE_FILE = "indexer_state.txt"
RECYCLE_CONN_EVERY = 20


def get_db():
    conn = sqlite3.connect(config.DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA cache_size=-500000;")
    conn.execute("PRAGMA mmap_size=30000000000;")
    return conn


def get_last_indexed_id():
    if not os.path.exists(STATE_FILE):
        return 0
    try:
        with open(STATE_FILE, "r") as f:
            content = f.read().strip()
            return int(content) if content else 0
    except Exception:
        return 0


def update_last_indexed_id(rowid):
    try:
        with open(STATE_FILE, "w") as f:
            f.write(str(rowid))
    except Exception as e:
        print(f"Error saving state: {e}")


def check_for_reset(conn, last_id):
    try:
        c = conn.cursor()
        c.execute("SELECT MAX(rowid) FROM visited")
        result = c.fetchone()
        max_db_id = result[0] if result and result[0] is not None else 0
        
        if last_id > max_db_id:
            print(f" [!] State Mismatch: Indexer at {last_id} but DB max is {max_db_id}. Resetting to 0.")
            return 0
        return last_id
    except:
        return last_id


def get_wal_size():
    try:
        wal_path = f"{config.DB_PATH}-wal"
        if os.path.exists(wal_path):
            return os.path.getsize(wal_path) / (1024 * 1024)
    except:
        pass
    return 0.0


def run_indexer():
    print("--- Vigilo Indexer ---")
    
    conn = get_db()
    
    last_id = get_last_indexed_id()
    last_id = check_for_reset(conn, last_id)
    update_last_indexed_id(last_id)
    
    print(f"Resuming indexing from ID: {last_id}")
    
    batch_count = 0
    
    while True:
        try:
            if batch_count >= RECYCLE_CONN_EVERY:
                conn.close()
                time.sleep(0.5) 
                conn = get_db()
                batch_count = 0

            sql_fetch = """
                SELECT rowid, url, title, description, content, h1, h2, important_text
                FROM visited 
                WHERE rowid > ? 
                AND title IS NOT NULL
                ORDER BY rowid ASC
                LIMIT ?
            """
            
            c = conn.cursor()
            c.execute(sql_fetch, (last_id, BATCH_SIZE))
            rows = c.fetchall()
            
            if not rows:
                sys.stdout.write(f"\r [{datetime.now().strftime('%H:%M:%S')}] Waiting for new content... (WAL: {get_wal_size():.1f}MB)")
                sys.stdout.flush()
                time.sleep(5)
                continue
            
            start_time = time.time()
            timestamp = datetime.now().strftime("%H:%M:%S")
            start_id = rows[0][0]
            end_id = rows[-1][0]
            wal_mb = get_wal_size()
            
            print(f"\n[{timestamp}] Batch Start | IDs: {start_id}-{end_id} | Size: {len(rows)} | WAL: {wal_mb:.1f}MB")
            
            insert_data = []
            max_id_in_batch = last_id
            
            for r in rows:
                row_id = r[0]
                if row_id > max_id_in_batch:
                    max_id_in_batch = row_id
                
                insert_data.append((
                    r[1], # url
                    r[2], # title
                    r[3], # description
                    r[4], # content
                    r[5] if r[5] else "", # h1
                    r[6] if r[6] else "", # h2
                    r[7] if r[7] else ""  # important_text
                ))

            c.execute("BEGIN IMMEDIATE")
            c.executemany("""
                INSERT INTO search_index (url, title, description, content, h1, h2, important_text) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, insert_data)
            conn.commit()
            
            update_last_indexed_id(max_id_in_batch)
            last_id = max_id_in_batch
            batch_count += 1
            
            elapsed = time.time() - start_time
            rate = int(len(rows)/elapsed) if elapsed > 0 else 0
            print(f"       -> Finished in {elapsed:.2f}s ({rate} docs/sec)")
            
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                print(f"\n [!] DB Locked. Retrying...")
                time.sleep(1)
            else:
                print(f"\n [!] Database Error: {e}")
                time.sleep(5)
        except KeyboardInterrupt:
            print("\n [!] Stopping Indexer...")
            conn.close()
            sys.exit()
        except Exception as e:
            print(f"\n [!] Indexer Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_indexer()