import sqlite3
import time
import os
import config


BATCH_SIZE = 5000
STATE_FILE = "indexer_state.txt"


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


def run_indexer():
    print("--- Vigilo Background Indexer (Watermark Method) ---")
    
    last_id = get_last_indexed_id()
    print(f"Resuming indexing from ID: {last_id}")
    
    conn = get_db()
    
    while True:
        try:
            start_time = time.time()
            
            sql_fetch = """
                SELECT rowid, url, title, description, content, h1, h2, important_text
                FROM visited 
                WHERE rowid > ? 
                AND title IS NOT NULL
                LIMIT ?
            """
            
            c = conn.cursor()
            c.execute(sql_fetch, (last_id, BATCH_SIZE))
            rows = c.fetchall()
            
            if not rows:
                time.sleep(5)
                continue
                
            print(f"Indexing batch of {len(rows)} documents...")
            
            insert_data = []
            max_id_in_batch = last_id
            
            for r in rows:
                row_id = r[0]
                if row_id > max_id_in_batch:
                    max_id_in_batch = row_id
                
                insert_data.append((
                    r[1],
                    r[2],
                    r[3],
                    r[4],
                    r[5] if r[5] else "",
                    r[6] if r[6] else "",
                    r[7] if r[7] else ""
                ))

            c.execute("BEGIN IMMEDIATE")
            c.executemany("""
                INSERT INTO search_index (url, title, description, content, h1, h2, important_text) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, insert_data)
            conn.commit()
            
            update_last_indexed_id(max_id_in_batch)
            last_id = max_id_in_batch
            
            elapsed = time.time() - start_time
            rate = int(len(rows)/elapsed) if elapsed > 0 else 0
            print(f"Indexed {len(rows)} docs in {elapsed:.2f}s ({rate} docs/sec)")
            
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                time.sleep(1)
            else:
                print(f"Indexer Database Error: {e}")
                time.sleep(5)
        except Exception as e:
            print(f"Indexer Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_indexer()