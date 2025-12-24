import sqlite3
import os
import requests
import zipfile
import io
import config


def init_database():
    print("--- 1. Initialising Vigilo Database (Schema Fix) ---")
    
    db_dir = os.path.dirname(config.DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)

    conn = sqlite3.connect(config.DB_PATH)
    c = conn.cursor()
    
    c.execute("PRAGMA journal_mode=WAL;")
    
    print("[1/4] Creating table: frontier")
    c.execute("""
        CREATE TABLE IF NOT EXISTS frontier (
            url TEXT PRIMARY KEY,
            domain TEXT,
            priority INTEGER DEFAULT 10,
            added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            status INTEGER DEFAULT 0,
            retry_count INTEGER DEFAULT 0
        )
    """)
    
    print("      Creating partial index...")
    c.execute("DROP INDEX IF EXISTS idx_frontier_dispatch")
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_frontier_pending 
        ON frontier(priority ASC, added_at ASC) 
        WHERE status = 0
    """)
    
    print("[2/4] Creating table: visited")
    c.execute("""
        CREATE TABLE IF NOT EXISTS visited (
            url TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            keywords TEXT,
            content TEXT,
            h1 TEXT,
            h2 TEXT,
            language TEXT,
            out_links INTEGER,
            crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            domain_rank INTEGER DEFAULT 10000000 
        )
    """)
    
    print("[3/4] Creating table: search_index (FTS5)")
    c.execute("DROP TABLE IF EXISTS search_index")
    c.execute("""
        CREATE VIRTUAL TABLE search_index USING fts5(
            url UNINDEXED, 
            title, 
            description, 
            content, 
            h1, 
            h2, 
            important_text,
            tokenize='porter unicode61' 
        )
    """)
    
    print("[4/4] Creating table: domain_authority")
    c.execute("""
        CREATE TABLE IF NOT EXISTS domain_authority (
            domain TEXT PRIMARY KEY, 
            rank INTEGER
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_authority_rank ON domain_authority(rank)")
    
    conn.commit()
    conn.close()
    print("--- Schema Setup Complete ---")


def download_and_import_ranks():
    print("\n--- 2. Importing Authority Data ---")
    print("      Downloading Tranco Top 1M list...")
    
    url = "https://tranco-list.eu/top-1m.csv.zip"
    
    try:
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_filename = z.namelist()[0]
        
        print(f"      Extracting {csv_filename}...")
        
        domain_ranks = []
        with z.open(csv_filename) as f:
            for line in f:
                parts = line.decode('utf-8').strip().split(',')
                if len(parts) == 2:
                    rank = int(parts[0])
                    domain = parts[1]
                    if rank <= 1000000:
                        domain_ranks.append((domain, rank))
        
        print(f"      Parsed {len(domain_ranks)} domains.")
        print("      Bulk inserting into database...")
        
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        
        c.execute("PRAGMA synchronous=OFF") 
        c.execute("PRAGMA journal_mode=MEMORY") 
        c.execute("BEGIN IMMEDIATE")
        
        c.executemany("INSERT OR IGNORE INTO domain_authority (domain, rank) VALUES (?, ?)", domain_ranks)
        
        conn.commit()
        conn.close()
        
        print("--- Authority Import Complete ---")
        
    except Exception as e:
        print(f"Error importing ranks: {e}")


if __name__ == "__main__":
    init_database()
    print("\n[SUCCESS] Vigilo Database is ready for crawling.")