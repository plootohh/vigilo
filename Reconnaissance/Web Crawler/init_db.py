import sqlite3
import os
import config

def init_database():
    print("--- Initializing Vigilo Database (Fresh Start) ---")
    
    conn = sqlite3.connect(config.DB_PATH)
    c = conn.cursor()
    print("[1/4] Creating table: frontier")
    c.execute("""
        CREATE TABLE IF NOT EXISTS frontier (
            url TEXT PRIMARY KEY,
            domain TEXT,
            priority INTEGER DEFAULT 10,
            added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            retry_count INTEGER DEFAULT 0
        )
    """)
    
    print("[2/4] Creating table: visited")
    c.execute("""
        CREATE TABLE IF NOT EXISTS visited (
            url TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            keywords TEXT,
            content TEXT,
            language TEXT,
            out_links INTEGER,
            crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            domain_rank INTEGER DEFAULT 10000000 
        )
    """)
    
    print("[3/4] Creating table: search_index (Smart Search)")
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
    print("\n--- Success! Database ready for Intelligence Engine. ---")
    print("Next Steps:")
    print("1. Run 'python manage_ranks.py' to re-import the top 1M sites.")
    print("2. Run 'python run_crawler.py' to start indexing.")

if __name__ == "__main__":
    init_database()