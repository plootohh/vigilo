import requests
import zipfile
import io
import sqlite3
import os
import config

def download_and_import_ranks():
    print("--- Vigilo Authority System ---")
    print("[1/4] Downloading Tranco Top 1M list (approx 50MB)...")
    
    url = "https://tranco-list.eu/top-1m.csv.zip"
    
    try:
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_filename = z.namelist()[0]
        
        print(f"[2/4] Extracting {csv_filename}...")
        
        domain_ranks = []
        with z.open(csv_filename) as f:
            for line in f:
                parts = line.decode('utf-8').strip().split(',')
                if len(parts) == 2:
                    rank = int(parts[0])
                    domain = parts[1]
                    if rank <= 1000000:
                        domain_ranks.append((domain, rank))
        
        print(f"[3/4] Parsed {len(domain_ranks)} domains.")
        
        print("[4/4] Importing to Database...")
        conn = sqlite3.connect(config.DB_PATH)
        c = conn.cursor()
        
        c.execute("DROP TABLE IF EXISTS domain_authority")
        c.execute("CREATE TABLE domain_authority (domain TEXT PRIMARY KEY, rank INTEGER)")
        c.execute("CREATE INDEX idx_authority_rank ON domain_authority(rank)")
        
        c.execute("BEGIN IMMEDIATE")
        c.executemany("INSERT OR IGNORE INTO domain_authority (domain, rank) VALUES (?, ?)", domain_ranks)
        conn.commit()
        conn.close()
        
        print("Success! Your engine now knows the top 200,000 websites on Earth.")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if not os.path.exists(config.DB_PATH):
        print("Error: DB not found. Run the crawler at least once first.")
    else:
        download_and_import_ranks()