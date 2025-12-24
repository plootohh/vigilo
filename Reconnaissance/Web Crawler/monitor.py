import sqlite3
import time
import os
import sys
from collections import deque
from datetime import datetime

# Adjust path if needed to find config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import config


# --- CONFIGURATION ---
SPEED_WINDOW_SECONDS = 180 


def get_db():
    conn = sqlite3.connect(config.DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def get_file_size_mb(path):
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except OSError:
        return 0.0


def monitor():
    print("Initialising Monitor...")
    
    history = deque()
    
    while True:
        try:
            conn = get_db()
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM visited")
            total_indexed = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM frontier WHERE status = 0")
            queue_size = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM frontier WHERE priority < 20 AND status = 0")
            high_prio = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM frontier WHERE retry_count > 0 AND status = 0")
            retries = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM frontier WHERE status = 1")
            active_threads = c.fetchone()[0]
            
            c.execute("""
                SELECT title, url, crawled_at, domain_rank 
                FROM visited 
                ORDER BY rowid DESC LIMIT 15
            """)
            recent = c.fetchall()
            
            conn.close()

            now = time.time()
            history.append((now, total_indexed))
            
            while history and history[0][0] < now - SPEED_WINDOW_SECONDS:
                history.popleft()
            
            if len(history) > 1:
                start_time, start_count = history[0]
                time_diff = now - start_time
                count_diff = total_indexed - start_count
                
                if time_diff > 0:
                    current_ppm = (count_diff / time_diff) * 60
                else:
                    current_ppm = 0
            else:
                current_ppm = 0

            db_size = get_file_size_mb(config.DB_PATH)
            wal_size = get_file_size_mb(config.DB_PATH + "-wal")

            os.system('cls' if os.name == 'nt' else 'clear')
            
            print(f" INDEXED: {total_indexed:<10,}  PENDING: {queue_size:<10,}  ACTIVE: {active_threads:<4}  SPEED: {int(current_ppm):<4} PPM")
            print(f" PRIORITY: {high_prio:<9,}  RETRY: {retries:<10,}    DB: {db_size:.1f}MB   WAL: {wal_size:.1f}MB")
            print("-" * 110)
            
            print(f" {'TIME':<8} | {'RANK':<8} | {'TITLE':<40} | {'URL'}")
            print("-" * 110)

            for r in recent:
                t_str = "--:--:--"
                if r['crawled_at']:
                    try:
                        t_obj = datetime.strptime(r['crawled_at'], '%Y-%m-%d %H:%M:%S')
                        t_str = t_obj.strftime('%H:%M:%S')
                    except: pass
                
                rank = r['domain_rank']
                rank_str = f"#{rank:,}" if rank and rank < 10000000 else "-"
                
                title = r['title'] or "No Title"
                title = title.replace('\n', ' ').strip()
                if len(title) > 38:
                    title = title[:35] + "..."
                
                url = r['url']
                if len(url) > 45:
                    url = url[:42] + "..."
                print(f" {t_str:<8} | {rank_str:<8} | {title:<40} | {url}")
            print("-" * 110)
            
            time.sleep(2)
        except KeyboardInterrupt:
            print("\nMonitor closed.")
            sys.exit()
        except Exception as e:
            print(f"Monitor Error: {e}") 
            time.sleep(1)

if __name__ == "__main__":
    monitor()