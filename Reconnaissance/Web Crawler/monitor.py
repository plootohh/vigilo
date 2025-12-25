import sqlite3
import time
import os
import sys
from collections import deque

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import config


# --- CONFIGURATION ---
REFRESH_RATE = 2
AVG_WINDOW_SIZE = 10


def get_db():
    try:
        path = config.DB_PATH.replace("\\", "/") 
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)
    except:
        conn = sqlite3.connect(config.DB_PATH, timeout=1)
    
    conn.row_factory = sqlite3.Row
    return conn


def get_file_size_mb(path):
    try:
        return os.path.getsize(path) / (1024 * 1024)
    except OSError:
        return 0.0


def monitor():
    print("Initialising Monitor...")
    
    speed_history = deque(maxlen=AVG_WINDOW_SIZE)
    last_count = 0
    last_time = time.time()
    
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM visited")
        last_count = c.fetchone()[0]
        conn.close()
    except: pass

    while True:
        try:
            conn = get_db()
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM visited")
            current_total = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM frontier WHERE status = 1")
            active_threads = c.fetchone()[0]
            
            c.execute("""
                SELECT COUNT(*) FROM frontier 
                WHERE status = 0 
                OR (status = 2 AND next_crawl_time < datetime('now'))
            """)
            queue_size = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM frontier WHERE retry_count > 0 AND status = 0")
            retries = c.fetchone()[0]
            
            conn.close()

            now = time.time()
            time_delta = now - last_time
            count_delta = current_total - last_count
            
            if time_delta > 0:
                instant_ppm = (count_delta / time_delta) * 60
                speed_history.append(instant_ppm)
            
            last_count = current_total
            last_time = now

            if speed_history:
                avg_ppm = sum(speed_history) / len(speed_history)
            else:
                avg_ppm = 0
                
            daily_volume = avg_ppm * 60 * 24
            db_size = get_file_size_mb(config.DB_PATH)
            wal_size = get_file_size_mb(config.DB_PATH + "-wal")

            os.system('cls' if os.name == 'nt' else 'clear')
            
            print(f"================== VIGILO MONITOR =====================")
            print(f"")
            print(f"  PERFORMANCE")
            print(f"  -----------")
            print(f"  Speed:          {int(avg_ppm)} PPM")
            print(f"  Daily Vol:      {int(daily_volume):,} pages/day")
            print(f"  Active Threads: {active_threads}")
            print(f"")
            print(f"  STORAGE")
            print(f"  -------")
            print(f"  Database:       {db_size:.1f} MB")
            print(f"  WAL (Buffer):   {wal_size:.1f} MB")
            print(f"")
            print(f"  FRONTIER STATUS")
            print(f"  ---------------")
            print(f"  Total Indexed:  {current_total:,}")
            print(f"  Queue Size:     {queue_size:,}")
            print(f"  Retries:        {retries:,}")
            print(f"")
            print(f"=======================================================")
            print(f" Press Ctrl+C to exit monitor")

            time.sleep(REFRESH_RATE)

        except sqlite3.OperationalError:
            time.sleep(1)
        except KeyboardInterrupt:
            print("\nMonitor closed.")
            sys.exit()
        except Exception as e:
            print(f"Monitor Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    monitor()