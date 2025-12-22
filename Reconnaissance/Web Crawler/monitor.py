import sqlite3
import time, os, sys
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import config

def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

def monitor():
    print("Starting crawler monitor... Press Ctrl+C to exit.")
    
    start_time = time.time()
    conn_init = sqlite3.connect(config.DB_PATH)
    start_count = conn_init.execute("SELECT COUNT(*) FROM visited").fetchone()[0]
    conn_init.close()
    
    while True:
        try:
            conn = sqlite3.connect(config.DB_PATH, timeout=10)
            c = conn.cursor()
            
            c.execute("SELECT COUNT(*) FROM visited")
            total_visited = c.fetchone()[0]
            
            c.execute("SELECT COUNT(*) FROM frontier")
            total_frontier = c.fetchone()[0]
            
            c.execute("SELECT title, url, language, crawled_at FROM visited ORDER BY crawled_at DESC LIMIT 12")
            recent_rows = c.fetchall()
            
            status = "IDLE"
            if recent_rows:
                last_ts_str = recent_rows[0][3]
                last_ts = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S')
                if datetime.now() - last_ts < timedelta(seconds=45):
                    status = "RUNNING"
            
            conn.close()
            
            pages_this_session = total_visited - start_count
            elapsed_seconds = time.time() - start_time
            ppm = (pages_this_session / elapsed_seconds) * 60 if elapsed_seconds > 0 else 0
            
            clear_screen()
            print(f"========================================")
            print(f"      VIGILO ENGINE MONITOR (CLI)      ")
            print(f"========================================")
            print(f" STATUS:       {status}")
            print(f" DATABASE:     {os.path.basename(config.DB_PATH)}")
            print(f"----------------------------------------")
            print(f" TOTAL INDEXED:   {total_visited:,}")
            print(f" QUEUE SIZE:      {total_frontier:,}")
            print(f" SESSION SPEED:   {ppm:.1f} pages/min")
            print(f"----------------------------------------")
            print(f" LATEST CRAWLS:")
            
            for row in recent_rows:
                title, url, lang, _ = row
                
                if not title:
                    display_title = "No Title Data"
                else:
                    display_title = (title[:30] + '..') if len(title) > 30 else title
                
                display_url = (url[:40] + '..') if len(url) > 40 else url
                
                print(f" [{lang or '??'}] {display_title:<35} -> {display_url}")
            
            print(f"========================================")
            print(f" Press Ctrl+C to exit monitor")
            
            time.sleep(2)
        
        except KeyboardInterrupt:
            print("\nExiting monitor...")
            break
        except Exception as e:
            print(f"Error in monitor: {e}")
            time.sleep(2)

if __name__ == "__main__":
    if not os.path.exists(config.DB_PATH):
        print(f"Error: Database not found at {config.DB_PATH}")
        print("Run the crawler first!")
    else:
        monitor()