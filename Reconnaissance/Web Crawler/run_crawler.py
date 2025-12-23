import concurrent.futures
import time
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from crawler.utils import init_db
from crawler.bot import crawl_url, get_next_url, add_to_frontier_batch, force_flush_buffer


MAX_WORKERS = 20
THREAD_TIMEOUT = 30


SEED_LIST = [
    # --- News / Journalism ---
    "https://www.abc.net.au",
    "https://www.bbc.com",
    "https://www.bloomberg.com",
    "https://www.cnn.com",
    "https://www.aljazeera.com",
    "https://www.reuters.com",
    "https://www.npr.org",

    # --- Technology / Coding / Science ---
    "https://github.com",
    "https://www.stackoverflow.com/",
    "https://slashdot.org",
    "https://news.ycombinator.com",
    "https://dev.to",
    "https://www.w3schools.com",
    "https://developer.mozilla.org",

    # --- Reference / Directories / Education ---
    "https://www.wikipedia.org",
    "https://en.wikipedia.org/wiki/Main_Page",
    "https://curlie.org",
    "https://www.britannica.com",
    "https://archive.org",
    "https://www.mit.edu",
    "https://www.stanford.edu",
    "https://www.harvard.edu",

    # --- Social / Blogs / Discussion ---
    "https://www.youtube.com",
    "https://www.reddit.com",
    "https://medium.com",
    "https://wordpress.com/discover",

    # --- Commerce / Marketplaces ---
    "https://www.amazon.com",
    "https://www.ebay.com",
    "https://www.craigslist.org",

    # --- Aggregators (for finding new random domains) ---
    "https://www.popurls.com",
    "https://alltop.com",
    "https://drudgereport.com"
]


def start_engine():
    print(f"--- Launching Vigilo Engine with {MAX_WORKERS} workers ---")
    
    init_db(config.DB_PATH)
    
    print(f" [i] Seeding frontier with {len(SEED_LIST)} starting URLs...")
    add_to_frontier_batch(SEED_LIST)
    force_flush_buffer()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        active_crawls = {}
        
        while True:
            while len(active_crawls) < MAX_WORKERS:
                url, retry_count = get_next_url()
                if url:
                    future = executor.submit(crawl_url, url, retry_count)
                    active_crawls[future] = (url, time.time())
                else:
                    break
            
            now = time.time()
            for future, (url, start_time) in list(active_crawls.items()):
                if now - start_time > THREAD_TIMEOUT:
                    if not future.done():
                        logging.warning(f" Thread timeout: {url}")
                        active_crawls.pop(future)
            
            if active_crawls:
                done, _ = concurrent.futures.wait(
                    active_crawls.keys(), 
                    timeout=0.1, 
                    return_when=concurrent.futures.FIRST_COMPLETED
                )
                
                for future in done:
                    if future in active_crawls:
                        url, _ = active_crawls.pop(future)
                        try:
                            future.result() 
                        except Exception as e:
                            logging.error(f" Worker failed: {e}")
            else:
                print(" Frontier empty or locked. Sleeping 5s...")
                time.sleep(5)


if __name__ == "__main__":
    try:
        start_engine()
    except KeyboardInterrupt:
        print("\n [!] Shutdown signal received.")
        force_flush_buffer()
        print(" [!] Shutdown complete. Links saved.")