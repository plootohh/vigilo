import concurrent.futures
import time
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from crawler.utils import init_db
from crawler.bot import crawl_url, get_next_url, add_to_frontier_batch, force_flush_buffer


# CONFIGURATION
MAX_WORKERS = 20
THREAD_TIMEOUT = 30 
SEED_LIST = [
    "https://curlie.org",
    "https://www.wikipedia.org",
    "https://www.bbc.com",
    "https://www.bloomberg.com",
    "https://github.com",
    "https://www.amazon.com",
    "https://www.youtube.com",
    "https://www.stackoverflow.com/",
    "https://www.abc.net.au",
    "https://slashdot.org",
    "https://wordpress.com/discover"
]


def start_engine():
    print(f"--- Launching Vigilo Engine with {MAX_WORKERS} workers ---")
    
    init_db(config.DB_PATH)
    
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
                    timeout=0.5, 
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
                print(" Frontier empty. Sleeping...")
                time.sleep(5)


if __name__ == "__main__":
    try:
        start_engine()
    except KeyboardInterrupt:
        print("\n [!] Shutdown signal received.")
        force_flush_buffer()
        print(" [!] Shutdown complete. Links saved.")