import concurrent.futures
import time
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from crawler.utils import get_high_perf_connection
from crawler.bot import crawl_url, get_next_url, add_to_frontier_batch, start_writer, release_url, recover_on_startup


MAX_WORKERS = 100
THREAD_TIMEOUT = 20


SEED_LIST = [
    "https://www.abc.net.au",
    "https://www.bbc.com",
    "https://www.bloomberg.com",
    "https://www.cnn.com",
    "https://www.aljazeera.com",
    "https://www.reuters.com",
    "https://www.npr.org",
    "https://github.com",
    "https://www.stackoverflow.com/",
    "https://slashdot.org",
    "https://news.ycombinator.com",
    "https://dev.to",
    "https://www.w3schools.com",
    "https://developer.mozilla.org",
    "https://www.wikipedia.org",
    "https://en.wikipedia.org/wiki/Main_Page",
    "https://curlie.org",
    "https://www.britannica.com",
    "https://archive.org",
    "https://www.mit.edu",
    "https://www.stanford.edu",
    "https://www.harvard.edu",
    "https://www.youtube.com",
    "https://www.reddit.com",
    "https://medium.com",
    "https://wordpress.com/discover",
    "https://www.amazon.com",
    "https://www.ebay.com",
    "https://www.craigslist.org",
    "https://www.popurls.com",
    "https://alltop.com",
    "https://drudgereport.com"
]


def start_engine():
    print(f"--- Launching Vigilo Crawler ({MAX_WORKERS} Threads) ---")
    
    recover_on_startup()
    
    get_high_perf_connection(config.DB_PATH)
    writer = start_writer()
    
    add_to_frontier_batch(SEED_LIST)
    time.sleep(1)
    
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
    active_crawls = {}

    try:
        while True:
            while len(active_crawls) < MAX_WORKERS:
                task = get_next_url() 
                if task and task[0]:
                    url, retry = task
                    future = executor.submit(crawl_url, url, retry)
                    active_crawls[future] = (url, time.time())
                else:
                    break
            
            now = time.time()
            to_remove = []
            
            for future, (url, start_time) in active_crawls.items():
                if now - start_time > THREAD_TIMEOUT:
                    if not future.done():
                        print(f" [!] Timeout ({int(now-start_time)}s): Resetting {url}")
                        release_url(url) 
                        to_remove.append(future)
            
            for f in to_remove:
                active_crawls.pop(f)
            
            if active_crawls:
                done, _ = concurrent.futures.wait(
                    active_crawls.keys(), 
                    timeout=0.1, 
                    return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    if future in active_crawls:
                        active_crawls.pop(future)
            else:
                print(" [!] Frontier exhausted or DB locked. Retrying in 5s...")
                time.sleep(5)
                
    except KeyboardInterrupt:
        print("\n [!] Interrupt received. Stopping engine...")
        
        print(" [!] Resetting active URLs for next run...")
        for _, (url, _) in active_crawls.items():
            release_url(url)

        print(" [!] Saving remaining data...")
        writer.stop()
        writer.join()
        
        executor.shutdown(wait=False)
        print(" [!] Shutdown complete.")
        sys.exit(0)

if __name__ == "__main__":
    start_engine()