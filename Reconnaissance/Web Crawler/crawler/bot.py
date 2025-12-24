import sqlite3, time, logging, requests, sys, os, threading, queue, random
from urllib.parse import urljoin, urlparse
from urllib import robotparser
from requests.adapters import HTTPAdapter, Retry
from langdetect import detect, LangDetectException
from selectolax.parser import HTMLParser
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from crawler.utils import canonicalise, get_high_perf_connection, BloomFilter

# --- CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

ROBOTS_CACHE = {}
DOMAIN_LAST_ACCESSED = {}
DOMAIN_LOCK = threading.Lock()

LINK_BUFFER = queue.Queue()
VISITED_BUFFER = queue.Queue()
DISPATCH_QUEUE = queue.Queue()
DISPATCH_LOCK = threading.Lock()

MAX_BUFFER_SIZE = 5000   
VISITED_BATCH_SIZE = 200  

BLOOM = BloomFilter(100_000_000, 7) 
BLOOM_LOCK = threading.Lock()

thread_local = threading.local()

SESSION = requests.Session()
retries = Retry(total=2, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)


def get_db():
    if not hasattr(thread_local, "conn"):
        thread_local.conn = get_high_perf_connection(config.DB_PATH)
    return thread_local.conn


def calculate_priority(url):
    score = 10
    try:
        parsed = urlparse(url)
        score += url.count('/') * 2
        if parsed.query: score += 20
        trap_keywords = ['search', 'filter', 'login', 'signup', 'calendar', 'archive', 'tag']
        if any(k in url.lower() for k in trap_keywords): score += 50
        if len(parsed.path) <= 1 and not parsed.query: score = 1
    except: pass
    return score


def get_domain_rank(domain):
    try:
        if domain.startswith("www."): search_domain = domain[4:]
        else: search_domain = domain
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT rank FROM domain_authority WHERE domain = ? LIMIT 1", (search_domain,))
        row = c.fetchone()
        return row[0] if row else 10_000_000
    except:
        return 10_000_000


# --- PREFETCHING ---
def get_next_url():
    try:
        return DISPATCH_QUEUE.get_nowait()
    except queue.Empty:
        pass

    with DISPATCH_LOCK:
        if not DISPATCH_QUEUE.empty():
            return DISPATCH_QUEUE.get()

        conn = get_db()
        try:
            c = conn.cursor()
            c.execute("BEGIN IMMEDIATE")
            
            c.execute("""
                SELECT url, retry_count FROM frontier 
                WHERE status = 0
                ORDER BY priority ASC, added_at ASC 
                LIMIT 5000
            """)
            rows = c.fetchall()
            
            if rows:
                batch_urls = [(r[0],) for r in rows]
                c.executemany("UPDATE frontier SET status = 1 WHERE url = ?", batch_urls)
                conn.commit()
                
                random.shuffle(rows)
                for r in rows:
                    DISPATCH_QUEUE.put((r[0], r[1]))
                
                logging.info(f" [SYSTEM] Refueled: Dispatched {len(rows)} URLs.")
                return DISPATCH_QUEUE.get()
            else:
                conn.commit()
                time.sleep(1)
                return None, 0
        
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                time.sleep(random.uniform(0.5, 1.5))
            else:
                try: conn.rollback()
                except: pass
            return None, 0
        except Exception as e:
            logging.error(f"Dispatch Error: {e}")
            return None, 0


def crawl_url(current_url, retry_count):
    try:
        domain = urlparse(current_url).netloc
        
        with DOMAIN_LOCK:
            last = DOMAIN_LAST_ACCESSED.get(domain, 0)
            now = time.time()
            if now - last < config.CRAWL_DELAY:
                time.sleep(config.CRAWL_DELAY - (now - last))
            DOMAIN_LAST_ACCESSED[domain] = time.time()
        
        logging.info(f"Crawling: {current_url}")
        
        html = download_page(current_url)
        
        if html == "NETWORK_ERROR":
            if retry_count < 2:
                requeue_url(current_url, retry_count + 1)
            else:
                mark_frontier_status(current_url, 3)
            return

        if html:
            tree = HTMLParser(html)
            
            for tag in tree.css('script, style, nav, footer, header, noscript, iframe, svg'):
                tag.decompose()

            title_node = tree.css_first('title')
            title = title_node.text(strip=True) if title_node else "No Title"

            desc_node = tree.css_first('meta[name="description"]')
            description = desc_node.attributes.get('content', '') if desc_node else ""

            h1 = " ".join([n.text(strip=True) for n in tree.css('h1')])
            h2 = " ".join([n.text(strip=True) for n in tree.css('h2, h3')])
            content = tree.body.text(separator=' ', strip=True) if tree.body else ""
            
            links = []
            if DISPATCH_QUEUE.qsize() < 50000:
                for node in tree.css('a[href]'):
                    href = node.attributes.get('href')
                    clean = canonicalise(urljoin(current_url, href))
                    if clean: links.append(clean)
            
            lang = "unknown"
            try:
                if len(content) > 100:
                    lang = detect(content[:500])
            except LangDetectException: pass
            
            rank = get_domain_rank(domain)
            
            VISITED_BUFFER.put({
                'url': current_url,
                'title': title,
                'description': description,
                'content': content,
                'h1': h1,
                'h2': h2,
                'lang': lang,
                'out_links': len(links),
                'domain_rank': rank
            })
            
            add_to_frontier_batch(links)
        else:
            mark_frontier_status(current_url, 2)
            
    except Exception as e:
        logging.error(f"Thread Error: {e}")
        mark_frontier_status(current_url, 3)


def mark_frontier_status(url, status):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE frontier SET status = ? WHERE url = ?", (status, url))
        conn.commit()
    except: pass


def requeue_url(url, retry_count):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("UPDATE frontier SET status = 0, retry_count = ? WHERE url = ?", (retry_count, url))
        conn.commit()
    except: pass


def add_to_frontier_batch(urls):
    if not urls: return
    to_add = []
    with BLOOM_LOCK:
        for u in urls:
            if not BLOOM.lookup(u):
                BLOOM.add(u)
                to_add.append(u)
    for u in to_add:
        LINK_BUFFER.put(u)


def check_permission(target_url, agent=config.USER_AGENT):
    try:
        parsed = urlparse(target_url)
        domain = parsed.netloc
        cache_key = f"{parsed.scheme}://{domain}"
        if cache_key in ROBOTS_CACHE:
            return ROBOTS_CACHE[cache_key].can_fetch(agent, target_url)
        rp = robotparser.RobotFileParser()
        rp.set_url(f"{parsed.scheme}://{domain}/robots.txt")
        rp.read()
        ROBOTS_CACHE[cache_key] = rp
        return rp.can_fetch(agent, target_url)
    except:
        return True


def download_page(target_url):
    try:
        
        response = SESSION.get(target_url, headers={'User-Agent': config.USER_AGENT}, timeout=5, stream=True)
        if response.status_code != 200: return None
        if "text/html" not in response.headers.get("Content-Type", "").lower(): return None
        
        content = response.raw.read(config.MAX_BYTES + 1, decode_content=True)
        if len(content) > config.MAX_BYTES: return None
        
        return content.decode('utf-8', errors='replace')
    except:
        return "NETWORK_ERROR"


# --- DATABASE WRITER ---
class DatabaseWriter(threading.Thread):
    def __init__(self):
        super().__init__()
        self.daemon = True 
        self.running = True

    def stop(self):
        self.running = False

    def run(self):
        logging.info(" [DB] Writer thread started. Hydrating Bloom...")
        conn = get_high_perf_connection(config.DB_PATH)
        
        try:
            c = conn.cursor()
            c.execute("SELECT url FROM visited")
            while True:
                batch = c.fetchmany(50000)
                if not batch: break
                with BLOOM_LOCK:
                    for row in batch: BLOOM.add(row[0])
            
            c.execute("SELECT url FROM frontier")
            while True:
                batch = c.fetchmany(50000)
                if not batch: break
                with BLOOM_LOCK:
                    for row in batch: BLOOM.add(row[0])
            logging.info(" [DB] Bloom Hydrated.")
        except: pass

        while self.running:
            try:
                if LINK_BUFFER.qsize() >= MAX_BUFFER_SIZE or (not LINK_BUFFER.empty() and time.time() % 5 < 0.1):
                    self.flush_links(conn)
                
                if VISITED_BUFFER.qsize() >= VISITED_BATCH_SIZE or (not VISITED_BUFFER.empty() and time.time() % 5 < 0.1):
                    self.flush_visited(conn)
                
                time.sleep(0.1) 
            except Exception as e:
                logging.error(f"Writer Error: {e}")
                time.sleep(1)

        self.flush_links(conn)
        self.flush_visited(conn)
        conn.close()


    def flush_links(self, conn):
        links = []
        while not LINK_BUFFER.empty() and len(links) < MAX_BUFFER_SIZE:
            links.append(LINK_BUFFER.get())
        if not links: return

        data = []
        for u in links:
            data.append((u, urlparse(u).netloc, calculate_priority(u), 0))

        try:
            c = conn.cursor()
            c.execute("BEGIN IMMEDIATE")
            c.executemany("INSERT OR IGNORE INTO frontier (url, domain, priority, status) VALUES (?, ?, ?, ?)", data)
            conn.commit()
        except sqlite3.OperationalError: 
            pass


    def flush_visited(self, conn):
        batch = []
        urls_crawled = []
        
        while not VISITED_BUFFER.empty() and len(batch) < VISITED_BATCH_SIZE:
            item = VISITED_BUFFER.get()
            batch.append((
                item['url'], item['title'], item['description'], "", # Keywords placeholder
                item['content'], item['lang'], item['out_links'], 
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                item['domain_rank']
            ))
            urls_crawled.append((2, item['url']))

        if not batch: return

        try:
            c = conn.cursor()
            c.execute("BEGIN IMMEDIATE")
            
            c.executemany("""
                INSERT OR REPLACE INTO visited 
                (url, title, description, keywords, content, language, out_links, crawled_at, domain_rank) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            
            c.executemany("UPDATE frontier SET status = ? WHERE url = ?", urls_crawled)
            
            conn.commit()
            logging.info(f" [DB] Saved {len(batch)} pages.")
        except sqlite3.OperationalError:
            pass


def start_writer():
    writer = DatabaseWriter()
    writer.start()
    return writer