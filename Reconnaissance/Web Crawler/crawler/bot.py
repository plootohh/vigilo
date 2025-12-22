import sqlite3, time, logging, requests, sys, os, threading, queue
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote
from urllib import robotparser
from requests.adapters import HTTPAdapter, Retry
from langdetect import detect, LangDetectException

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from crawler.utils import canonicalise


# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


# --- GLOBAL CACHES, BUFFERS & THREAD LOCAL STORAGE ---
ROBOTS_CACHE = {}
DOMAIN_ACCESS = {}
LINK_BUFFER = queue.Queue()
MAX_BUFFER_SIZE = 500
SESSION = requests.Session()
thread_local = threading.local()


# --- RETRY SETUP ---
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"]
)
SESSION.mount("http://", HTTPAdapter(max_retries=retries))
SESSION.mount("https://", HTTPAdapter(max_retries=retries))


def get_db():
    if not hasattr(thread_local, "conn"):
        thread_local.conn = sqlite3.connect(config.DB_PATH, timeout=60)
        thread_local.conn.execute("PRAGMA journal_mode=WAL;")
        thread_local.conn.execute("PRAGMA synchronous=OFF;")
        thread_local.conn.execute("PRAGMA temp_store=MEMORY;")
        thread_local.conn.execute("PRAGMA cache_size=-20000;")
    return thread_local.conn


def calculate_priority(url):
    score = 10
    parsed = urlparse(url)
    url_lower = url.lower()
    score += url.count('/') * 5
    if parsed.query: score += 100
    trap_keywords = ['/search', '/filter', '/login', '/signup', '/calendar', '/tags', '/results']
    if any(k in url_lower for k in trap_keywords): score += 150
    if len(parsed.path) <= 1: score -= 5
    return score


def crawl_url(current_url, retry_count):
    try:
        domain = urlparse(current_url).netloc
        delay = get_crawl_delay(current_url)
        
        last = DOMAIN_ACCESS.get(domain, 0)
        now = time.time()
        if now - last < delay:
            time.sleep(delay - (now - last))
        DOMAIN_ACCESS[domain] = time.time()
        logging.info(f"Crawling (Attempt {retry_count+1}): {current_url}")
        html = download_page(current_url)
        if html == "NETWORK_ERROR":
            if retry_count < 3:
                logging.warning(f"   -> Network error on {current_url}. Re-queueing ({retry_count+1}/3)")
                requeue_url(current_url, retry_count + 1)
            else:
                logging.error(f"   -> Max retries reached for {current_url}. Dropping.")
                save_page(current_url, None, None, "network_error")
            return
        if html:
            title, content = extract_content(html)
            links = extract_links(html, current_url) 
            
            lang = "unknown"
            try:
                if len(content) > 50:
                    lang = detect(content[:500])
            except LangDetectException: pass
            
            logging.info(f"    > Title: {title} [{lang}]")
            logging.info(f"    > Found {len(links)} links. Buffering...")
            
            save_page(current_url, title, content, lang)
            add_to_frontier_batch(links)
            logging.info("    > Saved Resource.")
        else:
            save_page(current_url, None, None, None)
            
    except Exception as e:
        logging.error(f"Thread Error on {current_url}: {e}")


# --- CORE FUNCTIONS ---

def get_next_url():
    if not os.path.exists(config.DB_PATH): return None, 0
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        c.execute("""
            SELECT url, retry_count FROM (
                SELECT url, retry_count FROM frontier 
                ORDER BY priority ASC, added_at ASC 
                LIMIT 50
            ) ORDER BY RANDOM() LIMIT 1
        """)
        res = c.fetchone()
        if res and res[0]:
            url, count = res[0], res[1]
            c.execute("DELETE FROM frontier WHERE url=?", (url,))
            conn.commit()
            return url, count
        conn.rollback()
        return None, 0
    except Exception:
        try: conn.rollback()
        except: pass
        return None, 0


def add_to_frontier_batch(urls):
    if not urls: return
    for u in urls:
        LINK_BUFFER.put(u)
    if LINK_BUFFER.qsize() >= MAX_BUFFER_SIZE:
        flush_link_buffer()


def force_flush_buffer():
    if not LINK_BUFFER.empty():
        logging.info(f" [SYSTEM] Manual buffer flush triggered. Processing {LINK_BUFFER.qsize()} items...")
        flush_link_buffer()


def flush_link_buffer():
    links_to_process = []
    while not LINK_BUFFER.empty() and len(links_to_process) < MAX_BUFFER_SIZE * 2:
        links_to_process.append(LINK_BUFFER.get())
    
    if not links_to_process: return
    clean_data = []
    for u in links_to_process:
        clean = canonicalise(u)
        if clean:
            domain = urlparse(clean).netloc
            prio = calculate_priority(clean)
            clean_data.append((clean, domain, prio))
    
    if not clean_data: return
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        for u, dom, prio in clean_data:
            c.execute("""
                INSERT OR IGNORE INTO frontier (url, domain, priority) 
                SELECT ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM visited WHERE url = ?)
            """, (u, dom, prio, u))
        conn.commit()
        logging.info(f" [BUFFER_FLUSH] Committed {len(clean_data)} links.")
    except Exception as e:
        logging.error(f"Batch Flush Error: {e}")
        try: conn.rollback()
        except: pass


def save_page(url, title, content, language):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        c.execute("""INSERT OR REPLACE INTO visited (url, title, content, language) VALUES (?, ?, ?, ?)""", (url, title, content, language))
        c.execute("DELETE FROM search_index WHERE url=?", (url,))
        if title and content:
            c.execute("INSERT INTO search_index (url, title, content) VALUES (?, ?, ?)", (url, title, content))
        conn.commit()
    except Exception as e:
        logging.error(f"DB Error save_page: {e}")
        try: conn.rollback()
        except: pass


def check_permission(target_url, agent=config.USER_AGENT):
    parsed = urlparse(target_url)
    domain = parsed.netloc
    if not domain: return False
    cache_key = f"{parsed.scheme}://{domain}"
    if cache_key in ROBOTS_CACHE:
        return ROBOTS_CACHE[cache_key].can_fetch(agent, target_url)
    
    domain_robots = f"{parsed.scheme}://{domain}/robots.txt"
    rp = robotparser.RobotFileParser()
    rp.set_url(domain_robots)
    try:
        response = SESSION.get(domain_robots, headers={"User-Agent": agent}, timeout=10)
        if response.status_code == 200:
            rp.parse(response.text.splitlines())
        elif response.status_code in [401, 403]:
            rp.parse(["User-agent: *", "Disallow: /"])
        else:
            rp.parse(["User-agent: *", "Disallow:"])
        ROBOTS_CACHE[cache_key] = rp
        return rp.can_fetch(agent, target_url)
    except:
        return "NETWORK_ERROR"


def get_crawl_delay(url):
    parsed = urlparse(url)
    cache_key = f"{parsed.scheme}://{parsed.netloc}"
    rp = ROBOTS_CACHE.get(cache_key)
    if rp and rp.crawl_delay(config.USER_AGENT):
        return min(rp.crawl_delay(config.USER_AGENT), 10) 
    return config.CRAWL_DELAY


def download_page(target_url):
    allowed = check_permission(target_url)
    if allowed == "NETWORK_ERROR": return "NETWORK_ERROR"
    if not allowed: return None
    try:
        response = SESSION.get(target_url, headers={'User-Agent': config.USER_AGENT}, timeout=10, stream=True, allow_redirects=False)
        if response.status_code in [301, 302, 307, 308]:
            redirect_url = response.headers.get("Location")
            if redirect_url:
                full_redirect_url = urljoin(target_url, redirect_url)
                add_to_frontier_batch([full_redirect_url])
            return None
        if "text/html" not in response.headers.get("Content-Type", "").lower():
            return None
        content = b""
        for chunk in response.iter_content(1024):
            content += chunk
            if len(content) > config.MAX_BYTES: return None
        return content.decode(response.encoding or 'utf-8', errors='replace')
    except:
        return "NETWORK_ERROR"


def extract_content(html):
    soup = BeautifulSoup(html, "lxml")
    for x in soup(["script", "style", "nav", "footer", "header"]): x.extract()
    title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
    text = soup.get_text(separator=" ", strip=True)
    return title, text


def extract_links(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for link in soup.find_all("a", href=True):
        href = str(link["href"]).strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "#")): continue
        clean = canonicalise(urljoin(base_url, href)) 
        if clean: links.add(clean)
    return list(links)


def requeue_url(url, retry_count):
    conn = get_db()
    try:
        domain = urlparse(url).netloc
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT OR REPLACE INTO frontier (url, domain, retry_count, priority) VALUES (?, ?, ?, 15)", (url, domain, retry_count))
        conn.commit()
    except Exception as e:
        logging.error(f"DB Error requeue: {e}")
        try: conn.rollback()
        except: pass