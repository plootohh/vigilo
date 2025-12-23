import sqlite3, time, logging, requests, sys, os, threading, queue
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib import robotparser
from requests.adapters import HTTPAdapter, Retry
from langdetect import detect, LangDetectException

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from crawler.utils import canonicalise

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
DOMAIN_ACCESS = {}

LINK_BUFFER = queue.Queue()
VISITED_BUFFER = queue.Queue()

MAX_BUFFER_SIZE = 500
VISITED_BATCH_SIZE = 50

thread_local = threading.local()

SESSION = requests.Session()
retries = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)


def get_db():
    if not hasattr(thread_local, "conn"):
        thread_local.conn = sqlite3.connect(config.DB_PATH, timeout=60)
    return thread_local.conn


def calculate_priority(url):
    score = 10
    parsed = urlparse(url)
    url_lower = url.lower()
    
    score += url.count('/') * 2
    if parsed.query: score += 20
    
    trap_keywords = ['search', 'filter', 'login', 'signup', 'calendar', 'archive', 'tag']
    if any(k in url_lower for k in trap_keywords): score += 50
    
    if len(parsed.path) <= 1 and not parsed.query: score = 1
    return score


def get_domain_rank(domain):
    try:
        if domain.startswith("www."): search_domain = domain[4:]
        else: search_domain = domain
            
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT rank FROM domain_authority WHERE domain = ? LIMIT 1", (search_domain,))
        row = c.fetchone()
        if row: return row[0]
        return 10000000
    except:
        return 10000000


def crawl_url(current_url, retry_count):
    try:
        domain = urlparse(current_url).netloc
        
        delay = get_crawl_delay(current_url)
        last = DOMAIN_ACCESS.get(domain, 0)
        now = time.time()
        if now - last < delay:
            time.sleep(delay - (now - last))
        DOMAIN_ACCESS[domain] = time.time()
        
        logging.info(f"Crawling: {current_url}")
        
        html = download_page(current_url)
        
        if html == "NETWORK_ERROR":
            if retry_count < 2:
                requeue_url(current_url, retry_count + 1)
            return

        if html:
            data = extract_page_data(html, current_url)
            links = extract_links(html, current_url) 
            
            lang = "unknown"
            try:
                if len(data['content']) > 50:
                    lang = detect(data['content'][:500])
            except LangDetectException: pass
            
            rank = get_domain_rank(domain)
            
            VISITED_BUFFER.put({
                'url': current_url,
                'title': data['title'],
                'description': data['description'],
                'keywords': data['keywords'],
                'content': data['content'],
                'h1': data['h1'],
                'h2': data['h2'],
                'important_text': data['important_text'],
                'lang': lang,
                'out_links': len(links),
                'domain_rank': rank
            })
            
            if VISITED_BUFFER.qsize() >= VISITED_BATCH_SIZE:
                flush_visited_buffer()
            
            add_to_frontier_batch(links)
        else:
            VISITED_BUFFER.put({
                'url': current_url, 'title': None, 'description': None, 
                'keywords': None, 'content': None, 'lang': None, 'out_links': 0,
                'domain_rank': 10000000
            })
            
    except Exception as e:
        logging.error(f"Thread Error on {current_url}: {e}")


def extract_page_data(html, url):
    soup = BeautifulSoup(html, "lxml")
    
    # Clean junk
    for x in soup(["script", "style", "nav", "footer", "header", "noscript", "iframe", "svg"]): 
        x.extract()
        
    title = soup.title.string.strip() if soup.title and soup.title.string else "No Title"
    
    desc_tag = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', attrs={'property': 'og:description'})
    description = ""
    if desc_tag:
        raw_desc = desc_tag.get('content', '')
        if isinstance(raw_desc, list): description = " ".join(raw_desc).strip()
        else: description = str(raw_desc).strip()
    
    key_tag = soup.find('meta', attrs={'name': 'keywords'})
    keywords = ""
    if key_tag:
        raw_keys = key_tag.get('content', '')
        if isinstance(raw_keys, list): keywords = " ".join(raw_keys).strip()
        else: keywords = str(raw_keys).strip()
    
    h1_text = " ".join([h.get_text(" ", strip=True) for h in soup.find_all("h1")])
    
    h2_text = " ".join([h.get_text(" ", strip=True) for h in soup.find_all(["h2", "h3"])])
    
    important_text = " ".join([t.get_text(" ", strip=True) for t in soup.find_all(["strong", "em", "b"])])
    
    text = soup.get_text(separator=" ", strip=True)
    
    return {
        "title": title,
        "description": description,
        "keywords": keywords,
        "content": text,
        "h1": h1_text,
        "h2": h2_text,
        "important_text": important_text
    }


def extract_links(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    links = set()
    for link in soup.find_all("a", href=True):
        href = str(link["href"]).strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "#")): continue
        clean = canonicalise(urljoin(base_url, href)) 
        if clean: links.add(clean)
    return list(links)


def get_next_url():
    if not os.path.exists(config.DB_PATH): return None, 0
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        
        c.execute("""
            SELECT url, retry_count FROM (
                SELECT url, retry_count FROM frontier 
                WHERE priority < 100
                ORDER BY priority ASC, added_at ASC 
                LIMIT 3000
            ) ORDER BY RANDOM() LIMIT 1
        """)
        
        res = c.fetchone()
        if res:
            url, count = res
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
    logging.info(" [SYSTEM] Flushing all buffers...")
    flush_link_buffer()
    flush_visited_buffer()


def flush_link_buffer():
    if LINK_BUFFER.empty(): return
    
    links_to_process = []
    while not LINK_BUFFER.empty() and len(links_to_process) < MAX_BUFFER_SIZE * 2:
        links_to_process.append(LINK_BUFFER.get())
        
    clean_data = []
    for u in links_to_process:
        dom = urlparse(u).netloc
        prio = calculate_priority(u)
        clean_data.append((u, dom, prio))
        
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        c.executemany("""
            INSERT OR IGNORE INTO frontier (url, domain, priority) 
            VALUES (?, ?, ?)
        """, clean_data)
        conn.commit()
    except Exception as e:
        logging.error(f"Link Flush Error: {e}")
        try: conn.rollback()
        except: pass


def flush_visited_buffer():
    if VISITED_BUFFER.empty(): return
    
    batch_visited = []
    batch_index = []
    
    while not VISITED_BUFFER.empty():
        item = VISITED_BUFFER.get()
        rank = item.get('domain_rank', 10000000)

        batch_visited.append((
            item['url'], item['title'], item['description'], 
            item['keywords'], item['content'], item['lang'], 
            item.get('out_links', 0), rank
        ))
        
        if item.get('title'):
            batch_index.append((
                item['url'], 
                item['title'], 
                item['description'], 
                item['content'],
                item.get('h1', ''), 
                item.get('h2', ''), 
                item.get('important_text', '')
            ))

    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("BEGIN IMMEDIATE")
        
        c.executemany("""
            INSERT OR REPLACE INTO visited 
            (url, title, description, keywords, content, language, out_links, domain_rank) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_visited)
        
        urls = [(x[0],) for x in batch_visited]
        c.executemany("DELETE FROM search_index WHERE url=?", urls)
        
        c.executemany("""
            INSERT INTO search_index (url, title, description, content, h1, h2, important_text) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, batch_index)
        
        conn.commit()
        logging.info(f" [DB] Saved batch of {len(batch_visited)} pages.")
    except Exception as e:
        logging.error(f"Visited Flush Error: {e}")
        try: conn.rollback()
        except: pass


def requeue_url(url, retry_count):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO frontier (url, retry_count, priority) VALUES (?, ?, 50)", (url, retry_count))
        conn.commit()
    except: pass


def check_permission(target_url, agent=config.USER_AGENT):
    parsed = urlparse(target_url)
    domain = parsed.netloc
    cache_key = f"{parsed.scheme}://{domain}"
    
    if cache_key in ROBOTS_CACHE:
        return ROBOTS_CACHE[cache_key].can_fetch(agent, target_url)
    
    rp = robotparser.RobotFileParser()
    rp.set_url(f"{parsed.scheme}://{domain}/robots.txt")
    try:
        rp.read() 
        ROBOTS_CACHE[cache_key] = rp
        return rp.can_fetch(agent, target_url)
    except:
        return True


def get_crawl_delay(url):
    return config.CRAWL_DELAY


def download_page(target_url):
    try:
        if not check_permission(target_url): return None
        
        response = SESSION.get(
            target_url, 
            headers={'User-Agent': config.USER_AGENT}, 
            timeout=10,
            stream=True
        )
        
        if response.status_code != 200: return None
        if "text/html" not in response.headers.get("Content-Type", "").lower(): return None
        
        content = b""
        for chunk in response.iter_content(4096):
            content += chunk
            if len(content) > config.MAX_BYTES: 
                break
            
        return content.decode(response.encoding or 'utf-8', errors='replace')
    except Exception:
        return "NETWORK_ERROR"