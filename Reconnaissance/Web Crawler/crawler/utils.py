import sqlite3
from urllib.parse import urlparse, urldefrag, parse_qsl


def init_db(db_path):
    conn = sqlite3.connect(db_path, timeout=60, check_same_thread=False)
    c = conn.cursor()
    
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    c.execute("PRAGMA cache_size=-64000;")
    c.execute("PRAGMA temp_store=MEMORY;")
    
    c.execute("""CREATE TABLE IF NOT EXISTS frontier (
        url TEXT PRIMARY KEY, 
        domain TEXT,
        retry_count INTEGER DEFAULT 0, 
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        priority INTEGER DEFAULT 10
    )""")
    
    c.execute("""CREATE TABLE IF NOT EXISTS visited (
        url TEXT PRIMARY KEY, 
        title TEXT, 
        description TEXT,
        keywords TEXT,
        content TEXT, 
        language TEXT, 
        out_links INTEGER DEFAULT 0,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    c.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(url, title, description, content, tokenize='porter')""")
    
    c.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority_added ON frontier(priority, added_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier(domain)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_visited_crawled ON visited(crawled_at)")
    
    conn.commit()
    return conn


def canonicalise(url):
    try:
        url = str(url).strip()
        url = urldefrag(url)[0]
        parsed = urlparse(url)
        
        if parsed.scheme not in ("http", "https"):
            return None
        
        netloc = parsed.hostname
        if not netloc:
            return None
        netloc = netloc.lower()
        
        if parsed.port:
            if (parsed.scheme == "http" and parsed.port != 80) or (parsed.scheme == "https" and parsed.port != 443):
                netloc += f":{parsed.port}"
        
        path = parsed.path.replace("//", "/")
        if not path: path = "/"
        
        blocked_params = {
            "sessionid", "sid", "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "fbclid", "gclid", "ref", "source", "action", "token", "phpsessid"
        }
        query_args = parse_qsl(parsed.query)
        valid_args = []
        for k, v in query_args:
            if k.lower() not in blocked_params:
                valid_args.append(f"{k}={v}")
        
        query = "&".join(sorted(valid_args))
        
        clean_url = f"{parsed.scheme}://{netloc}{path}"
        if query:
            clean_url += f"?{query}"
            
        if clean_url.endswith("/") and path != "/":
            clean_url = clean_url[:-1]
            
        return clean_url
    except Exception:
        return None