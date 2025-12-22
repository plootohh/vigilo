import sqlite3
from urllib.parse import urlparse, urldefrag, parse_qsl

def init_db(db_path):
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    c = conn.cursor()
    
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    c.execute("PRAGMA cache_size=-10000;")
    
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
        content TEXT, 
        language TEXT, 
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    c.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(url, title, content, tokenize = 'porter')""")
    
    c.execute("CREATE INDEX IF NOT EXISTS idx_frontier_added ON frontier(added_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_frontier_priority ON frontier(priority)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_frontier_domain ON frontier(domain)")
    
    conn.commit()
    return conn

def canonicalise(url):
    try:
        url = str(url)
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
        
        blocked_params = {"sessionid", "sid", "utm_source", "utm_medium", "utm_campaign", "fbclid", "gclid"}
        query_args = parse_qsl(parsed.query)
        valid_args = []
        for k, v in query_args:
            if k.lower() not in blocked_params:
                valid_args.append(f"{k}={v}")
        query = "&".join(sorted(valid_args))
        
        clean_url = f"{parsed.scheme}://{netloc}{path}"
        if query:
            clean_url += f"?{query}"
        
        if clean_url.endswith("/"):
            clean_url = clean_url[:-1]
        return clean_url
    except Exception:
        return None