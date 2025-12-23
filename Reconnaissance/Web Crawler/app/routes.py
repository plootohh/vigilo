from flask import render_template, request, jsonify
from markupsafe import Markup
from app import app
from urllib.parse import urlparse
import sqlite3, time, config, re, os, math, tldextract
from datetime import datetime, timedelta


extract = tldextract.TLDExtract(cache_dir=None)


STOPWORDS = {"the", "a", "an", "of", "to", "and", "in", "on", "for", "with", "at", "by", "from"}


def get_db_connection():
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def simple_stem(word):
    if word.endswith("ing"): return word[:-3]
    if word.endswith("ed"): return word[:-2]
    if word.endswith("s") and len(word) > 3: return word[:-1]
    return word


def process_query(raw_query):
    raw_query = raw_query.lower()
    clean_raw = re.sub(r'[^a-z0-9\s]', '', raw_query)
    tokens = clean_raw.split()
    
    clean_terms = []
    processed_parts = []
    
    for t in tokens:
        if t in STOPWORDS and len(tokens) > 1:
            continue
            
        clean_terms.append(t)
        stem = simple_stem(t)
        
        if stem != t:
            processed_parts.append(f'("{t}" OR "{stem}"*)')
        else:
            processed_parts.append(f'"{t}"*')
            
    if len(clean_terms) > 1 and len(clean_terms) <= 4:
        phrase = " ".join(clean_terms)
        processed_parts.append(f'"{phrase}"')

    fts_query = " AND ".join(processed_parts)
    return fts_query, clean_terms


def calculate_smart_score(row, query_terms, nav_slug):
    score = 0.0
    
    url = row['url']
    try: domain_only = extract(url).domain.lower()
    except: domain_only = ""
        
    title = (row['title'] or "").lower()
    
    raw_rank = row['domain_rank'] if row['domain_rank'] else 10000000
    rank = min(raw_rank, 5000000)
    
    fts_score = row['text_score'] if row['text_score'] is not None else 0.0
    score += (fts_score * -3.5) 

    try:
        auth_points = (7.5 - math.log10(max(1, rank))) * 6.0
        score += max(0, auth_points)
    except: pass

    if len(query_terms) > 1:
        full_phrase = " ".join(query_terms)
        if full_phrase in title:
            score += 40.0
    
    if nav_slug and len(query_terms) <= 2:
        if nav_slug == domain_only:
            score += 500.0
            path = urlparse(url).path
            if path in ['', '/']:
                score += 150.0

    term_matches = sum(1 for t in query_terms if t in title)
    score += (term_matches * 8.0)

    slashes = url.count('/')
    if slashes <= 3: score += 10.0
    elif slashes > 5: score -= 5.0

    return score


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/monitor")
def dashboard():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM visited")
    total_visited = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM frontier")
    total_frontier = c.fetchone()[0]
    c.execute("SELECT crawled_at FROM visited ORDER BY crawled_at DESC LIMIT 1")
    last = c.fetchone()
    status = "IDLE"
    if last:
        try:
            if datetime.now() - datetime.strptime(last[0], "%Y-%m-%d %H:%M:%S") < timedelta(seconds=60):
                status = "ACTIVE"
        except: pass
    five_min = (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    c.execute("SELECT COUNT(*) FROM visited WHERE crawled_at > ?", (five_min,))
    ppm = round(c.fetchone()[0] / 5, 1)
    c.execute("SELECT COUNT(*) FROM frontier WHERE priority <= 20")
    high = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM frontier WHERE priority > 20")
    low = c.fetchone()[0]
    c.execute("SELECT title, url, language, crawled_at FROM visited ORDER BY crawled_at DESC LIMIT 15")
    recent = [dict(r) for r in c.fetchall()]
    conn.close()
    return render_template("monitor.html", total_visited="{:,}".format(total_visited), total_frontier="{:,}".format(total_frontier), recent_crawls=recent, high_prio="{:,}".format(high), low_prio="{:,}".format(low), ppm=ppm, status=status, db_name=os.path.basename(config.DB_PATH))


@app.route('/search')
def search():
    raw_query = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    if not raw_query: return render_template('index.html')
    
    start_time = time.time()
    
    fts_query, clean_terms = process_query(raw_query)
    nav_slug = "".join(clean_terms) if len(clean_terms) <= 2 else None
    
    if not fts_query: return render_template('index.html')

    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        sql = """
            SELECT 
                search_index.url, 
                search_index.title, 
                search_index.description, 
                visited.language, 
                IFNULL(visited.domain_rank, 10000000) as domain_rank,
                bm25(search_index) as text_score,
                snippet(search_index, 3, 'START_BOLD', 'END_BOLD', '...', 64) as content_snippet
            FROM search_index 
            JOIN visited ON search_index.url = visited.url
            WHERE search_index MATCH ? 
            LIMIT 1000
        """
        
        c.execute(sql, (fts_query,))
        rows = c.fetchall()
        
        scored_results = []
        seen_urls = set()
        
        for row in rows:
            raw_url = row['url']
            norm_url = re.sub(r'^https?://(www\.)?', '', raw_url).rstrip('/')
            if norm_url in seen_urls: continue
            seen_urls.add(norm_url)
            
            score = calculate_smart_score(row, clean_terms, nav_slug)
            
            scored_results.append({'row': row, 'score': score})
            
        scored_results.sort(key=lambda x: x['score'], reverse=True)
        
        total_results = len(scored_results)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        page_items = scored_results[start_idx:end_idx]
        
    except Exception as e:
        print(f"Search Error: {e}")
        page_items, total_results = [], 0
    
    conn.close()

    results = []
    
    highlight_pattern = "|".join([re.escape(t) for t in clean_terms])
    highlighter = None
    if highlight_pattern:
        highlighter = re.compile(f"({highlight_pattern})", re.IGNORECASE)

    for item in page_items:
        row = item['row']
        title_text = row['title'] or "No Title"
        
        snippet = row['content_snippet']
        desc = row['description']
        if desc and len(str(desc)) > 15:
            desc_lower = str(desc).lower()
            if any(t in desc_lower for t in clean_terms):
                snippet = desc
        
        if highlighter:
            snippet = highlighter.sub(r"<b>\1</b>", str(snippet))
            title_text = highlighter.sub(r"<b>\1</b>", str(title_text))
            
        rank = row['domain_rank']
        is_verified = True if rank and rank <= 2000 else False
        
        results.append({
            'title': Markup(title_text),
            'url': row['url'],
            'domain': urlparse(row['url']).netloc,
            'snippet': Markup(snippet),
            'lang': row['language'],
            'verified': is_verified,
            'rank': rank
        })
        
    return render_template(
        'index.html', 
        query=raw_query, 
        results=results, 
        count="{:,}".format(total_results), 
        time=round(time.time()-start_time, 4), 
        page=page, 
        total_pages=(total_results + per_page - 1) // per_page
    )


@app.route('/suggest')
def suggest():
    query = request.args.get('q', '')
    if len(query) < 2: return jsonify([])
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT title FROM visited WHERE title LIKE ? ORDER BY LENGTH(title) ASC LIMIT 5", (f"%{query}%",))
    
    suggestions = []
    seen = set()
    for row in c.fetchall():
        t = row['title']
        if t and t not in seen and t != "No Title" and len(t) < 60:
            suggestions.append(t)
            seen.add(t)
            
    conn.close()
    return jsonify(suggestions)