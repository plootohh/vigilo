from flask import render_template, request, jsonify
from markupsafe import Markup
from app import app
from urllib.parse import urlparse
import sqlite3, time, config, re, os
from datetime import datetime, timedelta
from typing import List, Any


def get_db_connection():
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/monitor")
def dashboard():
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. Basic Counts
    c.execute("SELECT COUNT(*) FROM visited")
    res_visited = c.fetchone()
    total_visited = res_visited[0] if res_visited else 0
    
    c.execute("SELECT COUNT(*) FROM frontier")
    res_frontier = c.fetchone()
    total_frontier = res_frontier[0] if res_frontier else 0
    
    c.execute("SELECT crawled_at FROM visited ORDER BY crawled_at DESC LIMIT 1")
    last_crawl = c.fetchone()
    status = "IDLE"
    if last_crawl:
        try:
            last_time = datetime.strptime(last_crawl[0], '%Y-%m-%d %H:%M:%S')
            if datetime.now() - last_time < timedelta(seconds=60):
                status = "ACTIVE"
        except: pass
            
    five_mins_ago = (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    c.execute("SELECT COUNT(*) FROM visited WHERE crawled_at > ?", (five_mins_ago,))
    res_ppm = c.fetchone()
    pages_last_5m = res_ppm[0] if res_ppm else 0
    ppm = round(pages_last_5m / 5, 1)
    
    c.execute("SELECT COUNT(*) FROM frontier WHERE priority <= 20")
    high_prio = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM frontier WHERE priority > 20")
    low_prio = c.fetchone()[0]
    
    c.execute("SELECT title, url, language, crawled_at FROM visited ORDER BY crawled_at DESC LIMIT 15")
    recent_raw = c.fetchall()
    recent_crawls = []
    for row in recent_raw:
        item = dict(row)
        if not item['title']: item['title'] = "No Title Data"
        recent_crawls.append(item)
        
    conn.close()
    
    return render_template(
        "monitor.html",
        total_visited="{:,}".format(total_visited),
        total_frontier="{:,}".format(total_frontier),
        recent_crawls=recent_crawls,
        high_prio="{:,}".format(high_prio),
        low_prio="{:,}".format(low_prio),
        ppm=ppm,
        status=status,
        db_name=os.path.basename(config.DB_PATH)
    )


@app.route('/search')
def search():
    raw_query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    if not raw_query: return render_template('index.html')
    
    start_time = time.time()
    conn = get_db_connection()
    c = conn.cursor()
    
    search_terms = []
    site_filter = None
    clean_query_terms = [] 
    
    tokens = raw_query.split()
    for token in tokens:
        if token.lower().startswith("site:"):
            domain_part = token[5:]
            if domain_part:
                site_filter = domain_part
        else:
            clean_word = re.sub(r'[^a-zA-Z0-9]', '', token)
            if clean_word:
                search_terms.append(f'"{clean_word}"*')
                clean_query_terms.append(clean_word)
    
    fts_query = " AND ".join(search_terms)
    
    highlight_regex_pattern = "|".join([re.escape(t) for t in clean_query_terms])
    
    if not fts_query: 
        conn.close()
        return render_template('index.html')
        
    buffer_limit = per_page * 3 
    offset = (page - 1) * per_page
    
    try:
        count_sql = "SELECT COUNT(*) FROM search_index WHERE search_index MATCH ?"
        count_params: List[Any] = [fts_query]
        
        if site_filter:
            count_sql += " AND url LIKE ?"
            count_params.append(f"%{site_filter}%")
        
        count_sql += " AND title NOT IN ('No Title', 'None')"
            
        c.execute(count_sql, tuple(count_params))
        res_count = c.fetchone()
        total_results = res_count[0] if res_count else 0
        
        sql_base = """
            SELECT 
                search_index.url, 
                search_index.title, 
                search_index.description, 
                snippet(search_index, 3, 'START_BOLD', 'END_BOLD', '...', 64) as content_snippet,
                visited.language
            FROM search_index 
            JOIN visited ON search_index.url = visited.url
            WHERE search_index MATCH ? 
        """
        params: List[Any] = [fts_query]
        
        if site_filter:
            sql_base += " AND search_index.url LIKE ? "
            params.append(f"%{site_filter}%")
            
        sql_base += " AND search_index.title NOT IN ('No Title', 'None')"
        
        nav_boost_sql = ""
        if len(clean_query_terms) == 1:
            term = clean_query_terms[0].lower()
            
            nav_boost_sql = """
                CASE 
                    -- TIER 0: Major TLDs
                    WHEN visited.url LIKE '%://' || ? || '.com%' THEN 0
                    WHEN visited.url LIKE '%://www.' || ? || '.com%' THEN 0
                    WHEN visited.url LIKE '%://' || ? || '.net%' THEN 0
                    WHEN visited.url LIKE '%://www.' || ? || '.net%' THEN 0
                    WHEN visited.url LIKE '%://' || ? || '.org%' THEN 0
                    WHEN visited.url LIKE '%://www.' || ? || '.org%' THEN 0
                    WHEN visited.url LIKE '%://' || ? || '.gov%' THEN 0
                    WHEN visited.url LIKE '%://' || ? || '.edu%' THEN 0
                    
                    -- TIER 1: Other TLDs (Catch-all for keyword domain match)
                    WHEN visited.url LIKE '%://' || ? || '.%' THEN 1
                    WHEN visited.url LIKE '%://www.' || ? || '.%' THEN 1
                    
                    ELSE 2
                END ASC,
            """
            for _ in range(8): params.append(term)
            for _ in range(2): params.append(term)
        
        sql_base += f" ORDER BY {nav_boost_sql} LENGTH(visited.url) ASC, bm25(search_index, 30.0, 5.0, 2.0, 1.0) ASC LIMIT ? OFFSET ?"
        
        params.extend([buffer_limit, offset])
        
        c.execute(sql_base, tuple(params))
        rows = c.fetchall()
        
    except Exception as e:
        print(f"Search Error: {e}")
        rows, total_results = [], 0
    
    conn.close()
    
    results = []
    seen_normalized_urls = set()
    
    for row in rows:
        if len(results) >= per_page:
            break
            
        raw_url = row['url']
        norm_url = re.sub(r'^https?://(www\.)?', '', raw_url).rstrip('/')
        
        if norm_url in seen_normalized_urls:
            continue
        
        seen_normalized_urls.add(norm_url)
        safe_content = row['content_snippet'].replace('<', '&lt;').replace('>', '&gt;')
        highlighted_content = safe_content.replace('START_BOLD', '<b>').replace('END_BOLD', '</b>')
        
        final_snippet = ""
        db_desc = row['description']
        if db_desc and len(str(db_desc)) > 15:
            safe_desc = str(db_desc).replace('<', '&lt;').replace('>', '&gt;')
            if highlight_regex_pattern:
                regex = re.compile(f"({highlight_regex_pattern})", re.IGNORECASE)
                safe_desc = regex.sub(r'<b>\1</b>', safe_desc)
            final_snippet = safe_desc
        else:
            final_snippet = highlighted_content
            if not final_snippet or final_snippet.strip() == "...":
                final_snippet = "No preview available."
        title_text = row['title']
        if highlight_regex_pattern:
            regex = re.compile(f"({highlight_regex_pattern})", re.IGNORECASE)
            title_text = regex.sub(r'<b>\1</b>', title_text)
        
        results.append({
            'title': Markup(title_text), 
            'url': row['url'], 
            'domain': urlparse(row['url']).netloc, 
            'snippet': Markup(final_snippet),
            'lang': row['language']
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
    if not query or len(query) < 2: return jsonify([])
    
    conn = get_db_connection()
    c = conn.cursor()
    
    c.execute("SELECT title FROM visited WHERE title LIKE ? LIMIT 5", (f"{query}%",))
    
    suggestions = []
    seen = set()
    for row in c.fetchall():
        t = row['title']
        if t and t not in seen and t != "No Title":
            suggestions.append(t)
            seen.add(t)
            
    conn.close()
    return jsonify(suggestions)