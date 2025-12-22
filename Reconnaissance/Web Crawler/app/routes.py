from flask import render_template, request, jsonify
from markupsafe import Markup
from app import app
from urllib.parse import urlparse
import sqlite3, time, config, re, os
from datetime import datetime, timedelta


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
    
    c.execute("SELECT COUNT(*) FROM visited")
    total_visited = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM frontier")
    total_frontier = c.fetchone()[0]
    
    c.execute("SELECT crawled_at FROM visited ORDER BY crawled_at DESC LIMIT 1")
    last_crawl = c.fetchone()
    status = "IDLE"
    if last_crawl:
        last_time = datetime.strptime(last_crawl[0], '%Y-%m-%d %H:%M:%S')
        if datetime.now() - last_time < timedelta(seconds=60):
            status = "ACTIVE"
    five_mins_ago = (datetime.now() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
    c.execute("SELECT COUNT(*) FROM visited WHERE crawled_at > ?", (five_mins_ago,))
    pages_last_5m = c.fetchone()[0]
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
    for word in raw_query.split():
        if word.startswith("site:"): site_filter = word[5:]
        else:
            clean_word = re.sub(r'[^a-zA-Z0-9]', '', word)
            if clean_word: 
                search_terms.append(f'"{clean_word}"')
                clean_query_terms.append(clean_word)
    
    fts_query = " AND ".join(search_terms)
    highlight_regex_pattern = "|".join([re.escape(t) for t in clean_query_terms])
    if not fts_query: return render_template('index.html')
    offset = (page - 1) * per_page
    try:
        count_query = "SELECT COUNT(*) FROM search_index WHERE search_index MATCH ? "
        count_params = [fts_query]
        if site_filter:
            count_query += "AND url LIKE ? "
            count_params.append(f"%{site_filter}%")
        c.execute(count_query + " AND title NOT IN ('No Title', 'None')", tuple(count_params))
        total_results = c.fetchone()[0]
        sql_base = """
            SELECT url, title, content, 
            snippet(search_index, 2, 'START_BOLD', 'END_BOLD', '...', 64) as highlighter
            FROM search_index 
            WHERE search_index MATCH ? 
        """
        params: list = [fts_query]
        if site_filter:
            sql_base += "AND url LIKE ? "
            params.append(f"%{site_filter}%")
            
        sql_base += "AND title NOT IN ('No Title', 'None') ORDER BY bm25(search_index) LIMIT ? OFFSET ?"
        params.extend([per_page, offset])
        c.execute(sql_base, tuple(params))
        rows = c.fetchall()
    except: rows, total_results = [], 0
    conn.close()
    
    results = []
    for row in rows:
        safe_text = row['highlighter'].replace('<', '&lt;').replace('>', '&gt;')
        final_snippet = safe_text.replace('START_BOLD', '<b>').replace('END_BOLD', '</b>')
        if not final_snippet or final_snippet.strip() == "...":
            final_snippet = row['content'][:160] + "..." if row['content'] else ""

        title_text = row['title']
        if highlight_regex_pattern:
            regex = re.compile(f"({highlight_regex_pattern})", re.IGNORECASE)
            title_text = regex.sub(r'<b>\1</b>', title_text)
        
        results.append({'title': Markup(title_text), 'url': row['url'], 'domain': urlparse(row['url']).netloc, 'snippet': Markup(final_snippet)})
    
    return render_template('index.html', query=raw_query, results=results, count="{:,}".format(total_results), time=round(time.time()-start_time, 4), page=page, total_pages=(total_results + per_page - 1) // per_page)


@app.route('/suggest')
def suggest():
    query = request.args.get('q', '')
    if not query or len(query) < 2: return jsonify([])
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT title FROM visited WHERE title LIKE ? LIMIT 5", (f"{query}%",))
    suggestions = [row['title'] for row in c.fetchall() if row['title']]
    conn.close()
    return jsonify(suggestions)