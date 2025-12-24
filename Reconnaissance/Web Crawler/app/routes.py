from flask import render_template, request, jsonify
from markupsafe import Markup
from app import app
from urllib.parse import urlparse
import sqlite3, time, config, re, os, math, tldextract
from datetime import datetime, timedelta


extract = tldextract.TLDExtract(cache_dir=None)


STOPWORDS = {
    "the","a","an","of","to","and","in","on","for","with","at","by","from",
    "how","what","why","when","where"
}


def get_db_connection():
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def simple_stem(word):
    if word.endswith("ing"): return word[:-3]
    if word.endswith("ed"): return word[:-2]
    if word.endswith("s") and len(word) > 3: return word[:-1]
    return word


def classify_intent(terms):
    if not terms:
        return "unknown"
    if len(terms) <= 2:
        return "navigational"
    if any(t in {"how","what","why","guide","tutorial","install"} for t in terms):
        return "informational"
    if any(re.search(r"\d", t) for t in terms):
        return "reference"
    return "informational"


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

    if 1 < len(clean_terms) <= 4:
        processed_parts.append(f'"{" ".join(clean_terms)}"')

    return " AND ".join(processed_parts), clean_terms


def proximity_score(text, terms):
    if not text:
        return 0.0

    tokens = re.findall(r"[a-z0-9]+", text.lower())
    positions = {t: [] for t in terms}

    for i, tok in enumerate(tokens):
        if tok in positions:
            positions[tok].append(i)

    valid = [v for v in positions.values() if v]
    if len(valid) < 2:
        return 0.0

    min_span = float("inf")
    for a in valid:
        for b in valid:
            if a is b:
                continue
            for i in a:
                for j in b:
                    min_span = min(min_span, abs(i - j))

    if min_span == float("inf"):
        return 0.0

    return max(0.0, 25.0 / (1.0 + min_span))


def calculate_smart_score(row, query_terms, nav_slug, intent):
    score = 0.0

    url = row["url"]
    title = (row["title"] or "").lower()
    desc = (row["description"] or "").lower()
    snippet = (row["content_snippet"] or "").lower()

    try:
        domain_only = extract(url).domain.lower()
    except:
        domain_only = ""

    fts = row["text_score"] or 0.0
    score += fts * -3.2

    rank = min(row["domain_rank"] or 10_000_000, 5_000_000)
    try:
        score += (1 / (1 + math.log10(rank))) * 60.0
    except:
        pass

    full_phrase = " ".join(query_terms)

    if full_phrase in title:
        score += 45
    elif full_phrase in desc:
        score += 25

    title_hits = sum(t in title for t in query_terms)
    desc_hits = sum(t in desc for t in query_terms)

    score += title_hits * 9
    score += desc_hits * 4

    path = urlparse(url).path.lower()
    path_hits = sum(t in path for t in query_terms)
    score += path_hits * 6

    score += proximity_score(snippet, query_terms)

    if intent == "navigational" and nav_slug == domain_only:
        score += 420
        if path in ("","/"):
            score += 120

    slashes = path.count("/")
    if slashes <= 2:
        score += 12
    elif slashes >= 6:
        score -= 10

    if "?" in url:
        score -= 8

    total_hits = title_hits + desc_hits + path_hits
    if total_hits == 0:
        score -= 20
    elif total_hits > len(query_terms) * 5:
        score -= 15

    try:
        if row["crawled_at"]:
            crawled = datetime.strptime(row["crawled_at"], "%Y-%m-%d %H:%M:%S")
            age_days = (datetime.now() - crawled).days
            if age_days > 365:
                score -= min(10, age_days / 365)
    except:
        pass

    return score

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/search")
def search():
    raw_query = request.args.get("q","").strip()
    page = request.args.get("page", 1, type=int)
    
    PER_PAGE_DISPLAY = 20
    POOL_SIZE = 200 
    
    if not raw_query:
        return render_template("index.html")

    start = time.time()

    fts_query, terms = process_query(raw_query)
    intent = classify_intent(terms)
    nav_slug = "".join(terms) if len(terms) <= 2 else None

    conn = get_db_connection()
    c = conn.cursor()

    results = []
    total_count = 0

    try:
        
        sql_fetch = """
            SELECT
                s.url,
                s.title,
                s.description,
                snippet(search_index, 3, 'START_BOLD', 'END_BOLD', '...', 64) AS content_snippet,
                bm25(search_index) AS text_score,
                v.language,
                v.crawled_at,
                IFNULL(v.domain_rank, 10000000) AS domain_rank
            FROM search_index s
            JOIN visited v ON s.url = v.url
            WHERE s MATCH ?
            ORDER BY (bm25(search_index) * -1) + (1000000.0 / (IFNULL(v.domain_rank, 1000000) + 1)) DESC
            LIMIT ?
        """
        
        
        c.execute(sql_fetch, (fts_query, POOL_SIZE))
        candidates = c.fetchall()

        count_sql = """
            SELECT count(*) 
            FROM search_index_rowid 
            WHERE rowid IN (
                SELECT rowid FROM search_index WHERE search_index MATCH ?
            )
        """
        try:
            c.execute(count_sql, (fts_query,))
            total_count = c.fetchone()[0]
        except:
            total_count = len(candidates)

        scored = []
        seen = set()

        for r in candidates:
            norm = re.sub(r'^https?://(www\.)?', '', r["url"]).rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)

            s = calculate_smart_score(r, terms, nav_slug, intent)
            scored.append((s, r))

        scored.sort(key=lambda x: x[0], reverse=True)

        
        start_idx = (page - 1) * PER_PAGE_DISPLAY
        end_idx = start_idx + PER_PAGE_DISPLAY
        
        page_rows = scored[start_idx:end_idx]

        highlight = re.compile("("+"|".join(map(re.escape, terms))+")", re.I) if terms else None

        for _, r in page_rows:
            title = r["title"] or "No Title"
            raw_snippet = r["content_snippet"] or ""
            raw_snippet = raw_snippet.replace("START_BOLD", "").replace("END_BOLD", "")
            
            if highlight:
                title = highlight.sub(r"<b>\1</b>", title)
                display_snippet = highlight.sub(r"<b>\1</b>", raw_snippet)
            else:
                display_snippet = raw_snippet

            results.append({
                "title": Markup(title),
                "url": r["url"],
                "domain": urlparse(r["url"]).netloc,
                "snippet": Markup(display_snippet),
                "lang": r["language"],
                "verified": bool(r["domain_rank"] and r["domain_rank"] <= 5000),
                "rank": r["domain_rank"]
            })

    except Exception as e:
        print(f"Search Error: {e}")
        results = []
        total_count = 0

    conn.close()

    return render_template(
        "index.html",
        query=raw_query,
        results=results,
        count="{:,}".format(total_count),
        time=round(time.time()-start, 4),
        page=page,
        total_pages=(total_count + PER_PAGE_DISPLAY - 1) // PER_PAGE_DISPLAY
    )


@app.route("/suggest")
def suggest():
    q = request.args.get("q","")
    if len(q) < 2:
        return jsonify([])

    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT title FROM visited WHERE title LIKE ? LIMIT 5",
        (f"%{q}%",)
    )

    seen=set()
    out=[]
    for r in c.fetchall():
        t=r["title"]
        if t and t not in seen and len(t)<60:
            out.append(t)
            seen.add(t)

    conn.close()
    return jsonify(out)