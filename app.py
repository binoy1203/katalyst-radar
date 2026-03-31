"""
Katalyst Judgment Radar v3
Comprehensive legal intelligence system with multiple source fetchers.
"""

import os
import json
import sqlite3
import hashlib
import logging
import time
import re
import threading
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import quote

import requests
import feedparser
from flask import Flask, jsonify, request
from apscheduler.schedulers.background import BackgroundScheduler

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-haiku-4-5-20251001"
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "radar.db")
SWEEP_INTERVAL_HOURS = int(os.environ.get("SWEEP_INTERVAL_HOURS", "12"))
PORT = int(os.environ.get("PORT", "8080"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("radar")

app = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS judgments (
        id TEXT PRIMARY KEY, title TEXT NOT NULL, court TEXT,
        date_decided TEXT, date_fetched TEXT NOT NULL, source TEXT NOT NULL,
        source_url TEXT, full_text_snippet TEXT, categories TEXT,
        sections TEXT, relevance_score TEXT, practitioner_note TEXT,
        raw_classification TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS sweep_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, sweep_time TEXT NOT NULL,
        source TEXT NOT NULL, results_fetched INTEGER,
        results_relevant INTEGER, errors TEXT)""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date_fetched ON judgments(date_fetched)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_categories ON judgments(categories)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_relevance ON judgments(relevance_score)")
    conn.commit()
    conn.close()
    logger.info("Database initialized at %s", DB_PATH)


SEARCH_QUERIES_NARROW = [
    "scheme of arrangement NCLT 2026",
    "demerger scheme NCLT order 2026",
    "amalgamation scheme NCLT 2026",
    "composite scheme demerger amalgamation",
    "capital reduction section 66 NCLT",
    "family arrangement settlement deed judgment",
    "family settlement partition High Court",
    "Hindu Succession coparcenary partition",
    "section 2(19AA) demerger ITAT",
    "section 47 exemption amalgamation ITAT",
    "section 56(2)(x) deemed gift tribunal",
    "section 45(4) reconstitution firm ITAT",
    "slump sale section 50B ITAT 2026",
    "SEBI regulation 10 inter se transfer",
    "open offer exemption SAT order",
    "stamp duty scheme arrangement High Court",
    "FEMA share transfer NRI family",
    "private trust capital gains ITAT",
    "section 29A IBC related party",
    "oppression mismanagement NCLT family",
]

SEARCH_QUERIES_BROAD = [
    "NCLT scheme of arrangement order",
    "NCLAT appeal scheme amalgamation",
    "demerger tax neutrality judgment India",
    "family business partition judgment India",
    "HUF partition capital gains",
    "listed company promoter restructuring SEBI",
    "delisting regulation family group",
    "trust taxation India ITAT",
    "LLP reconstitution tax",
    "section 9B dissolution partnership",
    "appointed date scheme arrangement NCLT",
    "valuation scheme arrangement",
    "related party transaction SEBI order",
    "stamp duty exemption amalgamation",
    "succession planning India trust",
    "family settlement registration requirement",
    "cross border merger India NCLT",
    "FEMA pricing guidelines share transfer",
    "RBI compounding order FEMA",
    "IBC resolution plan family company",
]

CLASSIFICATION_PROMPT = """You are a legal classification engine for an M&A and transaction structuring advisory firm in India (Katalyst Advisors). Your task is to determine whether a given judgment, order, or legal article is relevant to the firm's practice areas, and if so, classify it.

TAXONOMY OF RELEVANT CATEGORIES:

1. SCHEMES OF ARRANGEMENT (Companies Act 2013, Sections 230-232, 66)
   Covers: amalgamations, demergers, composite schemes, capital reductions, NCLT sanctions, appointed dates, class meetings, valuation methodology in schemes

2. FAMILY ARRANGEMENT AND SETTLEMENT
   Covers: family settlement deeds, partition disputes, Hindu Succession Act, Indian Succession Act, registration and enforceability of family arrangements, family arrangements effected through corporate structures

3. INCOME TAX (Succession and Restructuring)
   Covers: Section 2(19AA) demerger definition, Section 47 exemptions, Section 56(2)(x) deemed gifts, Section 45(4)/9B firm reconstitution, Section 50D FMV, Section 171 HUF partition, Section 50B slump sales, capital gains on family transfers

4. SEBI AND SECURITIES LAW
   Covers: Regulation 10 inter se transfers, open offer exemptions, Regulation 23 RPTs, Regulation 37 schemes, delisting in family context, SAT orders on change of control

5. STAMP DUTY AND REGISTRATION
   Covers: whether family arrangements are conveyances, stamp duty on schemes, state-level exemptions, registration requirements, Hindustan Lever line of cases

6. FEMA AND CROSS-BORDER
   Covers: FEMA pricing guidelines for family transfers involving NRIs, RBI compounding orders, LRS and inheritance, cross-border succession

7. TRUST LAW AND SUCCESSION VEHICLES
   Covers: private trusts, Sections 60-64 revocable transfers, Sections 161-164 trust taxation, trust distributions, irrevocable trust transfers

8. INSOLVENCY INTERSECTION
   Covers: Section 29A related party issues in family groups, oppression/mismanagement under Sections 241-244 in family disputes, IBC proceedings in family-controlled companies

INSTRUCTIONS:
Analyze the following judgment/article text and respond with a JSON object (no markdown, no backticks) containing:
{
  "relevant": true/false,
  "relevance_score": "high" | "medium" | "low",
  "categories": [list of category numbers that apply, e.g. [1, 3]],
  "category_names": [list of category names],
  "sections_engaged": [list of specific section numbers/regulations mentioned],
  "court_or_tribunal": "name of court/tribunal",
  "date_decided": "date if identifiable, else null",
  "parties": "party names if identifiable",
  "structural_mechanism": "brief description of the transaction structure or legal mechanism at issue",
  "practitioner_note": "A 2-3 sentence note written for a senior M&A structuring partner explaining WHY this judgment matters for transaction practice. Focus on the structural principle established, the precedent value, or the risk/opportunity it creates. Write as a peer, not as a reporter."
}

If the text is NOT relevant to any of the eight categories, return:
{"relevant": false, "relevance_score": "none", "reason": "brief reason"}

TEXT TO CLASSIFY:
"""


def safe_get(url, timeout=30):
    try:
        resp = requests.get(url, timeout=timeout, headers=HEADERS)
        if resp.status_code == 200:
            return resp
    except Exception as e:
        logger.error("HTTP error for %s: %s", url[:80], str(e)[:100])
    return None


def strip_html(text):
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def fetch_google_search(query, num=10):
    results = []
    try:
        search_url = "https://www.google.com/search?q=" + quote(query) + "&num=" + str(num)
        resp = safe_get(search_url)
        if resp:
            matches = re.findall(r'<a href="/url\?q=(https?://[^&"]+)', resp.text)
            title_matches = re.findall(r'<h3[^>]*>(.*?)</h3>', resp.text, re.DOTALL)
            for i, url in enumerate(matches[:num]):
                if "google.com" in url or "googleapis.com" in url:
                    continue
                title = strip_html(title_matches[i]) if i < len(title_matches) else url
                results.append({"title": title, "url": url, "snippet": "", "source": "Google Search"})
    except Exception as e:
        logger.error("Google search error for '%s': %s", query[:50], str(e)[:100])
    return results


def fetch_indian_kanoon(query, page=0):
    results = []
    try:
        api_key = os.environ.get("INDIAN_KANOON_API_KEY", "")
        if api_key:
            url = "https://api.indiankanoon.org/search/"
            params = {"formInput": query, "pagenum": page}
            ik_headers = {"Authorization": "Token " + api_key}
            resp = requests.get(url, params=params, headers=ik_headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                for doc in data.get("docs", []):
                    results.append({
                        "title": doc.get("title", ""),
                        "url": "https://indiankanoon.org/doc/" + str(doc.get("tid", "")) + "/",
                        "snippet": doc.get("headline", ""),
                        "source": "Indian Kanoon"
                    })
        else:
            gquery = "site:indiankanoon.org " + query
            g_results = fetch_google_search(gquery, 5)
            for r in g_results:
                if "indiankanoon.org" in r["url"]:
                    r["source"] = "Indian Kanoon (via Google)"
                    results.append(r)
    except Exception as e:
        logger.error("Indian Kanoon error for '%s': %s", query[:50], str(e)[:100])
    return results


def fetch_nclt_orders():
    results = []
    try:
        benches = [
            ("https://nclt.gov.in/order-judgment-by-bench/principal-bench-new-delhi", "NCLT Delhi"),
            ("https://nclt.gov.in/order-judgment-by-bench/mumbai-bench", "NCLT Mumbai"),
            ("https://nclt.gov.in/order-judgment-by-bench/ahmedabad-bench", "NCLT Ahmedabad"),
            ("https://nclt.gov.in/order-judgment-by-bench/bengaluru-bench", "NCLT Bengaluru"),
            ("https://nclt.gov.in/order-judgment-by-bench/chennai-bench", "NCLT Chennai"),
            ("https://nclt.gov.in/order-judgment-by-bench/kolkata-bench", "NCLT Kolkata"),
        ]
        for bench_url, bench_name in benches:
            resp = safe_get(bench_url)
            if resp:
                matches = re.findall(r'<a[^>]+href="([^"]*)"[^>]*>(.*?)</a>', resp.text, re.DOTALL)
                for href, title_raw in matches:
                    title = strip_html(title_raw).strip()
                    if len(title) > 15 and ("order" in href.lower() or "judgment" in href.lower() or ".pdf" in href.lower()):
                        full_url = href if href.startswith("http") else "https://nclt.gov.in" + href
                        results.append({"title": title[:200], "url": full_url, "snippet": "", "source": bench_name})
            time.sleep(1)
    except Exception as e:
        logger.error("NCLT fetch error: %s", str(e)[:100])
    logger.info("NCLT: %d results from direct scrape", len(results))
    return results


def fetch_nclat_orders():
    results = []
    try:
        resp = safe_get("https://nclat.nic.in/?page_id=585")
        if resp:
            matches = re.findall(r'<a[^>]+href="([^"]*)"[^>]*>(.*?)</a>', resp.text, re.DOTALL)
            for href, title_raw in matches:
                title = strip_html(title_raw).strip()
                if len(title) > 15 and (".pdf" in href.lower() or "order" in href.lower()):
                    full_url = href if href.startswith("http") else "https://nclat.nic.in/" + href
                    results.append({"title": title[:200], "url": full_url, "snippet": "", "source": "NCLAT"})
    except Exception as e:
        logger.error("NCLAT fetch error: %s", str(e)[:100])
    logger.info("NCLAT: %d results", len(results))
    return results


def fetch_sci_judgments():
    results = []
    try:
        resp = safe_get("https://main.sci.gov.in/judgments")
        if resp:
            matches = re.findall(r'<a[^>]+href="([^"]*)"[^>]*>(.*?)</a>', resp.text, re.DOTALL)
            for href, title_raw in matches:
                title = strip_html(title_raw).strip()
                if len(title) > 15 and ("judgment" in href.lower() or ".pdf" in href.lower() or "jonew" in href.lower()):
                    full_url = href if href.startswith("http") else "https://main.sci.gov.in" + href
                    results.append({"title": title[:200], "url": full_url, "snippet": "", "source": "Supreme Court of India"})
    except Exception as e:
        logger.error("SCI fetch error: %s", str(e)[:100])
    logger.info("Supreme Court: %d results", len(results))
    return results


def fetch_sebi_orders():
    results = []
    try:
        urls = [
            ("https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0", "SEBI Orders"),
            ("https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=2&ssid=9&smid=0", "SEBI Circulars"),
        ]
        for sebi_url, source_name in urls:
            resp = safe_get(sebi_url)
            if resp:
                matches = re.findall(r'<a[^>]+href="([^"]*)"[^>]*>(.*?)</a>', resp.text, re.DOTALL)
                for href, title_raw in matches:
                    title = strip_html(title_raw).strip()
                    if len(title) > 15:
                        full_url = href if href.startswith("http") else "https://www.sebi.gov.in" + href
                        results.append({"title": title[:200], "url": full_url, "snippet": "", "source": source_name})
            time.sleep(1)
    except Exception as e:
        logger.error("SEBI fetch error: %s", str(e)[:100])
    logger.info("SEBI: %d results", len(results))
    return results


def fetch_sat_orders():
    results = []
    try:
        resp = safe_get("https://sat.gov.in/english/orders.htm")
        if resp:
            matches = re.findall(r'<a[^>]+href="([^"]*)"[^>]*>(.*?)</a>', resp.text, re.DOTALL)
            for href, title_raw in matches:
                title = strip_html(title_raw).strip()
                if len(title) > 10 and (".pdf" in href.lower() or "order" in href.lower()):
                    full_url = href if href.startswith("http") else "https://sat.gov.in/english/" + href
                    results.append({"title": title[:200], "url": full_url, "snippet": "", "source": "SAT"})
    except Exception as e:
        logger.error("SAT fetch error: %s", str(e)[:100])
    logger.info("SAT: %d results", len(results))
    return results


def fetch_itat_orders():
    results = []
    queries = [
        "site:itatonline.org demerger section 2(19AA)",
        "site:itatonline.org slump sale section 50B",
        "site:itatonline.org family partition HUF",
        "site:itatonline.org trust taxation section 161",
        "site:itatonline.org capital gains exemption section 47",
        "site:itatonline.org section 56(2)(x) gift",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "ITAT (via Google)"
            results.append(r)
        time.sleep(1)
    logger.info("ITAT: %d results via Google", len(results))
    return results


RSS_FEEDS = [
    ("https://www.livelaw.in/feed", "LiveLaw"),
    ("https://www.barandbench.com/feed", "Bar and Bench"),
    ("https://www.taxmann.com/post/blog/feed/", "Taxmann"),
    ("https://taxguru.in/feed", "TaxGuru"),
    ("https://www.scconline.com/blog/post/feed/", "SCC Online Blog"),
    ("https://www.mondaq.com/india/feeds/rss/latest", "Mondaq India"),
    ("https://corporate.cyrilamarchandblogs.com/feed/", "CAM Blog"),
    ("https://indiacorplaw.in/feed", "IndiaCorpLaw"),
    ("https://taxguru.in/income-tax/feed", "TaxGuru Income Tax"),
    ("https://taxguru.in/company-law/feed", "TaxGuru Company Law"),
    ("https://taxguru.in/sebi/feed", "TaxGuru SEBI"),
    ("https://www.livelaw.in/tax-cases/feed", "LiveLaw Tax"),
    ("https://www.livelaw.in/corporate-law/feed", "LiveLaw Corporate"),
]


def fetch_rss_feed(feed_url, source_name):
    results = []
    try:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries[:20]:
            results.append({
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "snippet": strip_html(entry.get("summary", entry.get("description", "")))[:1000],
                "source": source_name,
                "published": entry.get("published", "")
            })
    except Exception as e:
        logger.error("RSS error for %s: %s", source_name, str(e)[:100])
    return results


def fetch_google_legal_news():
    results = []
    queries = [
        "NCLT scheme arrangement order 2026",
        "NCLT demerger order 2026",
        "family arrangement judgment India 2026",
        "ITAT demerger slump sale 2026",
        "SEBI open offer exemption order 2026",
        "stamp duty scheme arrangement High Court 2026",
        "FEMA NRI share transfer ruling 2026",
        "trust taxation ITAT India 2026",
        "NCLT amalgamation order 2026",
        "family settlement deed validity judgment",
        "section 56(2)(x) deemed gift ITAT",
        "capital reduction NCLT order",
        "SAT SEBI takeover regulation order",
        "composite scheme demerger NCLT",
        "HUF partition tax implications judgment",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 5)
        results.extend(g_results)
        time.sleep(2)
    logger.info("Google legal news: %d results", len(results))
    return results


def fetch_full_text(url):
    try:
        resp = safe_get(url)
        if resp:
            text = strip_html(resp.text)
            return text[:4000]
    except Exception as e:
        logger.error("Full text error for %s: %s", url[:50], str(e)[:100])
    return ""


def classify_judgment(text, title=""):
    if not ANTHROPIC_API_KEY:
        logger.error("No ANTHROPIC_API_KEY set.")
        return None
    combined_text = ("TITLE: " + title + "\n\nTEXT:\n" + text) if title else text
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": CLASSIFICATION_PROMPT + combined_text}],
            },
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            content = data.get("content", [{}])[0].get("text", "")
            content = content.strip().strip("`").strip()
            if content.startswith("json"):
                content = content[4:].strip()
            return json.loads(content)
        else:
            logger.error("Claude API error %d: %s", resp.status_code, resp.text[:200])
            return None
    except json.JSONDecodeError as e:
        logger.error("JSON parse error: %s", e)
        return None
    except Exception as e:
        logger.error("Classification error: %s", e)
        return None


def generate_id(title, url):
    raw = title + "|" + url
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def is_already_processed(judgment_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT 1 FROM judgments WHERE id = ?", (judgment_id,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists


def save_judgment(judgment_id, raw, classification):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""INSERT OR REPLACE INTO judgments
        (id, title, court, date_decided, date_fetched, source, source_url,
         full_text_snippet, categories, sections, relevance_score,
         practitioner_note, raw_classification)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (judgment_id, raw.get("title", "Unknown"),
         classification.get("court_or_tribunal", ""),
         classification.get("date_decided", ""),
         datetime.utcnow().isoformat(),
         raw.get("source", ""), raw.get("url", ""),
         raw.get("snippet", "")[:500],
         json.dumps(classification.get("categories", [])),
         json.dumps(classification.get("sections_engaged", [])),
         classification.get("relevance_score", ""),
         classification.get("practitioner_note", ""),
         json.dumps(classification)))
    conn.commit()
    conn.close()


def log_sweep(source, fetched, relevant, errors=""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""INSERT INTO sweep_log (sweep_time, source, results_fetched, results_relevant, errors)
        VALUES (?, ?, ?, ?, ?)""",
        (datetime.utcnow().isoformat(), source, fetched, relevant, errors))
    conn.commit()
    conn.close()


def process_results(results, source_label):
    relevant_count = 0
    seen = set()
    unique = []
    for r in results:
        if r["url"] not in seen:
            seen.add(r["url"])
            unique.append(r)
    logger.info("%s: %d unique results to process", source_label, len(unique))
    for result in unique:
        jid = generate_id(result["title"], result["url"])
        if is_already_processed(jid):
            continue
        text = result.get("snippet", "")
        if not text or len(text) < 80:
            text = fetch_full_text(result["url"])
            time.sleep(0.5)
        if not text or len(text) < 30:
            continue
        classification = classify_judgment(text, result["title"])
        if classification and classification.get("relevant"):
            save_judgment(jid, result, classification)
            relevant_count += 1
            logger.info("  RELEVANT: %s [%s]", result["title"][:80], classification.get("relevance_score", ""))
        time.sleep(0.3)
    log_sweep(source_label, len(unique), relevant_count)
    return relevant_count


def run_sweep():
    logger.info("=== STARTING COMPREHENSIVE SWEEP at %s ===", datetime.utcnow().isoformat())
    total_relevant = 0

    logger.info("--- Phase 1: RSS Feeds ---")
    rss_results = []
    for feed_url, source_name in RSS_FEEDS:
        logger.info("Fetching %s...", source_name)
        results = fetch_rss_feed(feed_url, source_name)
        rss_results.extend(results)
        time.sleep(0.5)
    total_relevant += process_results(rss_results, "RSS Feeds")

    logger.info("--- Phase 2: Indian Kanoon ---")
    ik_results = []
    for query in SEARCH_QUERIES_NARROW:
        results = fetch_indian_kanoon(query)
        ik_results.extend(results)
        time.sleep(1.5)
    total_relevant += process_results(ik_results, "Indian Kanoon")

    logger.info("--- Phase 3: Court Websites ---")
    logger.info("Fetching NCLT orders...")
    total_relevant += process_results(fetch_nclt_orders(), "NCLT Direct")
    logger.info("Fetching NCLAT orders...")
    total_relevant += process_results(fetch_nclat_orders(), "NCLAT Direct")
    logger.info("Fetching Supreme Court judgments...")
    total_relevant += process_results(fetch_sci_judgments(), "Supreme Court")
    logger.info("Fetching SEBI orders...")
    total_relevant += process_results(fetch_sebi_orders(), "SEBI")
    logger.info("Fetching SAT orders...")
    total_relevant += process_results(fetch_sat_orders(), "SAT")

    logger.info("--- Phase 4: ITAT via Google ---")
    total_relevant += process_results(fetch_itat_orders(), "ITAT")

    logger.info("--- Phase 5: Google Legal News ---")
    total_relevant += process_results(fetch_google_legal_news(), "Google Legal News")

    logger.info("--- Phase 6: Broad Keyword Search ---")
    broad_results = []
    for query in SEARCH_QUERIES_BROAD:
        results = fetch_indian_kanoon(query)
        broad_results.extend(results)
        time.sleep(1.5)
    total_relevant += process_results(broad_results, "Broad Search")

    logger.info("=== SWEEP COMPLETE. Total relevant: %d ===", total_relevant)


@app.route("/")
def index():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "index.html")
    with open(html_path, "r") as f:
        return f.read(), 200, {"Content-Type": "text/html; charset=utf-8"}


@app.route("/api/judgments")
def get_judgments():
    category = request.args.get("category", "")
    score = request.args.get("score", "")
    days = int(request.args.get("days", "90"))
    search = request.args.get("search", "")
    page = int(request.args.get("page", "1"))
    per_page = 20
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    where_clauses = []
    params = []
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    where_clauses.append("date_fetched >= ?")
    params.append(cutoff)
    if category:
        where_clauses.append("categories LIKE ?")
        params.append("%" + category + "%")
    if score:
        where_clauses.append("relevance_score = ?")
        params.append(score)
    if search:
        where_clauses.append("(title LIKE ? OR practitioner_note LIKE ? OR sections LIKE ?)")
        params.extend(["%" + search + "%"] * 3)
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    offset = (page - 1) * per_page
    count_sql = "SELECT COUNT(*) as cnt FROM judgments WHERE " + where_sql
    total = conn.execute(count_sql, params).fetchone()["cnt"]
    query_sql = "SELECT * FROM judgments WHERE " + where_sql + " ORDER BY date_fetched DESC LIMIT ? OFFSET ?"
    rows = conn.execute(query_sql, params + [per_page, offset]).fetchall()
    conn.close()
    judgments = []
    for row in rows:
        judgments.append({
            "id": row["id"], "title": row["title"], "court": row["court"],
            "date_decided": row["date_decided"], "date_fetched": row["date_fetched"],
            "source": row["source"], "source_url": row["source_url"],
            "categories": json.loads(row["categories"]) if row["categories"] else [],
            "sections": json.loads(row["sections"]) if row["sections"] else [],
            "relevance_score": row["relevance_score"],
            "practitioner_note": row["practitioner_note"],
        })
    return jsonify({"judgments": judgments, "total": total, "page": page,
                     "per_page": per_page, "total_pages": (total + per_page - 1) // per_page})


@app.route("/api/stats")
def get_stats():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    total = conn.execute("SELECT COUNT(*) as cnt FROM judgments").fetchone()["cnt"]
    high = conn.execute("SELECT COUNT(*) as cnt FROM judgments WHERE relevance_score = 'high'").fetchone()["cnt"]
    last_sweep = conn.execute("SELECT sweep_time FROM sweep_log ORDER BY id DESC LIMIT 1").fetchone()
    last_sweep_time = last_sweep["sweep_time"] if last_sweep else None
    all_cats = conn.execute("SELECT categories FROM judgments").fetchall()
    cat_counts = {}
    for row in all_cats:
        cats = json.loads(row["categories"]) if row["categories"] else []
        for c in cats:
            cat_counts[str(c)] = cat_counts.get(str(c), 0) + 1
    conn.close()
    return jsonify({"total_judgments": total, "high_relevance": high,
                     "last_sweep": last_sweep_time, "category_distribution": cat_counts})


@app.route("/api/sweep", methods=["POST"])
def trigger_sweep():
    t = threading.Thread(target=run_sweep)
    t.start()
    return jsonify({"status": "Sweep started", "time": datetime.utcnow().isoformat()})


init_db()
scheduler = BackgroundScheduler()
scheduler.add_job(run_sweep, "interval", hours=SWEEP_INTERVAL_HOURS, id="main_sweep",
                  next_run_time=datetime.utcnow() + timedelta(minutes=2))
scheduler.start()
logger.info("Scheduler started. Sweeps every %d hours.", SWEEP_INTERVAL_HOURS)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
