"""
Katalyst Judgment Radar
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


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS judgments (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            court TEXT,
            date_decided TEXT,
            date_fetched TEXT NOT NULL,
            source TEXT NOT NULL,
            source_url TEXT,
            full_text_snippet TEXT,
            categories TEXT,
            sections TEXT,
            relevance_score TEXT,
            practitioner_note TEXT,
            raw_classification TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sweep_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sweep_time TEXT NOT NULL,
            source TEXT NOT NULL,
            results_fetched INTEGER,
            results_relevant INTEGER,
            errors TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date_fetched ON judgments(date_fetched)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_categories ON judgments(categories)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_relevance ON judgments(relevance_score)")
    conn.commit()
    conn.close()
    logger.info("Database initialized at %s", DB_PATH)


SEARCH_QUERIES = [
    "scheme of arrangement section 230",
    "scheme of arrangement section 232",
    "amalgamation NCLT",
    "demerger scheme NCLT",
    "composite scheme demerger amalgamation",
    "capital reduction section 66",
    "appointed date scheme",
    "NCLT scheme sanction",
    "family arrangement settlement deed",
    "family settlement transfer",
    "Kale v Deputy Director family arrangement",
    "family partition registration stamp duty",
    "Hindu Succession Act section 6 coparcenary",
    "family arrangement memorandum",
    "section 2(19AA) demerger",
    "section 47 exemption amalgamation",
    "section 56(2)(x) deemed gift",
    "section 45(4) reconstitution firm",
    "section 9B dissolution firm",
    "section 50D fair market value",
    "HUF partition section 171",
    "section 47(iii) gift transfer",
    "slump sale section 50B",
    "capital gains family partition",
    "SEBI regulation 10 inter se transfer promoter",
    "open offer exemption family restructuring",
    "SEBI regulation 23 related party transaction",
    "SEBI regulation 37 scheme arrangement listing",
    "delisting equity family",
    "SAT order open offer",
    "stamp duty scheme arrangement",
    "conveyance family arrangement stamp",
    "amalgamation stamp duty exemption",
    "Hindustan Lever stamp duty scheme",
    "FEMA share transfer NRI family",
    "FEMA pricing guidelines family transfer",
    "RBI compounding order family",
    "liberalised remittance scheme inheritance",
    "private trust transfer capital gains",
    "section 60 revocable transfer trust",
    "section 161 discretionary trust taxation",
    "irrevocable trust settlor beneficiary",
    "trust succession planning",
    "section 29A related party resolution plan",
    "oppression mismanagement family section 241",
    "IBC family business dispute",
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


def fetch_indian_kanoon(query, page=0):
    results = []
    try:
        if not os.environ.get("INDIAN_KANOON_API_KEY"):
            public_url = "https://indiankanoon.org/search/?formInput=" + requests.utils.quote(query)
            resp = requests.get(public_url, timeout=30, headers={"User-Agent": "KatalystRadar/1.0"})
            if resp.status_code == 200:
                matches = re.findall(r'<a href="(/doc/\d+/)"[^>]*>(.*?)</a>', resp.text)
                for match in matches[:10]:
                    path, title = match
                    clean_title = re.sub(r'<[^>]+>', '', title).strip()
                    if clean_title and len(clean_title) > 10:
                        results.append({
                            "title": clean_title,
                            "url": "https://indiankanoon.org" + path,
                            "snippet": "",
                            "source": "Indian Kanoon"
                        })
        else:
            url = "https://api.indiankanoon.org/search/"
            params = {"formInput": query, "pagenum": page}
            headers = {"Authorization": "Token " + os.environ.get("INDIAN_KANOON_API_KEY", "")}
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                for doc in data.get("docs", []):
                    results.append({
                        "title": doc.get("title", ""),
                        "url": "https://indiankanoon.org/doc/" + str(doc.get("tid", "")) + "/",
                        "snippet": doc.get("headline", ""),
                        "source": "Indian Kanoon"
                    })
    except Exception as e:
        logger.error("Indian Kanoon fetch error for '%s': %s", query, e)
    return results


def fetch_rss_feed(feed_url, source_name):
    results = []
    try:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries[:15]:
            results.append({
                "title": entry.get("title", ""),
                "url": entry.get("link", ""),
                "snippet": entry.get("summary", entry.get("description", "")),
                "source": source_name,
                "published": entry.get("published", "")
            })
    except Exception as e:
        logger.error("RSS fetch error for %s: %s", source_name, e)
    return results


RSS_FEEDS = [
    ("https://www.livelaw.in/feed", "LiveLaw"),
    ("https://www.barandbench.com/feed", "Bar and Bench"),
    ("https://www.taxmann.com/post/blog/feed/", "Taxmann"),
    ("https://taxguru.in/feed", "TaxGuru"),
    ("https://www.scconline.com/blog/post/feed/", "SCC Online Blog"),
]


def fetch_judgment_full_text(url):
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "KatalystRadar/1.0"})
        if resp.status_code == 200:
            text = re.sub(r'<[^>]+>', ' ', resp.text)
            text = re.sub(r'\s+', ' ', text).strip()
            return text[:3000]
    except Exception as e:
        logger.error("Full text fetch error for %s: %s", url, e)
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
    conn.execute("""
        INSERT OR REPLACE INTO judgments
        (id, title, court, date_decided, date_fetched, source, source_url,
         full_text_snippet, categories, sections, relevance_score,
         practitioner_note, raw_classification)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        judgment_id,
        raw.get("title", "Unknown"),
        classification.get("court_or_tribunal", ""),
        classification.get("date_decided", ""),
        datetime.utcnow().isoformat(),
        raw.get("source", ""),
        raw.get("url", ""),
        raw.get("snippet", "")[:500],
        json.dumps(classification.get("categories", [])),
        json.dumps(classification.get("sections_engaged", [])),
        classification.get("relevance_score", ""),
        classification.get("practitioner_note", ""),
        json.dumps(classification),
    ))
    conn.commit()
    conn.close()


def log_sweep(source, fetched, relevant, errors=""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO sweep_log (sweep_time, source, results_fetched, results_relevant, errors)
        VALUES (?, ?, ?, ?, ?)
    """, (datetime.utcnow().isoformat(), source, fetched, relevant, errors))
    conn.commit()
    conn.close()


def run_sweep():
    logger.info("=== Starting sweep at %s ===", datetime.utcnow().isoformat())
    total_fetched = 0
    total_relevant = 0

    logger.info("Sweeping Indian Kanoon...")
    ik_results = []
    for query in SEARCH_QUERIES:
        results = fetch_indian_kanoon(query)
        ik_results.extend(results)
        time.sleep(1)

    seen_urls = set()
    unique_ik = []
    for r in ik_results:
        if r["url"] not in seen_urls:
            seen_urls.add(r["url"])
            unique_ik.append(r)
    ik_results = unique_ik

    logger.info("Indian Kanoon: %d unique results", len(ik_results))
    total_fetched += len(ik_results)

    for result in ik_results:
        jid = generate_id(result["title"], result["url"])
        if is_already_processed(jid):
            continue
        full_text = result.get("snippet", "")
        if not full_text or len(full_text) < 100:
            full_text = fetch_judgment_full_text(result["url"])
            time.sleep(0.5)
        if not full_text:
            continue
        classification = classify_judgment(full_text, result["title"])
        if classification and classification.get("relevant"):
            save_judgment(jid, result, classification)
            total_relevant += 1
            logger.info("  RELEVANT: %s [%s]", result["title"][:80], classification.get("relevance_score", ""))
        time.sleep(0.3)

    log_sweep("Indian Kanoon", len(ik_results), total_relevant)

    rss_relevant = 0
    for feed_url, source_name in RSS_FEEDS:
        logger.info("Sweeping %s...", source_name)
        results = fetch_rss_feed(feed_url, source_name)
        total_fetched += len(results)
        for result in results:
            jid = generate_id(result["title"], result["url"])
            if is_already_processed(jid):
                continue
            text = result.get("snippet", "")
            if not text or len(text) < 50:
                continue
            classification = classify_judgment(text, result["title"])
            if classification and classification.get("relevant"):
                save_judgment(jid, result, classification)
                rss_relevant += 1
                total_relevant += 1
                logger.info("  RELEVANT: %s [%s]", result["title"][:80], classification.get("relevance_score", ""))
            time.sleep(0.3)
        log_sweep(source_name, len(results), rss_relevant)

    logger.info("=== Sweep complete. %d fetched, %d relevant ===", total_fetched, total_relevant)


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
            "id": row["id"],
            "title": row["title"],
            "court": row["court"],
            "date_decided": row["date_decided"],
            "date_fetched": row["date_fetched"],
            "source": row["source"],
            "source_url": row["source_url"],
            "categories": json.loads(row["categories"]) if row["categories"] else [],
            "sections": json.loads(row["sections"]) if row["sections"] else [],
            "relevance_score": row["relevance_score"],
            "practitioner_note": row["practitioner_note"],
        })

    return jsonify({
        "judgments": judgments,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    })


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
    return jsonify({
        "total_judgments": total,
        "high_relevance": high,
        "last_sweep": last_sweep_time,
        "category_distribution": cat_counts,
    })


@app.route("/api/sweep", methods=["POST"])
def trigger_sweep():
    t = threading.Thread(target=run_sweep)
    t.start()
    return jsonify({"status": "Sweep started", "time": datetime.utcnow().isoformat()})


# Initialize database and scheduler at module level
init_db()

scheduler = BackgroundScheduler()
scheduler.add_job(
    run_sweep,
    "interval",
    hours=SWEEP_INTERVAL_HOURS,
    id="main_sweep",
    next_run_time=datetime.utcnow() + timedelta(minutes=2),
)
scheduler.start()
logger.info("Scheduler started. Sweeps every %d hours.", SWEEP_INTERVAL_HOURS)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
