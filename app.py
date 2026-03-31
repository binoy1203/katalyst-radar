"""
Katalyst Judgment Radar v5 (Updated)
Wider searches. Liberal classification. Short summaries.
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


CLASSIFICATION_PROMPT = """You are a classification engine for Katalyst Advisors, an M&A and transaction structuring advisory firm in India. Your job is to determine if a judgment, order, article, or news report has ANY relevance to the firm's practice.

IMPORTANT: Be LIBERAL in classifying. If there is any reasonable connection to M&A, corporate restructuring, family business, corporate governance, taxation of transactions, regulatory action on companies, or deal-related activity, classify it as relevant. When in doubt, include it. It is better to include a borderline result than to miss something useful.

CATEGORIES:

1. SCHEMES OF ARRANGEMENT: amalgamations, mergers, demergers, composite schemes, capital reductions, NCLT/NCLAT orders on schemes, appointed dates, class meetings, valuation disputes
2. FAMILY ARRANGEMENT: family settlements, partitions, HUF disputes, succession disputes, family business splits, will contests, inheritance disputes involving business assets
3. INCOME TAX: any tax ruling touching M&A, restructuring, capital gains on transfers, slump sales, demerger tax treatment, gift tax, deemed income, firm reconstitution, trust taxation, HUF taxation
4. SEBI / SECURITIES: takeover regulations, open offers, inter se transfers, RPTs, insider trading in context of restructuring, delisting, scheme-related SEBI orders, promoter reclassification
5. STAMP DUTY: stamp duty on transfers, schemes, conveyances, family arrangements, state-level rulings
6. FEMA / CROSS-BORDER: any FEMA ruling or article on cross-border transfers, NRI transactions, FDI in restructuring, RBI directions
7. TRUST LAW: private trusts, charitable trusts in business context, trust taxation, settlor-beneficiary disputes
8. INSOLVENCY: IBC matters involving promoter families, Section 29A, oppression and mismanagement, resolution plans, liquidation of family companies
9. BOARDROOM / GOVERNANCE: promoter disputes, board-level fights, director removals, EGM battles, shareholder activism, SEBI governance enforcement, proxy advisory actions, corporate governance failures
10. IND AS / ACCOUNTING: business combination accounting, Ind AS 103, common control, consolidation, fair value, purchase price allocation, EAC opinions, accounting for schemes

Respond with JSON only (no markdown, no backticks):
{
  "relevant": true/false,
  "relevance_score": "high" | "medium" | "low",
  "categories": [category numbers],
  "category_names": [names],
  "sections_engaged": [specific sections/regulations/standards if mentioned],
  "court_or_tribunal": "source court/tribunal/regulator or 'News' or 'Article'",
  "date_decided": "date or null",
  "parties": "names or null",
  "summary": "One to two line summary of what this is about and why it matters."
}

If NOT relevant: {"relevant": false, "relevance_score": "none", "reason": "brief reason"}

TEXT:
"""

# ---------------------------------------------------------------------------
# Search Queries: Technical + Plain English + Broad
# ---------------------------------------------------------------------------

SEARCH_QUERIES_TECHNICAL = [
    "scheme of arrangement NCLT 2026",
    "demerger scheme NCLT order",
    "amalgamation NCLT order",
    "section 230 Companies Act scheme",
    "section 232 Companies Act amalgamation",
    "section 66 capital reduction NCLT",
    "section 2(19AA) demerger",
    "section 47 exemption amalgamation transfer",
    "section 56(2)(x) deemed gift",
    "section 45(4) reconstitution firm",
    "section 9B dissolution partnership LLP",
    "section 50B slump sale",
    "section 50D fair market value",
    "section 171 HUF partition",
    "section 47(iii) gift transfer",
    "SEBI regulation 10 inter se transfer",
    "SEBI regulation 23 related party",
    "SEBI regulation 37 scheme listing",
    "SEBI takeover code open offer",
    "section 29A IBC related party",
    "section 241 oppression mismanagement",
    "Ind AS 103 business combination",
    "Ind AS 110 consolidation",
    "Ind AS 12 deferred tax",
    "Ind AS 113 fair value",
]

SEARCH_QUERIES_PLAIN_ENGLISH = [
    "company merger India",
    "company demerger India",
    "corporate restructuring India",
    "business restructuring India tax",
    "group restructuring India",
    "family business split India",
    "family business dispute India",
    "family business succession India",
    "promoter family fight India",
    "promoter shareholding change India",
    "promoter group restructuring",
    "listed company restructuring India",
    "business transfer agreement India",
    "slump sale India deal",
    "asset sale company India",
    "share swap merger India",
    "holding subsidiary restructuring India",
    "private equity buyout India",
    "open offer SEBI India",
    "delisting India promoter",
    "corporate governance India SEBI action",
    "boardroom fight India",
    "independent director India controversy",
    "shareholder dispute India",
    "minority shareholder India rights",
    "related party transaction India abuse",
    "NCLT order India latest",
    "NCLAT judgment India latest",
    "tax tribunal India restructuring",
    "stamp duty merger India",
    "NRI property transfer India FEMA",
    "trust wealth planning India",
    "family trust India tax",
    "business valuation India dispute",
    "fair value dispute India company",
    "capital gains exemption India merger",
    "tax free merger India",
    "succession planning India wealthy family",
    "insolvency India promoter family",
    "IBC resolution plan India",
]

SEARCH_QUERIES_GOVERNANCE = [
    "boardroom battle India 2026",
    "promoter family feud listed company",
    "corporate governance failure India",
    "SEBI penalty corporate governance",
    "independent director removal India",
    "shareholder activism India",
    "proxy advisory India governance",
    "promoter reclassification SEBI",
    "EGM requisition India promoter",
    "board coup India company",
    "chairman removal India company",
    "family succession corporate India",
]

SEARCH_QUERIES_INDAS = [
    "Ind AS 103 business combination India",
    "common control transaction accounting India",
    "purchase price allocation India",
    "deferred tax business combination India",
    "ICAI EAC opinion scheme",
    "goodwill impairment India Ind AS",
    "opening balance sheet demerger",
    "accounting amalgamation India",
    "fair value measurement India Ind AS 113",
]


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
        logger.error("Google search error: %s", str(e)[:100])
    return results


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
            })
    except Exception as e:
        logger.error("RSS error for %s: %s", source_name, str(e)[:100])
    return results


def scrape_links(url, source_name, min_title_len=15, href_filter=None):
    results = []
    try:
        resp = safe_get(url)
        if resp:
            matches = re.findall(r'<a[^>]+href="([^"]*)"[^>]*>(.*?)</a>', resp.text, re.DOTALL)
            for href, title_raw in matches:
                title = strip_html(title_raw).strip()
                if len(title) < min_title_len:
                    continue
                if href_filter and not href_filter(href):
                    continue
                full_url = href if href.startswith("http") else url.rsplit("/", 1)[0] + "/" + href.lstrip("/")
                results.append({"title": title[:200], "url": full_url, "snippet": "", "source": source_name})
    except Exception as e:
        logger.error("Scrape error for %s: %s", source_name, str(e)[:100])
    return results


def fetch_full_text(url):
    try:
        resp = safe_get(url)
        if resp:
            return strip_html(resp.text)[:4000]
    except Exception as e:
        logger.error("Full text error: %s", str(e)[:100])
    return ""


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
    ("https://www.nishithdesai.com/feed", "Nishith Desai"),
    ("https://erfrequently.com/feed/", "ER Frequently (S&R)"),
    ("https://www.livemint.com/rss/companies", "Livemint Companies"),
    ("https://www.livemint.com/rss/money", "Livemint Money"),
    ("https://economictimes.indiatimes.com/rssfeedstopstories.cms", "Economic Times"),
    ("https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", "ET Markets"),
    ("https://www.business-standard.com/rss/companies-122.rss", "BS Companies"),
    ("https://www.business-standard.com/rss/markets-102.rss", "BS Markets"),
    ("https://www.moneycontrol.com/rss/business.xml", "Moneycontrol"),
    ("https://www.financialexpress.com/feed/", "Financial Express"),
    ("https://www.thehindubusinessline.com/companies/feeder/default.rss", "Hindu BusinessLine"),
    ("https://www.fortuneindia.com/rss/enterprise", "Fortune India"),
    ("https://www.icai.org/rss/rss_feed.html", "ICAI"),
]


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
        logger.error("Indian Kanoon error: %s", str(e)[:100])
    return results


def fetch_nclt_all_benches():
    results = []
    benches = [
        ("https://nclt.gov.in/order-judgment-by-bench/principal-bench-new-delhi", "NCLT Delhi"),
        ("https://nclt.gov.in/order-judgment-by-bench/mumbai-bench", "NCLT Mumbai"),
        ("https://nclt.gov.in/order-judgment-by-bench/ahmedabad-bench", "NCLT Ahmedabad"),
        ("https://nclt.gov.in/order-judgment-by-bench/bengaluru-bench", "NCLT Bengaluru"),
        ("https://nclt.gov.in/order-judgment-by-bench/chennai-bench", "NCLT Chennai"),
        ("https://nclt.gov.in/order-judgment-by-bench/kolkata-bench", "NCLT Kolkata"),
        ("https://nclt.gov.in/order-judgment-by-bench/hyderabad-bench", "NCLT Hyderabad"),
        ("https://nclt.gov.in/order-judgment-by-bench/chandigarh-bench", "NCLT Chandigarh"),
        ("https://nclt.gov.in/order-judgment-by-bench/jaipur-bench", "NCLT Jaipur"),
        ("https://nclt.gov.in/order-judgment-by-bench/guwahati-bench", "NCLT Guwahati"),
        ("https://nclt.gov.in/order-judgment-by-bench/cuttack-bench", "NCLT Cuttack"),
        ("https://nclt.gov.in/order-judgment-by-bench/kochi-bench", "NCLT Kochi"),
        ("https://nclt.gov.in/order-judgment-by-bench/indore-bench", "NCLT Indore"),
        ("https://nclt.gov.in/order-judgment-by-bench/amaravati-bench", "NCLT Amaravati"),
    ]
    for bench_url, bench_name in benches:
        logger.info("  %s...", bench_name)
        bench_results = scrape_links(bench_url, bench_name, href_filter=lambda h: "order" in h.lower() or "judgment" in h.lower() or ".pdf" in h.lower())
        results.extend(bench_results)
        time.sleep(1)
    logger.info("NCLT total: %d", len(results))
    return results


def fetch_nclat():
    r = scrape_links("https://nclat.nic.in/?page_id=585", "NCLAT", href_filter=lambda h: ".pdf" in h.lower() or "order" in h.lower())
    logger.info("NCLAT: %d", len(r))
    return r

def fetch_supreme_court():
    r = scrape_links("https://main.sci.gov.in/judgments", "Supreme Court", href_filter=lambda h: "judgment" in h.lower() or ".pdf" in h.lower() or "jonew" in h.lower())
    logger.info("SC: %d", len(r))
    return r

def fetch_high_courts():
    results = []
    hc_queries = [
        "site:bombayhighcourt.nic.in scheme arrangement",
        "site:bombayhighcourt.nic.in family settlement stamp duty",
        "site:bombayhighcourt.nic.in company petition",
        "site:delhihighcourt.nic.in scheme arrangement",
        "site:delhihighcourt.nic.in family arrangement",
        "site:delhihighcourt.nic.in company law",
        "site:ghconline.gov.in scheme arrangement",
        "site:ghconline.gov.in family settlement",
        "site:karnatakajudiciary.kar.nic.in scheme arrangement",
        "site:highcourtofkerala.nic.in scheme arrangement",
        "site:allahabadhighcourt.in family arrangement",
        "site:mhc.tn.gov.in scheme arrangement",
        "site:mhc.tn.gov.in company petition",
        "High Court scheme arrangement order India 2026",
        "High Court family settlement judgment India 2026",
        "High Court stamp duty scheme 2026",
        "High Court company petition restructuring 2026",
        "High Court writ petition NCLT 2026",
        "High Court capital gains exemption merger 2026",
        "High Court corporate dispute India 2026",
    ]
    for q in hc_queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "High Court (via Google)"
            results.append(r)
        time.sleep(2)
    logger.info("High Courts: %d", len(results))
    return results

def fetch_sebi():
    results = []
    for sebi_url, name in [
        ("https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0", "SEBI Orders"),
        ("https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=2&ssid=9&smid=0", "SEBI Circulars"),
    ]:
        results.extend(scrape_links(sebi_url, name, min_title_len=15))
        time.sleep(1)
    logger.info("SEBI: %d", len(results))
    return results

def fetch_sat():
    r = scrape_links("https://sat.gov.in/english/orders.htm", "SAT", href_filter=lambda h: ".pdf" in h.lower() or "order" in h.lower())
    logger.info("SAT: %d", len(r))
    return r

def fetch_itat():
    results = []
    for q in ["site:itatonline.org demerger", "site:itatonline.org slump sale", "site:itatonline.org family partition",
              "site:itatonline.org trust taxation", "site:itatonline.org capital gains exemption",
              "site:itatonline.org deemed gift", "site:itatonline.org reconstitution", "site:itatonline.org dissolution"]:
        for r in fetch_google_search(q, 3):
            r["source"] = "ITAT (via Google)"
            results.append(r)
        time.sleep(1)
    logger.info("ITAT: %d", len(results))
    return results

def fetch_cci():
    results = []
    for q in ["CCI combination approval order India 2026", "Competition Commission merger India 2026", "site:cci.gov.in combination order"]:
        for r in fetch_google_search(q, 5):
            r["source"] = "CCI"
            results.append(r)
        time.sleep(1.5)
    logger.info("CCI: %d", len(results))
    return results

def fetch_mca_ibbi():
    results = []
    for q in ["MCA notification Companies Act 2026", "IBBI circular valuation 2026", "site:mca.gov.in notification 2026", "site:ibbi.gov.in circular 2026"]:
        for r in fetch_google_search(q, 3):
            r["source"] = "MCA/IBBI"
            results.append(r)
        time.sleep(1.5)
    logger.info("MCA/IBBI: %d", len(results))
    return results

def fetch_exchange():
    results = []
    for q in ["BSE scheme demerger announcement 2026", "NSE amalgamation announcement 2026", "BSE open offer announcement 2026"]:
        for r in fetch_google_search(q, 3):
            r["source"] = "Stock Exchange"
            results.append(r)
        time.sleep(1.5)
    logger.info("Exchange: %d", len(results))
    return results

def fetch_newspaper_governance():
    results = []
    for site, queries in [
        ("site:livemint.com", ["boardroom battle 2026", "promoter family dispute 2026", "demerger scheme 2026", "corporate governance 2026", "company restructuring 2026"]),
        ("site:economictimes.indiatimes.com", ["promoter feud 2026", "boardroom coup 2026", "family business split 2026", "M&A deal India 2026", "corporate restructuring 2026"]),
        ("site:business-standard.com", ["family business dispute 2026", "promoter restructuring 2026", "SEBI governance 2026", "merger acquisition India 2026"]),
        ("site:moneycontrol.com", ["boardroom battle 2026", "promoter dispute 2026", "merger demerger India 2026"]),
    ]:
        for q in queries:
            for r in fetch_google_search(site + " " + q, 3):
                r["source"] = "Business News"
                results.append(r)
            time.sleep(2)
    logger.info("Newspaper: %d", len(results))
    return results

def fetch_law_firm_blogs():
    results = []
    for firm in ["site:azbpartners.com", "site:khaitanco.com", "site:trilegal.com", "site:nishithdesai.com", "site:cyrilamarchandblogs.com"]:
        for topic in ["scheme arrangement", "family settlement", "SEBI takeover", "M&A India", "corporate restructuring", "tax restructuring"]:
            for r in fetch_google_search(firm + " " + topic, 2):
                r["source"] = "Law Firm Blog"
                results.append(r)
            time.sleep(1.5)
    logger.info("Law firms: %d", len(results))
    return results

def fetch_proxy_advisory():
    results = []
    for q in ["IiAS proxy advisory India 2026", "InGovern governance India 2026", "proxy advisory related party India"]:
        for r in fetch_google_search(q, 3):
            r["source"] = "Proxy Advisory"
            results.append(r)
        time.sleep(1.5)
    logger.info("Proxy: %d", len(results))
    return results


def classify_judgment(text, title=""):
    if not ANTHROPIC_API_KEY:
        logger.error("No ANTHROPIC_API_KEY set.")
        return None
    combined_text = ("TITLE: " + title + "\n\nTEXT:\n" + text) if title else text
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01"},
            json={"model": CLAUDE_MODEL, "max_tokens": 512, "messages": [{"role": "user", "content": CLASSIFICATION_PROMPT + combined_text}]},
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
    return hashlib.sha256((title + "|" + url).encode()).hexdigest()[:16]

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
        (judgment_id, raw.get("title", "Unknown"), classification.get("court_or_tribunal", ""),
         classification.get("date_decided", ""), datetime.utcnow().isoformat(),
         raw.get("source", ""), raw.get("url", ""), raw.get("snippet", "")[:500],
         json.dumps(classification.get("categories", [])), json.dumps(classification.get("sections_engaged", [])),
         classification.get("relevance_score", ""), classification.get("summary", classification.get("practitioner_note", "")),
         json.dumps(classification)))
    conn.commit()
    conn.close()

def log_sweep(source, fetched, relevant, errors=""):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("INSERT INTO sweep_log (sweep_time, source, results_fetched, results_relevant, errors) VALUES (?, ?, ?, ?, ?)",
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
    logger.info("=== STARTING v5-UPDATED SWEEP at %s ===", datetime.utcnow().isoformat())
    total_relevant = 0

    logger.info("--- Phase 1: RSS Feeds ---")
    rss_results = []
    for feed_url, source_name in RSS_FEEDS:
        logger.info("Fetching %s...", source_name)
        rss_results.extend(fetch_rss_feed(feed_url, source_name))
        time.sleep(0.5)
    total_relevant += process_results(rss_results, "RSS Feeds")

    logger.info("--- Phase 2: Indian Kanoon (technical) ---")
    ik_results = []
    for query in SEARCH_QUERIES_TECHNICAL:
        ik_results.extend(fetch_indian_kanoon(query))
        time.sleep(1.5)
    total_relevant += process_results(ik_results, "Indian Kanoon Technical")

    logger.info("--- Phase 3: Courts ---")
    logger.info("NCLT (14 benches)...")
    total_relevant += process_results(fetch_nclt_all_benches(), "NCLT")
    logger.info("NCLAT...")
    total_relevant += process_results(fetch_nclat(), "NCLAT")
    logger.info("Supreme Court...")
    total_relevant += process_results(fetch_supreme_court(), "Supreme Court")
    logger.info("High Courts...")
    total_relevant += process_results(fetch_high_courts(), "High Courts")
    logger.info("SEBI...")
    total_relevant += process_results(fetch_sebi(), "SEBI")
    logger.info("SAT...")
    total_relevant += process_results(fetch_sat(), "SAT")
    logger.info("ITAT...")
    total_relevant += process_results(fetch_itat(), "ITAT")
    logger.info("CCI...")
    total_relevant += process_results(fetch_cci(), "CCI")

    logger.info("--- Phase 4: Regulatory ---")
    total_relevant += process_results(fetch_mca_ibbi(), "MCA/IBBI")
    total_relevant += process_results(fetch_exchange(), "Exchanges")

    logger.info("--- Phase 5: Plain English Google ---")
    pe_results = []
    for q in SEARCH_QUERIES_PLAIN_ENGLISH:
        pe_results.extend(fetch_google_search(q, 3))
        time.sleep(2)
    total_relevant += process_results(pe_results, "Google Plain English")

    logger.info("--- Phase 6: Ind AS ---")
    indas_results = []
    for q in SEARCH_QUERIES_INDAS:
        indas_results.extend(fetch_google_search(q, 3))
        time.sleep(1.5)
    total_relevant += process_results(indas_results, "Ind AS")

    logger.info("--- Phase 7: Governance ---")
    gov_results = []
    for q in SEARCH_QUERIES_GOVERNANCE:
        gov_results.extend(fetch_google_search(q, 3))
        time.sleep(2)
    total_relevant += process_results(gov_results, "Governance")
    total_relevant += process_results(fetch_newspaper_governance(), "Newspaper Governance")
    total_relevant += process_results(fetch_proxy_advisory(), "Proxy Advisory")

    logger.info("--- Phase 8: Law Firm Blogs ---")
    total_relevant += process_results(fetch_law_firm_blogs(), "Law Firm Blogs")

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
    days = int(request.args.get("days", "365"))
    search = request.args.get("search", "")
    page = int(request.args.get("page", "1"))
    per_page = 20
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    wc = []
    params = []
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    wc.append("date_fetched >= ?")
    params.append(cutoff)
    if category:
        wc.append("categories LIKE ?")
        params.append("%" + category + "%")
    if score:
        wc.append("relevance_score = ?")
        params.append(score)
    if search:
        wc.append("(title LIKE ? OR practitioner_note LIKE ? OR sections LIKE ?)")
        params.extend(["%" + search + "%"] * 3)
    where_sql = " AND ".join(wc) if wc else "1=1"
    offset = (page - 1) * per_page
    total = conn.execute("SELECT COUNT(*) as cnt FROM judgments WHERE " + where_sql, params).fetchone()["cnt"]
    rows = conn.execute("SELECT * FROM judgments WHERE " + where_sql + " ORDER BY date_fetched DESC LIMIT ? OFFSET ?", params + [per_page, offset]).fetchall()
    conn.close()
    judgments = []
    for row in rows:
        judgments.append({
            "id": row["id"], "title": row["title"], "court": row["court"],
            "date_decided": row["date_decided"], "date_fetched": row["date_fetched"],
            "source": row["source"], "source_url": row["source_url"],
            "categories": json.loads(row["categories"]) if row["categories"] else [],
            "sections": json.loads(row["sections"]) if row["sections"] else [],
            "relevance_score": row["relevance_score"], "practitioner_note": row["practitioner_note"],
        })
    return jsonify({"judgments": judgments, "total": total, "page": page, "per_page": per_page, "total_pages": (total + per_page - 1) // per_page})

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
    return jsonify({"total_judgments": total, "high_relevance": high, "last_sweep": last_sweep_time, "category_distribution": cat_counts})

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
logger.info("v5-updated Scheduler started. Sweeps every %d hours.", SWEEP_INTERVAL_HOURS)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
