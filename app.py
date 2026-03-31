"""
Katalyst Judgment Radar v5
Comprehensive legal, regulatory, and transaction intelligence system.
10 taxonomy categories. 50+ sources. 8 sweep phases.
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


# ---------------------------------------------------------------------------
# 10-Category Classification Prompt
# ---------------------------------------------------------------------------

CLASSIFICATION_PROMPT = """You are a legal classification engine for an M&A and transaction structuring advisory firm in India (Katalyst Advisors). Classify the given text against this taxonomy.

CATEGORIES:

1. SCHEMES OF ARRANGEMENT (Companies Act 2013, Sections 230-232, 66)
   amalgamations, demergers, composite schemes, capital reductions, NCLT sanctions, appointed dates, class meetings, valuation in schemes

2. FAMILY ARRANGEMENT AND SETTLEMENT
   family settlement deeds, partition disputes, Hindu Succession Act, Indian Succession Act, family arrangements via corporate structures

3. INCOME TAX (Succession and Restructuring)
   Section 2(19AA) demerger, Section 47 exemptions, Section 56(2)(x) deemed gifts, Section 45(4)/9B firm reconstitution, Section 50D FMV, Section 171 HUF partition, Section 50B slump sales, capital gains on family transfers

4. SEBI AND SECURITIES LAW
   Regulation 10 inter se transfers, open offer exemptions, Regulation 23 RPTs, Regulation 37 schemes, delisting, SAT orders, change of control

5. STAMP DUTY AND REGISTRATION
   family arrangements as conveyances, stamp duty on schemes, state exemptions, registration requirements

6. FEMA AND CROSS-BORDER
   FEMA pricing guidelines for family transfers with NRIs, RBI compounding orders, LRS and inheritance

7. TRUST LAW AND SUCCESSION VEHICLES
   private trusts, Sections 60-64 revocable transfers, Sections 161-164 trust taxation, trust distributions

8. INSOLVENCY INTERSECTION
   Section 29A related party, oppression/mismanagement Sections 241-244 in family disputes, IBC in family companies

9. BOARDROOM BATTLES AND CORPORATE GOVERNANCE
   promoter feuds, boardroom coups, director removals, EGM requisitions, shareholder activism, SEBI governance enforcement, minority oppression, proxy fights, promoter reclassification, pledge enforcement

10. TRANSACTION ACCOUNTING AND IND AS
    Ind AS 103 business combinations, common control transactions, Ind AS 110 consolidation, Ind AS 27 separate financial statements, Ind AS 12 deferred tax on restructurings, Ind AS 113 fair value measurement, purchase price allocation, ICAI EAC opinions on scheme accounting, opening balance sheet treatment, goodwill and bargain purchase in M&A

Respond with JSON only (no markdown, no backticks):
{
  "relevant": true/false,
  "relevance_score": "high" | "medium" | "low",
  "categories": [category numbers],
  "category_names": [category names],
  "sections_engaged": [specific sections/regulations/standards],
  "court_or_tribunal": "court/tribunal/regulator or 'News Report' or 'Advisory/Article'",
  "date_decided": "date or null",
  "parties": "party/company names or null",
  "structural_mechanism": "brief description of structure or issue",
  "practitioner_note": "2-3 sentences for a senior M&A structuring partner on WHY this matters. For news, flag the structural vulnerability and advisory opportunity. Write as a peer."
}

If NOT relevant: {"relevant": false, "relevance_score": "none", "reason": "brief reason"}

TEXT TO CLASSIFY:
"""


# ---------------------------------------------------------------------------
# Search Queries
# ---------------------------------------------------------------------------

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

SEARCH_QUERIES_GOVERNANCE = [
    "boardroom battle India promoter family",
    "promoter family feud listed company India",
    "corporate governance failure India SEBI",
    "independent director removal India",
    "oppression mismanagement petition NCLT 2026",
    "shareholder activism India proxy fight",
    "promoter group infighting listed company",
    "family succession dispute corporate India",
    "SEBI corporate governance violation order",
    "minority shareholder oppression India judgment",
    "related party transaction abuse SEBI penalty",
    "promoter reclassification SEBI",
    "family split demerger listed company India",
    "succession battle Indian business family",
    "group company restructuring family dispute India",
    "EGM requisition promoter fight India",
    "NCLT oppression petition family business 2026",
]

SEARCH_QUERIES_INDAS = [
    "Ind AS 103 business combination India",
    "common control transaction accounting India",
    "Ind AS 103 amalgamation pooling interest",
    "purchase price allocation India Ind AS",
    "Ind AS 110 consolidation restructuring",
    "deferred tax business combination Ind AS 12",
    "ICAI EAC opinion scheme arrangement accounting",
    "Ind AS 113 fair value measurement transaction",
    "goodwill impairment business combination India",
    "opening balance sheet demerger accounting",
    "Ind AS 27 separate financial statements restructuring",
    "bargain purchase negative goodwill Ind AS",
    "contingent consideration Ind AS 103",
    "ICAI guidance note amalgamation accounting",
]

SEARCH_QUERIES_REGULATORY = [
    "CCI combination approval order 2026",
    "Competition Commission merger control India",
    "MCA notification Companies Act amendment 2026",
    "IBBI circular valuation IBC",
    "IBBI registered valuer regulation",
    "SEBI consultation paper 2026",
    "SEBI working group report",
    "BSE scheme arrangement announcement",
    "NSE corporate announcement demerger",
    "stock exchange open offer announcement India",
]


# ---------------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------------

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
    """Generic link scraper for court/tribunal websites."""
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


# ---------------------------------------------------------------------------
# Source Fetchers
# ---------------------------------------------------------------------------

# RSS Feeds (legal + newspapers + accounting)
RSS_FEEDS = [
    # Legal
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
    # Law firm blogs
    ("https://www.nishithdesai.com/feed", "Nishith Desai"),
    ("https://erfrequently.com/feed/", "ER Frequently (S&R)"),
    # Newspapers
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
    # Accounting / Ind AS
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
        logger.info("  Fetching %s...", bench_name)
        bench_results = scrape_links(bench_url, bench_name, href_filter=lambda h: "order" in h.lower() or "judgment" in h.lower() or ".pdf" in h.lower())
        results.extend(bench_results)
        time.sleep(1)
    logger.info("NCLT total: %d results from %d benches", len(results), len(benches))
    return results


def fetch_nclat():
    results = scrape_links("https://nclat.nic.in/?page_id=585", "NCLAT",
                           href_filter=lambda h: ".pdf" in h.lower() or "order" in h.lower())
    logger.info("NCLAT: %d results", len(results))
    return results


def fetch_supreme_court():
    results = scrape_links("https://main.sci.gov.in/judgments", "Supreme Court",
                           href_filter=lambda h: "judgment" in h.lower() or ".pdf" in h.lower() or "jonew" in h.lower())
    logger.info("Supreme Court: %d results", len(results))
    return results


def fetch_high_courts():
    """Fetch from all major High Court websites via Google search."""
    results = []
    hc_queries = [
        "site:bombayhighcourt.nic.in scheme arrangement demerger",
        "site:bombayhighcourt.nic.in family settlement stamp duty",
        "site:delhihighcourt.nic.in scheme arrangement",
        "site:delhihighcourt.nic.in family arrangement partition",
        "site:ghconline.gov.in scheme arrangement stamp duty",
        "site:ghconline.gov.in family settlement",
        "site:karnatakajudiciary.kar.nic.in scheme arrangement",
        "site:karnatakajudiciary.kar.nic.in family partition",
        "site:highcourtofkerala.nic.in scheme arrangement",
        "site:allahabadhighcourt.in family arrangement",
        "site:phc.gov.in scheme arrangement",
        "site:mhc.tn.gov.in scheme arrangement demerger",
        "site:highcourtchd.gov.in scheme arrangement",
        "site:jharkhandhighcourt.nic.in family partition",
        "site:cghighcourt.nic.in scheme arrangement",
        # General High Court searches
        "High Court scheme arrangement demerger order 2026 India",
        "High Court family settlement partition judgment 2026",
        "High Court stamp duty scheme arrangement 2026",
        "High Court section 47 capital gains exemption amalgamation",
        "High Court writ NCLT scheme arrangement",
    ]
    for q in hc_queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "High Court (via Google)"
            results.append(r)
        time.sleep(2)
    logger.info("High Courts: %d results via Google", len(results))
    return results


def fetch_sebi():
    results = []
    urls = [
        ("https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=2&smid=0", "SEBI Orders"),
        ("https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=2&ssid=9&smid=0", "SEBI Circulars"),
    ]
    for sebi_url, source_name in urls:
        r = scrape_links(sebi_url, source_name, min_title_len=15)
        results.extend(r)
        time.sleep(1)
    logger.info("SEBI: %d results", len(results))
    return results


def fetch_sat():
    results = scrape_links("https://sat.gov.in/english/orders.htm", "SAT",
                           href_filter=lambda h: ".pdf" in h.lower() or "order" in h.lower())
    logger.info("SAT: %d results", len(results))
    return results


def fetch_itat():
    results = []
    queries = [
        "site:itatonline.org demerger section 2(19AA)",
        "site:itatonline.org slump sale section 50B",
        "site:itatonline.org family partition HUF",
        "site:itatonline.org trust taxation section 161",
        "site:itatonline.org capital gains section 47",
        "site:itatonline.org section 56(2)(x) gift",
        "site:itatonline.org section 45(4) reconstitution",
        "site:itatonline.org section 9B dissolution",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "ITAT (via Google)"
            results.append(r)
        time.sleep(1)
    logger.info("ITAT: %d results", len(results))
    return results


def fetch_cci():
    """Fetch CCI combination/merger orders."""
    results = []
    queries = [
        "site:cci.gov.in combination order 2026",
        "site:cci.gov.in merger approval order",
        "Competition Commission India combination approval 2026",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 5)
        for r in g_results:
            r["source"] = "CCI"
            results.append(r)
        time.sleep(1.5)
    logger.info("CCI: %d results", len(results))
    return results


def fetch_mca_ibbi():
    """Fetch MCA notifications and IBBI circulars."""
    results = []
    queries = [
        "site:mca.gov.in notification Companies Act 2026",
        "site:mca.gov.in circular companies 2026",
        "site:ibbi.gov.in circular regulation 2026",
        "site:ibbi.gov.in valuation standard",
        "MCA notification companies act amendment 2026",
        "IBBI circular insolvency valuation 2026",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "MCA/IBBI"
            results.append(r)
        time.sleep(1.5)
    logger.info("MCA/IBBI: %d results", len(results))
    return results


def fetch_exchange_announcements():
    """Fetch BSE/NSE corporate announcements on schemes and restructurings."""
    results = []
    queries = [
        "site:bseindia.com scheme arrangement announcement",
        "site:bseindia.com demerger announcement",
        "site:nseindia.com scheme arrangement corporate announcement",
        "site:nseindia.com open offer announcement",
        "BSE corporate announcement scheme demerger 2026",
        "NSE corporate announcement amalgamation 2026",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "Stock Exchange"
            results.append(r)
        time.sleep(1.5)
    logger.info("Exchange announcements: %d results", len(results))
    return results


def fetch_indas_accounting():
    """Fetch Ind AS and accounting related content."""
    results = []
    queries = SEARCH_QUERIES_INDAS
    for q in queries:
        g_results = fetch_google_search(q, 3)
        results.extend(g_results)
        time.sleep(1.5)
    # Also search accounting firm publications
    firm_queries = [
        "site:deloitte.com/in Ind AS business combination",
        "site:pwc.in Ind AS accounting update",
        "site:ey.com/en_in Ind AS transaction accounting",
        "site:kpmg.com/in Ind AS first notes",
        "ICAI EAC opinion scheme arrangement accounting",
        "ICAI guidance note amalgamation accounting Ind AS",
    ]
    for q in firm_queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "Accounting Advisory"
            results.append(r)
        time.sleep(1.5)
    logger.info("Ind AS/Accounting: %d results", len(results))
    return results


def fetch_proxy_advisory():
    """Fetch proxy advisory and governance reports."""
    results = []
    queries = [
        "site:iiasadvisory.com governance report",
        "site:sesgovernance.com corporate governance",
        "site:ingovern.com governance advisory",
        "IiAS proxy advisory related party transaction India",
        "proxy advisory firm India corporate governance report 2026",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 3)
        for r in g_results:
            r["source"] = "Proxy Advisory"
            results.append(r)
        time.sleep(1.5)
    logger.info("Proxy advisory: %d results", len(results))
    return results


def fetch_newspaper_governance():
    results = []
    site_queries = [
        ("site:livemint.com", [
            "boardroom battle promoter family 2026",
            "demerger scheme arrangement 2026",
            "family business succession dispute 2026",
            "SEBI corporate governance penalty 2026",
            "promoter shareholding restructuring 2026",
        ]),
        ("site:economictimes.indiatimes.com", [
            "promoter family feud company 2026",
            "boardroom coup India 2026",
            "family business split demerger 2026",
            "corporate governance violation India 2026",
        ]),
        ("site:business-standard.com", [
            "family business dispute India 2026",
            "promoter group restructuring 2026",
            "independent director removal 2026",
            "SEBI enforcement corporate governance 2026",
        ]),
        ("site:moneycontrol.com", [
            "boardroom battle India 2026",
            "promoter family dispute listed company 2026",
            "succession planning Indian promoter 2026",
        ]),
    ]
    for site_prefix, queries in site_queries:
        for q in queries:
            full_query = site_prefix + " " + q
            g_results = fetch_google_search(full_query, 3)
            for r in g_results:
                r["source"] = "Business News (Google)"
                results.append(r)
            time.sleep(2)
    logger.info("Newspaper governance: %d results", len(results))
    return results


def fetch_law_firm_blogs():
    """Fetch from major law firm blogs via Google."""
    results = []
    firms = [
        "site:azbpartners.com",
        "site:khaitanco.com",
        "site:trilegal.com",
        "site:nishithdesai.com",
        "site:cyrilamarchandblogs.com",
        "site:singhassociates.in",
    ]
    topics = ["scheme arrangement demerger", "family settlement", "SEBI takeover", "M&A India 2026"]
    for firm in firms:
        for topic in topics:
            g_results = fetch_google_search(firm + " " + topic, 2)
            for r in g_results:
                r["source"] = "Law Firm Blog"
                results.append(r)
            time.sleep(1.5)
    logger.info("Law firm blogs: %d results", len(results))
    return results


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Core Sweep Logic
# ---------------------------------------------------------------------------

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
    logger.info("=== STARTING v5 COMPREHENSIVE SWEEP at %s ===", datetime.utcnow().isoformat())
    total_relevant = 0

    logger.info("--- Phase 1: RSS Feeds (Legal + News + Accounting) ---")
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

    logger.info("--- Phase 3: Courts and Tribunals ---")
    logger.info("Fetching all NCLT benches (14)...")
    total_relevant += process_results(fetch_nclt_all_benches(), "NCLT")
    logger.info("Fetching NCLAT...")
    total_relevant += process_results(fetch_nclat(), "NCLAT")
    logger.info("Fetching Supreme Court...")
    total_relevant += process_results(fetch_supreme_court(), "Supreme Court")
    logger.info("Fetching all High Courts...")
    total_relevant += process_results(fetch_high_courts(), "High Courts")
    logger.info("Fetching SEBI...")
    total_relevant += process_results(fetch_sebi(), "SEBI")
    logger.info("Fetching SAT...")
    total_relevant += process_results(fetch_sat(), "SAT")
    logger.info("Fetching ITAT...")
    total_relevant += process_results(fetch_itat(), "ITAT")
    logger.info("Fetching CCI...")
    total_relevant += process_results(fetch_cci(), "CCI")

    logger.info("--- Phase 4: Regulatory (MCA, IBBI, Exchanges) ---")
    total_relevant += process_results(fetch_mca_ibbi(), "MCA/IBBI")
    total_relevant += process_results(fetch_exchange_announcements(), "Exchanges")

    logger.info("--- Phase 5: Ind AS and Transaction Accounting ---")
    total_relevant += process_results(fetch_indas_accounting(), "Ind AS")

    logger.info("--- Phase 6: Google Legal News + Broad Search ---")
    google_results = []
    queries = SEARCH_QUERIES_BROAD + [
        "NCLT scheme arrangement order 2026",
        "NCLT demerger order 2026",
        "family arrangement judgment India 2026",
        "ITAT demerger slump sale 2026",
        "SEBI open offer exemption order 2026",
    ]
    for q in queries:
        g_results = fetch_google_search(q, 3)
        google_results.extend(g_results)
        time.sleep(2)
    total_relevant += process_results(google_results, "Google Legal")

    logger.info("--- Phase 7: Governance and Boardroom ---")
    gov_results = []
    for query in SEARCH_QUERIES_GOVERNANCE:
        results = fetch_google_search(query, 3)
        gov_results.extend(results)
        time.sleep(2)
    total_relevant += process_results(gov_results, "Governance")
    total_relevant += process_results(fetch_newspaper_governance(), "Newspaper Governance")
    total_relevant += process_results(fetch_proxy_advisory(), "Proxy Advisory")

    logger.info("--- Phase 8: Law Firm Blogs ---")
    total_relevant += process_results(fetch_law_firm_blogs(), "Law Firm Blogs")

    logger.info("=== v5 SWEEP COMPLETE. Total relevant: %d ===", total_relevant)


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

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
logger.info("v5 Scheduler started. Sweeps every %d hours.", SWEEP_INTERVAL_HOURS)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
