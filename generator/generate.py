import csv
import os
import re
import datetime as dt
from urllib.parse import urlparse
import requests
import secrets
import string

TIMEOUT = float(os.environ.get("TIMEOUT", "12"))
DATA_FILE = os.environ.get("DATA_FILE", "data/daily.csv")
DOCS_DIR = os.environ.get("DOCS_DIR", "docs")
BASE_URL = os.environ.get("BASE_URL", "https://USERNAME.github.io/REPO").rstrip("/")
MAX_PER_PAGE = int(os.environ.get("MAX_PER_PAGE", "10"))
DAYS_ON_INDEX = int(os.environ.get("DAYS_ON_INDEX", "30"))

# Optional: newline-separated URLs injected at runtime (workflow_dispatch)
EXTRA_URLS = os.environ.get("EXTRA_URLS", "").strip()

UA = {"User-Agent": "Mozilla/5.0 (compatible; DiscoveryHub/1.0)"}

URL_RE = re.compile(r"^https?://", re.I)

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def read_daily_csv(path: str):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        r = csv.reader(f)
        for line in r:
            if not line or len(line) < 2:
                continue
            date_s = (line[0] or "").strip()
            url = (line[1] or "").strip()
            if not date_s or not url:
                continue
            if not URL_RE.match(url):
                continue
            rows.append((date_s, url))
    return rows

def parse_extra_urls(extra: str):
    urls = []
    if not extra:
        return urls
    for line in extra.splitlines():
        u = line.strip()
        if not u:
            continue
        if not URL_RE.match(u):
            continue
        urls.append(u)
    return urls

def head_check(url: str):
    """
    Quick HTTP HEAD check:
    - status code
    - final URL after redirects
    - X-Robots-Tag header (noindex hints)
    """
    try:
        r = requests.head(url, allow_redirects=True, timeout=TIMEOUT, headers=UA)
        status = r.status_code
        final_url = r.url
        xrobots = r.headers.get("X-Robots-Tag", "")
        noindex = "noindex" in (xrobots or "").lower()
        return status, final_url, xrobots, noindex
    except Exception as e:
        return "", "", f"error:{type(e).__name__}", False

def chunk(items, n):
    for i in range(0, len(items), n):
        yield items[i:i+n]

def build_index_page(dates_sorted, latest_date):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    latest_link = f'<p><a href="{BASE_URL}/d/{latest_date}.html">Open latest: {latest_date}</a></p>' if latest_date else ""
    # recent days list
    recent = list(reversed(dates_sorted[-DAYS_ON_INDEX:]))
    days_html = "\n".join([f'<li><a href="{BASE_URL}/d/{d}.html">{d}</a></li>' for d in recent]) if recent else "<li>No data yet</li>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Backlink Discovery Hub</title>
  <meta name="robots" content="index,follow">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body>
  <h1>Backlink Discovery Hub</h1>
  <p>Updated: {updated}</p>
  {latest_link}
  <p><a href="{BASE_URL}/all.html">All recent links</a> | <a href="{BASE_URL}/backlink-feed.xml">Atom feed</a> | <a href="{BASE_URL}/sitemap.xml">Sitemap</a></p>
  <h2>Recent days</h2>
  <ul>
    {days_html}
  </ul>
</body>
</html>"""

def build_daily_page(date_s, urls):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    items = []
    for u in urls:
        # Include a small context sentence to avoid "bare list" pattern
        items.append(f'<li><a href="{u}" target="_blank" rel="noopener">{u}</a><br><small>Reference link discovered on {date_s}.</small></li>')
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Daily Discovery {date_s}</title>
  <meta name="robots" content="index,follow">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body>
  <p><a href="{BASE_URL}/">Hub</a> | <a href="{BASE_URL}/all.html">All</a> | <a href="{BASE_URL}/backlink-feed.xml">Feed</a></p>
  <h1>Daily Discovery: {date_s}</h1>
  <p>Updated: {updated}</p>
  <ol>
    {"".join(items) if items else "<li>No links</li>"}
  </ol>
</body>
</html>"""

def build_all_page(unique_urls):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    items = "\n".join([f'<li><a href="{u}" target="_blank" rel="noopener">{u}</a></li>' for u in unique_urls[:500]])
    plain = "\n".join(unique_urls[:500])
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>All Recent Links</title>
  <meta name="robots" content="index,follow">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body>
  <p><a href="{BASE_URL}/">Hub</a> | <a href="{BASE_URL}/backlink-feed.xml">Feed</a></p>
  <h1>All Recent Links</h1>
  <p>Updated: {updated}</p>
  <ul>
    {items if items else "<li>No links</li>"}
  </ul>
  <h2>Plain list</h2>
  <pre>{plain}</pre>
</body>
</html>"""

def build_atom_feed(latest_urls):
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    feed_id = f"{BASE_URL}/backlink-feed.xml"
    entries = []
    for u in latest_urls[:100]:
        pu = urlparse(u)
        eid = f"urn:link:{pu.netloc}{pu.path}"
        entries.append(f"""
  <entry>
    <title>{u}</title>
    <id>{eid}</id>
    <link href="{u}" />
    <updated>{now}</updated>
  </entry>""".rstrip())
    return f"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Backlink Discovery Feed</title>
  <id>{feed_id}</id>
  <updated>{now}</updated>
  <link rel="self" href="{feed_id}" />
{''.join(entries)}
</feed>
"""

def build_sitemap(dates_sorted):
    now = dt.datetime.utcnow().date().isoformat()
    urls = [f"{BASE_URL}/", f"{BASE_URL}/all.html", f"{BASE_URL}/backlink-feed.xml"] + [f"{BASE_URL}/d/{d}.html" for d in dates_sorted[-365:]]
    items = "\n".join([f"<url><loc>{u}</loc><lastmod>{now}</lastmod></url>" for u in urls])
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{items}
</urlset>
"""

def build_robots():
    return f"User-agent: *\nAllow: /\nSitemap: {BASE_URL}/sitemap.xml\n"


# =========================
# Broadcast features (added)
# =========================

def _find_existing_indexnow_key(docs_dir: str) -> str:
    """
    Look for an existing IndexNow key file in docs root:
    The file must be named <key>.txt and contain the same key on the first line.
    """
    try:
        for fn in os.listdir(docs_dir):
            if not fn.lower().endswith(".txt"):
                continue
            stem = fn[:-4]
            if len(stem) < 8:
                continue
            p = os.path.join(docs_dir, fn)
            try:
                with open(p, "r", encoding="utf-8") as f:
                    first = (f.readline() or "").strip()
                if first and first == stem:
                    return stem
            except Exception:
                continue
    except Exception:
        return ""
    return ""

def _generate_indexnow_key(length: int = 32) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))

def ensure_indexnow_key_file(docs_dir: str, base_url: str) -> tuple[str, str]:
    """
    Ensure the key file exists in docs root as <key>.txt.
    Returns (key, key_location_url).
    """
    key = os.environ.get("INDEXNOW_KEY", "").strip()
    if not key:
        key = _find_existing_indexnow_key(docs_dir)
    if not key:
        key = _generate_indexnow_key()
        key_file = os.path.join(docs_dir, f"{key}.txt")
        with open(key_file, "w", encoding="utf-8") as f:
            f.write(key)
    key_location = f"{base_url}/{key}.txt"
    return key, key_location

def broadcast_indexnow(url_to_submit: str, docs_dir: str, base_url: str) -> None:
    """
    Submit the given URL to IndexNow (Bing/Yandex partners).
    Generates an API key file if missing.
    """
    try:
        key, key_location = ensure_indexnow_key_file(docs_dir, base_url)
        host = urlparse(base_url).netloc
        payload = {"host": host, "key": key, "keyLocation": key_location, "urlList": [url_to_submit]}
        endpoint = os.environ.get("INDEXNOW_ENDPOINT", "https://api.indexnow.org/indexnow")
        r = requests.post(endpoint, json=payload, timeout=TIMEOUT, headers=UA)
        print(f"[IndexNow] {r.status_code} submit={url_to_submit}")
    except Exception as e:
        print(f"[IndexNow] error: {type(e).__name__}")

def broadcast_pingomatic(feed_url: str) -> None:
    """
    XML-RPC ping to Ping-O-Matic endpoint.
    """
    try:
        endpoint = os.environ.get("PINGOMATIC_ENDPOINT", "http://rpc.pingomatic.com/")
        xml = f"""<?xml version="1.0"?>
<methodCall>
  <methodName>weblogUpdates.ping</methodName>
  <params>
    <param><value><string>Daily Backlink Hub</string></value></param>
    <param><value><string>{feed_url}</string></value></param>
  </params>
</methodCall>
"""
        headers = {"Content-Type": "text/xml"}
        r = requests.post(endpoint, data=xml.encode("utf-8"), headers=headers, timeout=TIMEOUT)
        print(f"[Ping-O-Matic] {r.status_code} feed={feed_url}")
    except Exception as e:
        print(f"[Ping-O-Matic] error: {type(e).__name__}")


def main():
    rows = read_daily_csv(DATA_FILE)
    by_date = {}
    for date_s, url in rows:
        by_date.setdefault(date_s, []).append(url)

    today = dt.datetime.utcnow().date().isoformat()

    # One-off URLs (from workflow input) are added to today's page only
    extra = parse_extra_urls(EXTRA_URLS)
    if extra:
        by_date.setdefault(today, []).extend(extra)

    dates_sorted = sorted(by_date.keys())
    latest_date = dates_sorted[-1] if dates_sorted else ""

    # Keep only last DAYS_ON_INDEX in hub list, but pages remain available if in csv
    safe_mkdir(DOCS_DIR)
    safe_mkdir(os.path.join(DOCS_DIR, "d"))
    safe_mkdir(os.path.join(DOCS_DIR, "health"))

    # Build pages
    unique_recent = []
    # collect recent unique urls from last N days
    for d in reversed(dates_sorted[-DAYS_ON_INDEX:]):
        for u in by_date.get(d, []):
            if u not in unique_recent:
                unique_recent.append(u)

    # daily pages + health checks
    for d in dates_sorted[-365:]:
        urls = by_date.get(d, [])[:MAX_PER_PAGE]
        with open(os.path.join(DOCS_DIR, "d", f"{d}.html"), "w", encoding="utf-8") as f:
            f.write(build_daily_page(d, urls))

        health_path = os.path.join(DOCS_DIR, "health", f"{d}.csv")
        with open(health_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "input_url", "status", "final_url", "x_robots_tag", "noindex_hint"])
            for u in urls:
                status, final_url, xrobots, noindex = head_check(u)
                w.writerow([d, u, status, final_url, xrobots, "yes" if noindex else ""])

    # hub index
    with open(os.path.join(DOCS_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(build_index_page(dates_sorted, latest_date))

    # all page
    with open(os.path.join(DOCS_DIR, "all.html"), "w", encoding="utf-8") as f:
        f.write(build_all_page(unique_recent))

    # atom feed
    with open(os.path.join(DOCS_DIR, "backlink-feed.xml"), "w", encoding="utf-8") as f:
        f.write(build_atom_feed(unique_recent))

    # sitemap + robots
    with open(os.path.join(DOCS_DIR, "sitemap.xml"), "w", encoding="utf-8") as f:
        f.write(build_sitemap(dates_sorted))

    with open(os.path.join(DOCS_DIR, "robots.txt"), "w", encoding="utf-8") as f:
        f.write(build_robots())

    # GitHub Pages: disable Jekyll processing
    with open(os.path.join(DOCS_DIR, ".nojekyll"), "w", encoding="utf-8") as f:
        f.write("")

    # =========================
    # Broadcast features (end)
    # =========================
    daily_page_url = f"{BASE_URL}/d/{today}.html"
    broadcast_indexnow(daily_page_url, DOCS_DIR, BASE_URL)

    feed_url = f"{BASE_URL}/backlink-feed.xml"
    broadcast_pingomatic(feed_url)

if __name__ == "__main__":
    main()
