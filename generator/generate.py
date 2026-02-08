import csv
import os
import re
import json
import datetime as dt
from urllib.parse import urlparse
import requests
import secrets
import string

TIMEOUT = float(os.environ.get("TIMEOUT", "12"))
DATA_FILE = os.environ.get("DATA_FILE", "data/daily.csv")
META_FILE = os.environ.get("META_FILE", "data/meta.csv")
DOCS_DIR = os.environ.get("DOCS_DIR", "docs")

BASE_URL = os.environ.get("BASE_URL", "https://USERNAME.github.io/REPO").rstrip("/")
CANONICAL_BASE = os.environ.get("CANONICAL_BASE", BASE_URL).rstrip("/")

MAX_PER_PAGE = int(os.environ.get("MAX_PER_PAGE", "15"))
DAYS_ON_INDEX = int(os.environ.get("DAYS_ON_INDEX", "45"))

FETCH_META = (os.environ.get("FETCH_META", "1").strip() == "1")
META_TIMEOUT = float(os.environ.get("META_TIMEOUT", "12"))
META_MAX_BYTES = int(os.environ.get("META_MAX_BYTES", "180000"))

# Optional: newline-separated URLs injected at runtime (workflow_dispatch)
EXTRA_URLS = os.environ.get("EXTRA_URLS", "").strip()

UA = {"User-Agent": "Mozilla/5.0 (compatible; DiscoveryHub/2.0)"}
URL_RE = re.compile(r"^https?://", re.I)

# -------------------------
# Utils
# -------------------------

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def norm_tags(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"[,\s]+", s) if p.strip()]
    parts = parts[:8]
    return ",".join(parts)

def safe_text(s: str, limit: int) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    if len(s) > limit:
        return s[:limit - 1].rstrip() + "…"
    return s

def escape_html(s: str) -> str:
    s = (s or "")
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;"))

def canonical_for(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{CANONICAL_BASE}{path}"

def read_daily_csv(path: str):
    """
    Flexible CSV:
    date,url[,note][,tags][,title]
    """
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
            note = (line[2] or "").strip() if len(line) >= 3 else ""
            tags = (line[3] or "").strip() if len(line) >= 4 else ""
            title = (line[4] or "").strip() if len(line) >= 5 else ""
            if not date_s or not url:
                continue
            if not URL_RE.match(url):
                continue
            rows.append({
                "date": date_s,
                "url": url,
                "note": note,
                "tags": norm_tags(tags),
                "title_hint": title,
            })
    return rows

def parse_extra_urls(extra: str):
    """
    Each line:
    url
    url | note
    url | note | tag1,tag2
    """
    items = []
    if not extra:
        return items
    for line in extra.splitlines():
        raw = line.strip()
        if not raw:
            continue
        parts = [p.strip() for p in raw.split("|")]
        url = parts[0].strip()
        if not URL_RE.match(url):
            continue
        note = parts[1].strip() if len(parts) >= 2 else ""
        tags = norm_tags(parts[2].strip()) if len(parts) >= 3 else ""
        items.append({"url": url, "note": note, "tags": tags})
    return items

def head_check(url: str):
    """
    Quick HTTP HEAD check:
    - status code
    - final URL after redirects
    - Content-Type
    - X-Robots-Tag header (noindex hints)
    """
    try:
        r = requests.head(url, allow_redirects=True, timeout=TIMEOUT, headers=UA)
        status = r.status_code
        final_url = r.url
        ctype = r.headers.get("Content-Type", "")
        xrobots = r.headers.get("X-Robots-Tag", "")
        noindex = "noindex" in (xrobots or "").lower()
        return status, final_url, ctype, xrobots, noindex
    except Exception as e:
        return "", "", "", f"error:{type(e).__name__}", False

def fetch_meta(url: str):
    """
    GET a small chunk and extract:
    - <title>
    - meta name="description"
    - meta name="robots" noindex
    """
    try:
        r = requests.get(url, allow_redirects=True, timeout=META_TIMEOUT, headers=UA, stream=True)
        status = r.status_code
        final_url = r.url
        ctype = r.headers.get("Content-Type", "") or ""
        xrobots = r.headers.get("X-Robots-Tag", "") or ""

        # Only parse html-ish
        if "text/html" not in ctype.lower() and "application/xhtml" not in ctype.lower():
            return {
                "url": url,
                "final_url": final_url,
                "status_code": str(status),
                "content_type": ctype[:120],
                "title": "",
                "description": "",
                "robots_meta_noindex": "",
                "x_robots_tag": xrobots[:200],
            }

        buf = b""
        for chunk in r.iter_content(chunk_size=4096):
            if not chunk:
                break
            buf += chunk
            if len(buf) >= META_MAX_BYTES:
                break

        text = buf.decode("utf-8", errors="ignore")

        # title
        m = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.I | re.S)
        title = safe_text(re.sub(r"<[^>]+>", " ", m.group(1)) if m else "", 90)

        # description
        md = re.search(r'<meta[^>]+name=["\']description["\'][^>]*content=["\'](.*?)["\']', text, flags=re.I | re.S)
        if not md:
            md = re.search(r'<meta[^>]+content=["\'](.*?)["\'][^>]*name=["\']description["\']', text, flags=re.I | re.S)
        description = safe_text(re.sub(r"<[^>]+>", " ", md.group(1)) if md else "", 180)

        # robots meta
        mr = re.search(r'<meta[^>]+name=["\']robots["\'][^>]*content=["\'](.*?)["\']', text, flags=re.I | re.S)
        if not mr:
            mr = re.search(r'<meta[^>]+content=["\'](.*?)["\'][^>]*name=["\']robots["\']', text, flags=re.I | re.S)
        robots = (mr.group(1) if mr else "").strip().lower()
        robots_noindex = "yes" if ("noindex" in robots) else ""

        return {
            "url": url,
            "final_url": final_url,
            "status_code": str(status),
            "content_type": ctype[:120],
            "title": title,
            "description": description,
            "robots_meta_noindex": robots_noindex,
            "x_robots_tag": xrobots[:200],
        }
    except Exception as e:
        return {
            "url": url,
            "final_url": "",
            "status_code": "",
            "content_type": "",
            "title": "",
            "description": "",
            "robots_meta_noindex": "",
            "x_robots_tag": f"error:{type(e).__name__}",
        }

def read_meta_csv(path: str):
    meta = {}
    if not os.path.exists(path):
        return meta
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            u = (row.get("url") or "").strip()
            if not u:
                continue
            meta[u] = row
    return meta

def write_meta_csv(path: str, meta: dict):
    fields = ["url","final_url","status_code","content_type","title","description","robots_meta_noindex","x_robots_tag","last_fetch_utc"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for u, row in meta.items():
            out = {k: (row.get(k) or "") for k in fields}
            w.writerow(out)

# -------------------------
# HTML builders
# -------------------------

def common_head(title: str, description: str, path: str):
    t = escape_html(title)
    d = escape_html(description)
    canon = canonical_for(path)
    return f"""<head>
  <meta charset="utf-8">
  <title>{t}</title>
  <meta name="description" content="{d}">
  <link rel="canonical" href="{canon}">
  <meta name="robots" content="index,follow">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <link rel="alternate" type="application/atom+xml" title="Atom feed" href="{BASE_URL}/backlink-feed.xml">
  <style>
    :root {{
      --bg: #0b1220;
      --card: #0f1b33;
      --muted: #98a2b3;
      --text: #e5e7eb;
      --link: #93c5fd;
      --line: rgba(255,255,255,.12);
      --chip: rgba(147,197,253,.14);
      --warn: rgba(245,158,11,.18);
      --bad: rgba(239,68,68,.18);
      --good: rgba(34,197,94,.18);
    }}
    body {{
      margin: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
      background: radial-gradient(1200px 800px at 10% 0%, rgba(59,130,246,.18), transparent 60%),
                  radial-gradient(900px 700px at 90% 10%, rgba(34,197,94,.12), transparent 60%),
                  var(--bg);
      color: var(--text);
      line-height: 1.45;
    }}
    a {{ color: var(--link); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .wrap {{ max-width: 980px; padding: 22px 16px 60px; margin: 0 auto; }}
    .topbar {{
      display: flex; gap: 10px; flex-wrap: wrap;
      align-items: center; justify-content: space-between;
      border: 1px solid var(--line); background: rgba(15,27,51,.7);
      padding: 10px 12px; border-radius: 14px;
      backdrop-filter: blur(10px);
    }}
    .topbar .links a {{ margin-right: 10px; }}
    h1 {{ font-size: 26px; margin: 18px 0 8px; }}
    h2 {{ font-size: 18px; margin: 22px 0 10px; color: #dbeafe; }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    .grid {{ display: grid; grid-template-columns: repeat(1,minmax(0,1fr)); gap: 12px; margin-top: 14px; }}
    @media (min-width: 760px) {{
      .grid {{ grid-template-columns: repeat(2,minmax(0,1fr)); }}
    }}
    .card {{
      border: 1px solid var(--line);
      background: rgba(15,27,51,.72);
      border-radius: 16px;
      padding: 12px 12px 10px;
      backdrop-filter: blur(10px);
    }}
    .card .t {{ font-size: 15px; font-weight: 700; margin: 0 0 4px; }}
    .card .u {{ font-size: 13px; word-break: break-word; opacity: .95; }}
    .chips {{ display: flex; gap: 6px; flex-wrap: wrap; margin-top: 8px; }}
    .chip {{
      font-size: 12px; padding: 3px 8px; border-radius: 999px;
      background: var(--chip); border: 1px solid var(--line); color: #dbeafe;
    }}
    .chip.good {{ background: var(--good); }}
    .chip.warn {{ background: var(--warn); }}
    .chip.bad {{ background: var(--bad); }}
    .desc {{ margin-top: 8px; font-size: 13px; color: #e2e8f0; }}
    pre {{
      white-space: pre-wrap; word-break: break-word;
      background: rgba(15,27,51,.72); border: 1px solid var(--line);
      padding: 12px; border-radius: 16px; overflow: auto;
    }}
    .footer {{
      margin-top: 26px; color: var(--muted); font-size: 12px;
      border-top: 1px solid var(--line); padding-top: 14px;
    }}
  </style>
</head>"""

def build_index_page(dates_sorted, months_sorted, latest_date):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    desc = "Daily hub of reference URLs with archives, feed, sitemap, and health checks."
    latest_link = f'<a href="{BASE_URL}/d/{latest_date}.html">Open latest</a>' if latest_date else ""
    recent = list(reversed(dates_sorted[-DAYS_ON_INDEX:]))
    days_html = "".join([f'<li><a href="{BASE_URL}/d/{d}.html">{d}</a></li>' for d in recent]) if recent else "<li>No data yet</li>"
    months_html = "".join([f'<li><a href="{BASE_URL}/m/{m}.html">{m}</a></li>' for m in reversed(months_sorted[-18:])]) if months_sorted else "<li>No months yet</li>"

    path = "/"
    return f"""<!doctype html>
<html lang="en">
{common_head("Backlink Discovery Hub", desc, path)}
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="links">
        <a href="{BASE_URL}/all.html">All</a>
        <a href="{BASE_URL}/d/index.html">Daily index</a>
        <a href="{BASE_URL}/backlink-feed.xml">Feed</a>
        <a href="{BASE_URL}/recent.json">JSON</a>
        <a href="{BASE_URL}/sitemap.xml">Sitemap</a>
      </div>
      <div class="muted">Updated: {updated}</div>
    </div>

    <h1>Backlink Discovery Hub</h1>
    <p class="muted">A structured archive of reference URLs with basic crawl hints and metadata.</p>
    <p>{latest_link}</p>

    <h2>Recent days</h2>
    <ul>{days_html}</ul>

    <h2>Recent months</h2>
    <ul>{months_html}</ul>

    <div class="footer">
      Canonical: {escape_html(CANONICAL_BASE)}
    </div>
  </div>
</body>
</html>"""

def build_daily_page(date_s, items):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    desc = f"Reference URL list and metadata for {date_s}."
    cards = []
    plain = []
    for it in items:
        url = it["url"]
        title = it.get("title") or it.get("title_hint") or url
        note = it.get("note") or ""
        tags = it.get("tags") or ""
        status = it.get("status_code") or ""
        robots_noindex = it.get("robots_meta_noindex") or ""
        xrobots = (it.get("x_robots_tag") or "").lower()
        hint = ""
        if robots_noindex == "yes" or "noindex" in xrobots:
            hint = "noindex"
        elif status and status.startswith("2"):
            hint = "ok"
        elif status:
            hint = f"status:{status}"

        chips = []
        if hint:
            cls = "chip good" if hint == "ok" else ("chip bad" if hint == "noindex" else "chip warn")
            chips.append(f'<span class="{cls}">{escape_html(hint)}</span>')
        if tags:
            for t in tags.split(",")[:6]:
                chips.append(f'<span class="chip">{escape_html(t)}</span>')

        if note:
            chips.append(f'<span class="chip">{escape_html(safe_text(note, 60))}</span>')

        description = it.get("description") or ""
        desc_html = f'<div class="desc">{escape_html(description)}</div>' if description else ""

        cards.append(f"""<div class="card">
  <div class="t">{escape_html(title) if title else escape_html(url)}</div>
  <div class="u"><a href="{escape_html(url)}" target="_blank" rel="noopener noreferrer">{escape_html(url)}</a></div>
  <div class="chips">{''.join(chips)}</div>
  {desc_html}
</div>""")
        plain.append(url)

    path = f"/d/{date_s}.html"
    return f"""<!doctype html>
<html lang="en">
{common_head(f"Daily Discovery {date_s}", desc, path)}
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="links">
        <a href="{BASE_URL}/">Hub</a>
        <a href="{BASE_URL}/all.html">All</a>
        <a href="{BASE_URL}/d/index.html">Daily index</a>
        <a href="{BASE_URL}/backlink-feed.xml">Feed</a>
      </div>
      <div class="muted">Updated: {updated}</div>
    </div>

    <h1>Daily Discovery: {date_s}</h1>
    <p class="muted">Cards include lightweight metadata to reduce bare list patterns.</p>

    <div class="grid">
      {''.join(cards) if cards else '<div class="card"><div class="t">No links</div></div>'}
    </div>

    <h2>Plain list</h2>
    <pre>{escape_html("\\n".join(plain))}</pre>

    <div class="footer">Source: {escape_html(BASE_URL)}</div>
  </div>
</body>
</html>"""

def build_daily_index_page(dates_sorted):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    desc = "Index of daily pages."
    li = "".join([f'<li><a href="{BASE_URL}/d/{d}.html">{d}</a></li>' for d in reversed(dates_sorted[-365:])]) if dates_sorted else "<li>No data</li>"
    path = "/d/index.html"
    return f"""<!doctype html>
<html lang="en">
{common_head("Daily Index", desc, path)}
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="links">
        <a href="{BASE_URL}/">Hub</a>
        <a href="{BASE_URL}/all.html">All</a>
        <a href="{BASE_URL}/backlink-feed.xml">Feed</a>
      </div>
      <div class="muted">Updated: {updated}</div>
    </div>

    <h1>Daily Index</h1>
    <ul>{li}</ul>
  </div>
</body>
</html>"""

def build_month_page(month_s, items):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    desc = f"Monthly archive for {month_s}."
    groups = {}
    for it in items:
        groups.setdefault(it["date"], []).append(it)
    blocks = []
    for d in sorted(groups.keys(), reverse=True):
        blocks.append(f"<h2>{escape_html(d)}</h2>")
        cards = []
        for it in groups[d][:MAX_PER_PAGE]:
            url = it["url"]
            title = it.get("title") or it.get("title_hint") or url
            description = it.get("description") or ""
            tags = it.get("tags") or ""
            chips = []
            if tags:
                for t in tags.split(",")[:6]:
                    chips.append(f'<span class="chip">{escape_html(t)}</span>')
            cards.append(f"""<div class="card">
  <div class="t">{escape_html(title) if title else escape_html(url)}</div>
  <div class="u"><a href="{escape_html(url)}" target="_blank" rel="noopener noreferrer">{escape_html(url)}</a></div>
  <div class="chips">{''.join(chips)}</div>
  {f'<div class="desc">{escape_html(description)}</div>' if description else ''}
</div>""")
        blocks.append(f'<div class="grid">{"".join(cards)}</div>')
    path = f"/m/{month_s}.html"
    return f"""<!doctype html>
<html lang="en">
{common_head(f"Monthly Archive {month_s}", desc, path)}
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="links">
        <a href="{BASE_URL}/">Hub</a>
        <a href="{BASE_URL}/all.html">All</a>
        <a href="{BASE_URL}/d/index.html">Daily index</a>
        <a href="{BASE_URL}/backlink-feed.xml">Feed</a>
      </div>
      <div class="muted">Updated: {updated}</div>
    </div>

    <h1>Monthly Archive: {escape_html(month_s)}</h1>
    {''.join(blocks) if blocks else '<p class="muted">No data</p>'}
  </div>
</body>
</html>"""

def build_all_page(unique_items):
    updated = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    desc = "All recent reference URLs with metadata."
    cards = []
    plain = []
    for it in unique_items[:600]:
        url = it["url"]
        title = it.get("title") or it.get("title_hint") or url
        description = it.get("description") or ""
        tags = it.get("tags") or ""
        date_s = it.get("date") or ""
        chips = []
        if date_s:
            chips.append(f'<span class="chip">{escape_html(date_s)}</span>')
        if tags:
            for t in tags.split(",")[:6]:
                chips.append(f'<span class="chip">{escape_html(t)}</span>')
        cards.append(f"""<div class="card">
  <div class="t">{escape_html(title) if title else escape_html(url)}</div>
  <div class="u"><a href="{escape_html(url)}" target="_blank" rel="noopener noreferrer">{escape_html(url)}</a></div>
  <div class="chips">{''.join(chips)}</div>
  {f'<div class="desc">{escape_html(description)}</div>' if description else ''}
</div>""")
        plain.append(url)

    path = "/all.html"
    return f"""<!doctype html>
<html lang="en">
{common_head("All Recent Links", desc, path)}
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="links">
        <a href="{BASE_URL}/">Hub</a>
        <a href="{BASE_URL}/d/index.html">Daily index</a>
        <a href="{BASE_URL}/backlink-feed.xml">Feed</a>
        <a href="{BASE_URL}/recent.json">JSON</a>
      </div>
      <div class="muted">Updated: {updated}</div>
    </div>

    <h1>All Recent Links</h1>
    <p class="muted">This list is limited to recent unique URLs.</p>

    <div class="grid">
      {''.join(cards) if cards else '<div class="card"><div class="t">No links</div></div>'}
    </div>

    <h2>Plain list</h2>
    <pre>{escape_html("\\n".join(plain))}</pre>
  </div>
</body>
</html>"""

def build_atom_feed(latest_items):
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    feed_id = f"{BASE_URL}/backlink-feed.xml"
    entries = []
    for it in latest_items[:120]:
        u = it["url"]
        pu = urlparse(u)
        eid = f"urn:link:{pu.netloc}{pu.path}"
        title = it.get("title") or it.get("title_hint") or u
        summary = it.get("description") or it.get("note") or ""
        summary = safe_text(summary, 200)
        entries.append(f"""
  <entry>
    <title>{escape_html(title)}</title>
    <id>{escape_html(eid)}</id>
    <link href="{escape_html(u)}" />
    <updated>{now}</updated>
    <summary>{escape_html(summary)}</summary>
  </entry>""".rstrip())
    return f"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Backlink Discovery Feed</title>
  <id>{escape_html(feed_id)}</id>
  <updated>{now}</updated>
  <link rel="self" href="{escape_html(feed_id)}" />
{''.join(entries)}
</feed>
"""

def build_recent_json(latest_items):
    out = []
    for it in latest_items[:250]:
        out.append({
            "date": it.get("date") or "",
            "url": it["url"],
            "title": it.get("title") or it.get("title_hint") or "",
            "description": it.get("description") or "",
            "tags": it.get("tags") or "",
            "status": it.get("status_code") or "",
            "final_url": it.get("final_url") or "",
        })
    return json.dumps({"updated_utc": dt.datetime.utcnow().isoformat() + "Z", "items": out}, ensure_ascii=False, indent=2)

def build_sitemap(dates_sorted, months_sorted, latest_date):
    """
    Accurate-ish lastmod:
    - daily pages: date itself
    - month pages: first day of month
    - index/all/feed/recent/d-index: latest_date if available else today
    """
    today = dt.datetime.utcnow().date().isoformat()
    base_lastmod = latest_date or today

    urls = [
        (f"{BASE_URL}/", base_lastmod),
        (f"{BASE_URL}/all.html", base_lastmod),
        (f"{BASE_URL}/backlink-feed.xml", base_lastmod),
        (f"{BASE_URL}/recent.json", base_lastmod),
        (f"{BASE_URL}/d/index.html", base_lastmod),
        (f"{BASE_URL}/robots.txt", base_lastmod),
    ]

    for d in dates_sorted[-365:]:
        urls.append((f"{BASE_URL}/d/{d}.html", d))

    for m in months_sorted[-60:]:
        # Use day 01 for sitemap lastmod format
        lm = f"{m}-01"
        urls.append((f"{BASE_URL}/m/{m}.html", lm))

    items = "\n".join([f"<url><loc>{escape_html(u)}</loc><lastmod>{escape_html(lm)}</lastmod></url>" for (u, lm) in urls])
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{items}
</urlset>
"""

def build_robots():
    return f"User-agent: *\nAllow: /\nSitemap: {BASE_URL}/sitemap.xml\n"

# -------------------------
# IndexNow + Ping-O-Matic
# -------------------------

def _find_existing_indexnow_key(docs_dir: str) -> str:
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

def ensure_indexnow_key_file(docs_dir: str, base_url: str):
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

# -------------------------
# Main
# -------------------------

def main():
    rows = read_daily_csv(DATA_FILE)

    by_date = {}
    for it in rows:
        by_date.setdefault(it["date"], []).append(it)

    today = dt.datetime.utcnow().date().isoformat()

    extra = parse_extra_urls(EXTRA_URLS)
    if extra:
        for x in extra:
            by_date.setdefault(today, []).append({
                "date": today,
                "url": x["url"],
                "note": x.get("note", ""),
                "tags": x.get("tags", ""),
                "title_hint": "",
            })

    dates_sorted = sorted(by_date.keys())
    latest_date = dates_sorted[-1] if dates_sorted else ""
    months_sorted = sorted({d[:7] for d in dates_sorted if re.match(r"^\d{4}-\d{2}-\d{2}$", d)})

    safe_mkdir(DOCS_DIR)
    safe_mkdir(os.path.join(DOCS_DIR, "d"))
    safe_mkdir(os.path.join(DOCS_DIR, "m"))
    safe_mkdir(os.path.join(DOCS_DIR, "health"))

    # Load meta cache
    meta_cache = read_meta_csv(META_FILE)

    # Collect recent unique items (kept in memory for all/feed/json)
    recent_unique = []
    seen = set()

    for d in reversed(dates_sorted[-DAYS_ON_INDEX:]):
        for it in by_date.get(d, []):
            u = it["url"]
            if u in seen:
                continue
            seen.add(u)
            recent_unique.append(it)

    # Prepare daily pages (last 365 days)
    for d in dates_sorted[-365:]:
        items = by_date.get(d, [])[:MAX_PER_PAGE]

        # enrich items: status + meta
        for it in items:
            u = it["url"]
            status, final_url, ctype, xrobots, noindex_hint = head_check(u)
            it["status_code"] = str(status) if status else ""
            it["final_url"] = final_url or ""
            it["content_type"] = ctype or ""
            it["x_robots_tag"] = xrobots or ""
            it["noindex_hint"] = "yes" if noindex_hint else ""

            if FETCH_META:
                cached = meta_cache.get(u, {})
                cached_ok = (cached.get("title") or cached.get("description") or cached.get("robots_meta_noindex"))
                if not cached_ok:
                    m = fetch_meta(u)
                    m["last_fetch_utc"] = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    meta_cache[u] = m
                    cached = m
                it["title"] = (cached.get("title") or "").strip() or (it.get("title_hint") or "")
                it["description"] = (cached.get("description") or "").strip()
                it["robots_meta_noindex"] = (cached.get("robots_meta_noindex") or "").strip()

        # write daily page
        with open(os.path.join(DOCS_DIR, "d", f"{d}.html"), "w", encoding="utf-8") as f:
            f.write(build_daily_page(d, items))

        # write health csv (expanded columns)
        health_path = os.path.join(DOCS_DIR, "health", f"{d}.csv")
        with open(health_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date","input_url","status","final_url","content_type","x_robots_tag","head_noindex_hint","meta_noindex","title","description"])
            for it in items:
                w.writerow([
                    d,
                    it["url"],
                    it.get("status_code",""),
                    it.get("final_url",""),
                    it.get("content_type",""),
                    it.get("x_robots_tag",""),
                    it.get("noindex_hint",""),
                    it.get("robots_meta_noindex",""),
                    it.get("title",""),
                    it.get("description",""),
                ])

    # Enrich recent_unique from meta cache
    enriched_recent = []
    for it in recent_unique:
        u = it["url"]
        cached = meta_cache.get(u, {})
        it2 = dict(it)
        it2["title"] = (cached.get("title") or "").strip() or (it2.get("title_hint") or "")
        it2["description"] = (cached.get("description") or "").strip()
        it2["final_url"] = (cached.get("final_url") or "").strip()
        it2["status_code"] = (cached.get("status_code") or "").strip()
        enriched_recent.append(it2)

    # index
    with open(os.path.join(DOCS_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(build_index_page(dates_sorted, months_sorted, latest_date))

    # daily index page
    with open(os.path.join(DOCS_DIR, "d", "index.html"), "w", encoding="utf-8") as f:
        f.write(build_daily_index_page(dates_sorted))

    # month pages
    items_all = []
    for d in dates_sorted:
        items_all.extend(by_date.get(d, []))
    # attach meta for month pages using cache
    for it in items_all:
        u = it["url"]
        cached = meta_cache.get(u, {})
        it["title"] = (cached.get("title") or "").strip() or (it.get("title_hint") or "")
        it["description"] = (cached.get("description") or "").strip()

    for m in months_sorted[-60:]:
        month_items = [it for it in items_all if (it.get("date","")[:7] == m)]
        with open(os.path.join(DOCS_DIR, "m", f"{m}.html"), "w", encoding="utf-8") as f:
            f.write(build_month_page(m, month_items))

    # all page
    with open(os.path.join(DOCS_DIR, "all.html"), "w", encoding="utf-8") as f:
        f.write(build_all_page(enriched_recent))

    # atom feed
    with open(os.path.join(DOCS_DIR, "backlink-feed.xml"), "w", encoding="utf-8") as f:
        f.write(build_atom_feed(enriched_recent))

    # recent.json
    with open(os.path.join(DOCS_DIR, "recent.json"), "w", encoding="utf-8") as f:
        f.write(build_recent_json(enriched_recent))

    # sitemap + robots
    with open(os.path.join(DOCS_DIR, "sitemap.xml"), "w", encoding="utf-8") as f:
        f.write(build_sitemap(dates_sorted, months_sorted, latest_date))

    with open(os.path.join(DOCS_DIR, "robots.txt"), "w", encoding="utf-8") as f:
        f.write(build_robots())

    # GitHub Pages: disable Jekyll processing
    with open(os.path.join(DOCS_DIR, ".nojekyll"), "w", encoding="utf-8") as f:
        f.write("")

    # Persist meta cache
    write_meta_csv(META_FILE, meta_cache)

    # Broadcast
    daily_page_url = f"{BASE_URL}/d/{today}.html"
    broadcast_indexnow(daily_page_url, DOCS_DIR, BASE_URL)

    feed_url = f"{BASE_URL}/backlink-feed.xml"
    broadcast_pingomatic(feed_url)

if __name__ == "__main__":
    main()
