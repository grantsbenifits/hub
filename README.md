# Backlink Discovery Hub (GitHub Pages / Vercel)

This project generates:
- A daily hub page (index.html)
- Daily pages (docs/d/YYYY-MM-DD.html)
- Atom feed (docs/backlink-feed.xml)
- Sitemap + robots.txt
- Quick health checks (docs/health/*.csv)

Purpose: Improve discovery of backlink URLs by publishing a frequently-updated hub property.
No guarantees: search engines may still choose not to crawl or index third-party pages.

## Zero hosting cost (recommended): GitHub Pages

### A) Create repo and upload files
1. Create a new GitHub repo (public).
2. Upload everything from this ZIP into the repo root (keep folders as-is).

### B) Enable GitHub Pages
1. Repo Settings -> Pages
2. Source: Deploy from a branch
3. Branch: main
4. Folder: /docs
5. Save

Your hub will become:
https://USERNAME.github.io/REPO/

### C) Set BASE_URL in workflow
Open:
.github/workflows/build.yml

Replace:
https://USERNAME.github.io/REPO
with your real GitHub Pages URL.

### D) Add links (batch mode)
Edit:
data/daily.csv

Format:
YYYY-MM-DD,https://some-backlink-page

Add 1 link or 10 links, both work.

### E) Run automatically
GitHub Actions will:
- Run daily on schedule
- Rebuild docs/*
- Commit and push docs/*

### F) Add links without editing files (single or batch)
GitHub -> Actions -> build-hub -> Run workflow
Paste one or multiple URLs in the input box.
This updates today's page and feed without changing data/daily.csv.

## Vercel option
You can deploy the same repo to Vercel.
Build output is already in /docs; set the project to serve /docs as static output.

## Files you edit
- data/daily.csv (optional, persistent history)
- OR use Actions "Run workflow" input for one-off URLs

