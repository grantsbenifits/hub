# Backlink Discovery Hub (heavy edition)

This repo generates a frequently updated hub that lists your reference URLs in multiple formats:
- Daily pages: `/d/YYYY-MM-DD.html`
- Monthly archive pages: `/m/YYYY-MM.html`
- All recent list: `/all.html`
- Atom feed: `/backlink-feed.xml`
- Sitemap + robots.txt
- JSON feed: `/recent.json`
- Health checks (status + robots hints): `/health/*.csv`

Notes
- Discovery is not a guarantee of indexing.
- IndexNow is for Bing ecosystem; Google does not use IndexNow.

## Quick setup

1) Create a public GitHub repo and upload this ZIP contents to repo root.
2) Enable GitHub Pages:
   - Settings -> Pages
   - Source: Deploy from a branch
   - Branch: main
   - Folder: /docs

3) Edit `.github/workflows/build.yml`:
   - Set BASE_URL to your GitHub Pages URL
   - Set CANONICAL_BASE to the same URL

4) Add links
- Persistent: edit `data/daily.csv`
  Format (flexible):
  - date,url
  - date,url,note
  - date,url,note,tags
  - date,url,note,tags,title

- One-off: Actions -> build-hub -> Run workflow
  Paste lines like:
  - https://site.com/page
  - https://site.com/page | short note | tag1,tag2

## Deploy to Netlify and Vercel

Netlify secrets:
- NETLIFY_AUTH_TOKEN
- NETLIFY_SITE_ID

Vercel secrets:
- VERCEL_TOKEN
- VERCEL_ORG_ID
- VERCEL_PROJECT_ID

If VERCEL_PROJECT_ID is not set, the Vercel step is skipped.
