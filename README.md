# Backlink Discovery Hub (Fixed)
Upload ALL files/folders in this ZIP to your repo root.

What this fixes:
- If Netlify/Vercel secrets are missing, deployment steps are skipped (workflow still succeeds).
- MAX_PER_PAGE default set to 20 in workflow.

Add links:
- Easiest: Actions -> build-hub -> Run workflow -> paste URLs (one per line) in input box.
- Persistent: edit data/daily.csv as: YYYY-MM-DD,https://url

GitHub Pages:
Settings -> Pages -> Deploy from a branch -> main -> /docs
