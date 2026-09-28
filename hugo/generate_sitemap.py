#!/usr/bin/env python3
"""
Generate a sitemap index plus per-section sitemap files.

Replaces Hugo's single flat `sitemap.xml` (disabled in hugo.toml via
`disableKinds = ["sitemap"]`). That file was last fetched by Google in
April 2026 and, worse, carried an identical `lastmod` on every one of its
~13,000 URLs — a signal that told crawlers nothing had changed and gave
them no reason to prioritize a re-crawl. Splitting into a sitemap index
with per-section files (facilities, organizations, contractors, people,
money, fights, entries, county, state, signals, other) and real per-page
`lastmod` from each KB entry's file mtime should make re-crawling
sections that actually changed cheaper for Google to prioritize.

Run from the hugo/ directory, after generate_content.py (needs
content/**/*.md to already be written) and before `hugo build`:

    python3 generate_content.py
    python3 generate_sitemap.py
    hugo --gc --minify

Writes hugo/static/sitemap.xml (the index) and one
hugo/static/sitemap-<section>.xml per section; Hugo copies hugo/static/
verbatim into the published site root, so these land at
/sitemap.xml and /sitemap-<section>.xml — the same URL robots.txt
already advertises.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "kb" / "scripts"))
from frontmatter import parse as parse_frontmatter_yaml

BASE_URL = "https://detention-pipeline.transparencycascade.org"
CONTENT_PATH = Path("content")
STATIC_PATH = Path("static")

# (content-relative directory, section key) in priority order — first
# match wins, so put more specific prefixes (players/contractors) before
# less specific ones that don't exist here but would collide (players).
SECTION_DIRS = [
    ("facilities", "facilities"),
    ("organizations", "organizations"),
    ("players/contractors", "contractors"),
    ("players/people", "people"),
    ("players/money", "money"),
    ("fights", "fights"),
    ("entry", "entries"),
    ("county", "county"),
    ("state", "state"),
    ("signals", "signals"),
    ("blog", "blog"),
]
DEFAULT_SECTION = "other"


def section_for(rel_dir: str) -> str:
    for prefix, section in SECTION_DIRS:
        if rel_dir == prefix or rel_dir.startswith(prefix + "/"):
            return section
    return DEFAULT_SECTION


def url_for(md_file: Path) -> str:
    """Mirror Hugo's pretty-URL routing for this repo's content layout:
    content/<dir>/<slug>.md -> /<dir>/<slug>/, content/<dir>/_index.md ->
    /<dir>/, content/_index.md -> /."""
    rel = md_file.relative_to(CONTENT_PATH)
    parts = list(rel.parts)
    if parts[-1] == "_index.md":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][: -len(".md")]
    if not parts:
        return "/"
    return "/" + "/".join(parts) + "/"


def is_disabled(fields: dict) -> bool:
    if fields.get("noindex") in (True, "true", "True"):
        return True
    sitemap_cfg = fields.get("sitemap")
    if isinstance(sitemap_cfg, dict) and sitemap_cfg.get("disable") in (True, "true", "True"):
        return True
    return False


def collect_urls():
    """Returns {section: [(url, lastmod), ...]}."""
    by_section = {}
    for md_file in sorted(CONTENT_PATH.rglob("*.md")):
        text = md_file.read_text(encoding="utf-8", errors="ignore")
        parsed = parse_frontmatter_yaml(text)
        fields = parsed.fields if parsed else {}
        if is_disabled(fields):
            continue
        rel_dir = str(md_file.relative_to(CONTENT_PATH).parent)
        if rel_dir == ".":
            rel_dir = ""
        section = section_for(rel_dir)
        url = BASE_URL + url_for(md_file)
        lastmod = fields.get("lastmod") or ""
        if not lastmod:
            lastmod = md_file.stat().st_mtime
            from datetime import datetime, timezone
            lastmod = datetime.fromtimestamp(lastmod, tz=timezone.utc).strftime("%Y-%m-%d")
        by_section.setdefault(section, []).append((url, str(lastmod)))
    return by_section


def write_section_sitemap(section: str, urls, out_path: Path):
    parts = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for url, lastmod in urls:
        parts.append(f"<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>")
    parts.append("</urlset>")
    out_path.write_text("\n".join(parts), encoding="utf-8")


def write_sitemap_index(sections_with_files, out_path: Path):
    parts = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for section, filename, latest_lastmod in sections_with_files:
        parts.append(
            f"<sitemap><loc>{BASE_URL}/{filename}</loc>"
            f"<lastmod>{latest_lastmod}</lastmod></sitemap>"
        )
    parts.append("</sitemapindex>")
    out_path.write_text("\n".join(parts), encoding="utf-8")


def main():
    if not CONTENT_PATH.exists():
        print("No content/ directory — run generate_content.py first.", file=sys.stderr)
        sys.exit(1)

    by_section = collect_urls()
    STATIC_PATH.mkdir(parents=True, exist_ok=True)

    sections_with_files = []
    total_urls = 0
    for section in sorted(by_section):
        urls = by_section[section]
        total_urls += len(urls)
        filename = f"sitemap-{section}.xml"
        write_section_sitemap(section, urls, STATIC_PATH / filename)
        latest = max((lm for _, lm in urls), default="")
        sections_with_files.append((section, filename, latest))
        print(f"  {filename}: {len(urls)} urls (latest lastmod {latest})")

    write_sitemap_index(sections_with_files, STATIC_PATH / "sitemap.xml")
    print(f"Wrote sitemap index (static/sitemap.xml) covering {len(sections_with_files)} "
          f"section sitemaps, {total_urls} urls total.")


if __name__ == "__main__":
    main()
