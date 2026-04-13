#!/usr/bin/env python3
"""
Generate OG social card images for the detention pipeline site.

Produces 1200x630 PNG cards for key page types:
- Default site card
- County pages (with heat score, signal breakdown)
- Fight pages (with status, outcome summary)
- Player/organization pages (with role, summary)
- State pages (with county count, top score)

Uses Pillow for rendering. Fonts: Menlo (mono), Helvetica (sans/serif).

Usage:
    python generate_og_cards.py                  # generate all cards
    python generate_og_cards.py --type county    # only county cards
    python generate_og_cards.py --top 20         # top 20 counties only
"""

import json
import os
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

# Card dimensions (Twitter/FB recommended)
W, H = 1200, 630

# Colors (matching site design system)
BG = (10, 10, 15)           # --bg-deep
BG_PANEL = (17, 17, 24)     # --bg-panel
ACCENT = (201, 59, 59)      # --accent
ACCENT_WARM = (212, 106, 47)  # --accent-warm
TEXT_LIGHT = (232, 230, 225)
TEXT_DIM = (122, 120, 130)
TEXT_MID = (138, 136, 146)
BORDER = (42, 42, 53)

# Signal colors
SIGNAL_COLORS = {
    "igsa": (201, 59, 59),
    "287g-agreement": (212, 106, 47),
    "anc-contract": (196, 144, 37),
    "commission-activity": (106, 138, 26),
    "job-posting": (26, 138, 90),
    "sheriff-network": (26, 106, 154),
    "comms-discipline": (90, 79, 165),
    "budget-distress": (138, 63, 165),
    "real-estate-trace": (165, 63, 122),
    "legislative-trace": (79, 90, 106),
}

SIGNAL_LABELS = {
    "igsa": "IGSA",
    "287g-agreement": "287(g)",
    "anc-contract": "ANC",
    "commission-activity": "Commission",
    "job-posting": "Jobs",
    "sheriff-network": "Sheriff",
    "comms-discipline": "Comms",
    "budget-distress": "Budget",
    "real-estate-trace": "Real Estate",
    "legislative-trace": "Legislative",
}

# Font loading
def load_fonts():
    """Load fonts with fallbacks."""
    fonts = {}
    for name, size in [("mono_sm", 14), ("mono", 18), ("mono_lg", 24),
                       ("mono_xl", 36), ("mono_xxl", 56),
                       ("sans_sm", 14), ("sans", 18), ("sans_lg", 24),
                       ("sans_xl", 32), ("title", 42), ("title_xl", 56)]:
        family = "Menlo" if "mono" in name else "Helvetica"
        try:
            fonts[name] = ImageFont.truetype(family, size)
        except OSError:
            fonts[name] = ImageFont.load_default()
    return fonts


def load_heatmap_overlay():
    """Load the cropped heatmap screenshot for compositing into cards."""
    hm_path = Path(__file__).parent / "static" / "og" / "heatmap-clean.png"
    if not hm_path.exists():
        return None
    return Image.open(hm_path).convert("RGBA")


def draw_gradient_bg(draw):
    """Draw subtle gradient background."""
    for y in range(H):
        t = y / H
        r = int(10 + t * 7)
        g = int(10 + t * 7)
        b = int(15 + t * 9)
        draw.line([(0, y), (W, y)], fill=(r, g, b))


def composite_heatmap(img, heatmap_src, opacity=60):
    """Use the heatmap screenshot as the card background, dimmed for text readability."""
    if heatmap_src is None:
        return img

    # Resize heatmap to fill entire card
    hm = heatmap_src.copy().convert("RGBA")
    hm = hm.resize((W, H), Image.LANCZOS)

    # Darken the heatmap so text is readable on top
    # Blend with a dark overlay at the given opacity (0=full map, 100=fully dark)
    dark = Image.new("RGBA", (W, H), (10, 10, 15, int(255 * opacity / 100)))
    hm = Image.alpha_composite(hm, dark)

    return hm.convert("RGB")


def draw_brand(draw, fonts):
    """Draw site branding at bottom."""
    y = H - 48
    draw.line([(80, y), (W - 80, y)], fill=BORDER, width=1)
    draw.text((80, y + 12), "DETENTION PIPELINE", fill=TEXT_DIM, font=fonts["mono"])
    draw.text((W - 80, y + 12), "EARLY WARNING SYSTEM", fill=ACCENT, font=fonts["mono"], anchor="ra")


def draw_signal_dots(draw, fonts, signals, x, y):
    """Draw signal type dots with labels."""
    cx = x
    for sig_type, info in signals.items():
        if cx > W - 120:
            break
        color = SIGNAL_COLORS.get(sig_type, TEXT_DIM)
        label = SIGNAL_LABELS.get(sig_type, sig_type)
        # Dot
        draw.ellipse([cx, y + 4, cx + 10, y + 14], fill=color)
        # Label + count
        text = f"{label} {info['count']}"
        draw.text((cx + 16, y), text, fill=TEXT_MID, font=fonts["mono"])
        bbox = fonts["mono"].getbbox(text)
        cx += 16 + (bbox[2] - bbox[0]) + 24


def draw_score_badge(draw, fonts, score, x, y):
    """Draw heat score as a prominent badge."""
    if score >= 70:
        color = ACCENT
    elif score >= 40:
        color = ACCENT_WARM
    else:
        color = TEXT_MID

    score_text = str(score)
    bbox = fonts["mono_xxl"].getbbox(score_text)
    tw = bbox[2] - bbox[0]

    draw.text((x, y), score_text, fill=color, font=fonts["mono_xxl"])
    draw.text((x, y + 62), "HEAT SCORE", fill=TEXT_DIM, font=fonts["mono"])
    return tw + 30


def generate_default_card(fonts, output_path, heatmap_src=None):
    """Generate the default site-wide OG card."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    # Composite heatmap first (behind text)
    img = composite_heatmap(img, heatmap_src, opacity=50)
    draw = ImageDraw.Draw(img)

    LM = 80

    # Title
    draw.text((LM, 70), "Detention Pipeline", fill=TEXT_LIGHT, font=fonts["title_xl"])
    draw.text((LM, 135), "Early Warning System", fill=ACCENT, font=fonts["mono_xl"])

    # Subtitle
    draw.text((LM, 200), "Tracking signal convergence across", fill=TEXT_MID, font=fonts["sans_xl"])
    draw.text((LM, 240), "U.S. counties to detect ICE detention", fill=TEXT_MID, font=fonts["sans_xl"])
    draw.text((LM, 280), "expansion before it happens.", fill=TEXT_MID, font=fonts["sans_xl"])

    # Stats
    stats_y = 350
    stats = [("1,988", "Counties"), ("43", "Fights"), ("85", "Facilities")]
    sx = LM
    for val, label in stats:
        draw.text((sx, stats_y), val, fill=TEXT_LIGHT, font=fonts["mono_xl"])
        bbox = fonts["mono_xl"].getbbox(val)
        draw.text((sx, stats_y + 42), label.upper(), fill=TEXT_DIM, font=fonts["mono"])
        sx += (bbox[2] - bbox[0]) + 80

    # Accent line
    draw.rectangle([LM, 440, LM + 240, 444], fill=ACCENT)

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_county_card(fonts, county_data, output_path):
    """Generate OG card for a county page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    LM = 80
    name = county_data["county"]
    score = county_data["score"]
    signal_types = county_data["signal_types"]
    signals = county_data["signals"]

    # Score badge (large, left side)
    sw = draw_score_badge(draw, fonts, score, LM, 50)

    # County name
    draw.text((LM + sw, 58), name, fill=TEXT_LIGHT, font=fonts["title"])

    # Signal type count
    draw.text((LM + sw, 108), f"{signal_types} signal types converging", fill=TEXT_MID, font=fonts["sans_lg"])

    # Signal detail list
    y = 180
    for sig_type, info in signals.items():
        if y > 470:
            break
        color = SIGNAL_COLORS.get(sig_type, TEXT_DIM)
        label = SIGNAL_LABELS.get(sig_type, sig_type)
        draw.ellipse([LM, y + 5, LM + 12, y + 17], fill=color)
        draw.text((LM + 20, y), f"{label}", fill=TEXT_LIGHT, font=fonts["mono_lg"])
        draw.text((LM + 260, y), str(info["count"]), fill=TEXT_MID, font=fonts["mono_lg"])
        # Show first entry name truncated
        if info.get("entries"):
            entry_text = info["entries"][0][:50]
            draw.text((LM + 310, y), entry_text, fill=(80, 78, 86), font=fonts["mono"])
        y += 34

    # Accent line
    draw.rectangle([LM, y + 10, LM + 200, y + 14], fill=ACCENT if score >= 70 else ACCENT_WARM)

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_state_card(fonts, state_abbr, state_name, counties, output_path):
    """Generate OG card for a state page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    LM = 80
    max_score = max((c["score"] for c in counties), default=0)
    total_entries = sum(sum(s["count"] for s in c["signals"].values()) for c in counties)

    # State name large
    draw.text((LM, 50), state_name, fill=TEXT_LIGHT, font=fonts["title_xl"])
    draw.text((LM, 115), f"{state_abbr} — Detention Pipeline", fill=TEXT_DIM, font=fonts["mono_lg"])

    # Stats row
    stats_y = 170
    stats = [
        (str(len(counties)), "Counties"),
        (str(total_entries), "Entries"),
        (str(max_score), "Max Score"),
    ]
    sx = LM
    for val, label in stats:
        draw.text((sx, stats_y), val, fill=TEXT_LIGHT, font=fonts["mono_xl"])
        bbox = fonts["mono_xl"].getbbox(val)
        draw.text((sx, stats_y + 42), label.upper(), fill=TEXT_DIM, font=fonts["mono"])
        sx += max((bbox[2] - bbox[0]) + 60, 220)

    # Top counties
    draw.text((LM, 290), "HOTTEST COUNTIES", fill=TEXT_DIM, font=fonts["mono"])
    y = 324
    top = sorted(counties, key=lambda c: -c["score"])[:5]
    for c in top:
        if y > 490:
            break
        sc = c["score"]
        color = ACCENT if sc >= 70 else ACCENT_WARM if sc >= 40 else TEXT_MID
        draw.text((LM, y), str(sc), fill=color, font=fonts["mono_lg"])
        draw.text((LM + 80, y), c["county"], fill=TEXT_LIGHT, font=fonts["sans_lg"])
        draw.text((700, y), f"{c['signal_types']} signals", fill=TEXT_DIM, font=fonts["mono"])
        y += 34

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_fight_card(fonts, title, summary, status, state, output_path, heatmap_src=None):
    """Generate OG card for a county fight page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    img = composite_heatmap(img, heatmap_src, opacity=50)
    draw = ImageDraw.Draw(img)

    LM = 80

    # Status badge
    status_color = ACCENT if status == "contested" else (212, 106, 47) if status == "litigation" else (42, 138, 90)
    draw.rectangle([LM, 50, LM + 12, 50 + 36], fill=status_color)
    draw.text((LM + 22, 52), "COUNTY FIGHT", fill=TEXT_MID, font=fonts["mono_lg"])
    draw.text((LM + 240, 52), (status or "").upper(), fill=status_color, font=fonts["mono_lg"])

    # Title (may need wrapping)
    title_clean = title.split(" — ")[0] if " — " in title else title
    words = title_clean.split()
    lines = []
    current = ""
    for w in words:
        if len(current + " " + w) < 28:
            current = (current + " " + w).strip()
        else:
            lines.append(current)
            current = w
    if current:
        lines.append(current)
    for i, line in enumerate(lines[:2]):
        draw.text((LM, 110 + i * 64), line, fill=TEXT_LIGHT, font=fonts["title_xl"])
    summary_y = 110 + min(len(lines), 2) * 64 + 24

    # Summary (wrapped)
    if summary:
        words = summary.split()
        lines = []
        current = ""
        for w in words:
            if len(current + " " + w) < 50:
                current = (current + " " + w).strip()
            else:
                lines.append(current)
                current = w
        if current:
            lines.append(current)
        for i, line in enumerate(lines[:4]):
            draw.text((LM, summary_y + i * 32), line, fill=TEXT_MID, font=fonts["sans_lg"])

    # Accent
    draw.rectangle([LM, H - 100, LM + 200, H - 96], fill=status_color)

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_player_card(fonts, title, summary, player_type, signal_color_hex, output_path, heatmap_src=None):
    """Generate OG card for a player/organization page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    img = composite_heatmap(img, heatmap_src, opacity=50)
    draw = ImageDraw.Draw(img)

    LM = 80

    # Parse hex color
    try:
        hc = signal_color_hex.lstrip("#")
        color = tuple(int(hc[i:i+2], 16) for i in (0, 2, 4))
    except (ValueError, IndexError):
        color = ACCENT_WARM

    # Type label
    type_label = (player_type or "player").upper().replace("-", " ")
    draw.rectangle([LM, 50, LM + 12, 50 + 36], fill=color)
    draw.text((LM + 22, 52), type_label, fill=TEXT_MID, font=fonts["mono_lg"])

    # Title (wrapped)
    title_clean = title.split(" — ")[0] if " — " in title else title
    words = title_clean.split()
    lines = []
    current = ""
    for w in words:
        if len(current + " " + w) < 28:
            current = (current + " " + w).strip()
        else:
            lines.append(current)
            current = w
    if current:
        lines.append(current)
    for i, line in enumerate(lines[:2]):
        draw.text((LM, 110 + i * 64), line, fill=TEXT_LIGHT, font=fonts["title_xl"])
    summary_y = 110 + min(len(lines), 2) * 64 + 24

    # Summary
    if summary:
        words = summary.split()
        lines = []
        current = ""
        for w in words:
            if len(current + " " + w) < 50:
                current = (current + " " + w).strip()
            else:
                lines.append(current)
                current = w
        if current:
            lines.append(current)
        for i, line in enumerate(lines[:4]):
            draw.text((LM, summary_y + i * 32), line, fill=TEXT_MID, font=fonts["sans_lg"])

    draw.rectangle([LM, H - 100, LM + 200, H - 96], fill=color)
    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_blog_card(fonts, title, summary, date, output_path, heatmap_src=None):
    """Generate OG card for a blog post."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    img = composite_heatmap(img, heatmap_src, opacity=50)
    draw = ImageDraw.Draw(img)

    LM = 80

    # Type label
    draw.rectangle([LM, 50, LM + 12, 50 + 36], fill=ACCENT_WARM)
    draw.text((LM + 22, 52), "UPDATE", fill=TEXT_MID, font=fonts["mono_lg"])
    if date:
        draw.text((LM + 160, 56), date.upper(), fill=TEXT_DIM, font=fonts["mono"])

    # Title (wrapped, large)
    title_clean = title or "Pipeline Update"
    words = title_clean.split()
    lines = []
    current = ""
    for w in words:
        if len(current + " " + w) < 28:
            current = (current + " " + w).strip()
        else:
            lines.append(current)
            current = w
    if current:
        lines.append(current)
    for i, line in enumerate(lines[:2]):
        draw.text((LM, 110 + i * 64), line, fill=TEXT_LIGHT, font=fonts["title_xl"])

    # Summary (wrapped, readable)
    summary_y = 110 + min(len(lines), 2) * 64 + 24
    if summary:
        words = summary.split()
        lines = []
        current = ""
        for w in words:
            if len(current + " " + w) < 50:
                current = (current + " " + w).strip()
            else:
                lines.append(current)
                current = w
        if current:
            lines.append(current)
        for i, line in enumerate(lines[:4]):
            draw.text((LM, summary_y + i * 32), line, fill=TEXT_MID, font=fonts["sans_lg"])

    draw.rectangle([LM, H - 100, LM + 200, H - 96], fill=ACCENT_WARM)
    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate OG social card images")
    parser.add_argument("--type", choices=["default", "county", "state", "fight", "player", "blog"], help="Generate only this type")
    parser.add_argument("--top", type=int, default=50, help="Top N counties by score (default: 50)")
    parser.add_argument("--output-dir", type=str, default="static/og", help="Output directory")
    args = parser.parse_args()

    fonts = load_fonts()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    heat_path = Path("../docs/heat_data.json")
    heat_data = []
    if heat_path.exists():
        with open(heat_path) as f:
            heat_data = json.load(f)

    generated = 0

    # Load heatmap overlay for cards that use it
    heatmap_src = load_heatmap_overlay()
    if heatmap_src:
        print(f"  Loaded heatmap overlay for compositing")
    else:
        print(f"  No heatmap overlay found (run screenshot first)")

    # Default card
    if not args.type or args.type == "default":
        generate_default_card(fonts, out / "default.png", heatmap_src=heatmap_src)
        print(f"  Generated default card")
        generated += 1

    # County cards (top N by score)
    if not args.type or args.type == "county":
        for county in heat_data[:args.top]:
            fips = county["fips"]
            generate_county_card(fonts, county, out / f"county-{fips}.png")
            generated += 1
        print(f"  Generated {min(len(heat_data), args.top)} county cards")

    # State cards
    if not args.type or args.type == "state":
        by_state = {}
        for c in heat_data:
            by_state.setdefault(c["state"], []).append(c)

        STATE_NAMES = {"AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"}

        for abbr, counties in by_state.items():
            name = STATE_NAMES.get(abbr, abbr)
            generate_state_card(fonts, abbr, name, counties, out / f"state-{abbr.lower()}.png")
            generated += 1
        print(f"  Generated {len(by_state)} state cards")

    # Fight and player cards from Hugo content
    content_path = Path("content")
    if content_path.exists():
        if not args.type or args.type == "fight":
            for md in sorted(content_path.glob("fights/*.md")):
                if md.name == "_index.md":
                    continue
                text = md.read_text()
                fm = _parse_frontmatter(text)
                if fm:
                    generate_fight_card(fonts, fm.get("title", ""), fm.get("summary", ""),
                                       fm.get("status", ""), fm.get("state", ""),
                                       out / f"fight-{md.stem}.png", heatmap_src=heatmap_src)
                    generated += 1
            print(f"  Generated fight cards")

        if not args.type or args.type == "player":
            for subdir in ["players/contractors", "players/people", "organizations"]:
                for md in sorted((content_path / subdir).glob("*.md")):
                    if md.name == "_index.md":
                        continue
                    text = md.read_text()
                    fm = _parse_frontmatter(text)
                    if fm:
                        generate_player_card(fonts, fm.get("title", ""), fm.get("summary", ""),
                                            fm.get("entry_type", fm.get("player_type", "")),
                                            fm.get("signal_color", "#d46a2f"),
                                            out / f"player-{md.stem}.png",
                                            heatmap_src=heatmap_src)
                        generated += 1
            print(f"  Generated player cards")

        if not args.type or args.type == "blog":
            blog_count = 0
            for md in sorted(content_path.glob("blog/*.md")):
                if md.name == "_index.md":
                    continue
                text = md.read_text()
                fm = _parse_frontmatter(text)
                if fm:
                    generate_blog_card(fonts, fm.get("title", ""), fm.get("summary", ""),
                                      fm.get("date", ""),
                                      out / f"blog-{md.stem}.png",
                                      heatmap_src=heatmap_src)
                    generated += 1
                    blog_count += 1
            print(f"  Generated {blog_count} blog cards")

    print(f"\nTotal: {generated} OG cards → {out}/")


def _parse_frontmatter(text):
    """Quick frontmatter parser for OG card generation."""
    if not text.startswith("---"):
        return None
    try:
        end = text.index("---", 3)
    except ValueError:
        return None
    fm = {}
    for line in text[3:end].split("\n"):
        if ":" in line:
            key, val = line.split(":", 1)
            fm[key.strip()] = val.strip().strip('"')
    return fm


if __name__ == "__main__":
    main()
