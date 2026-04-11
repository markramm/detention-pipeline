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
                       ("sans_xl", 32), ("title", 42)]:
        family = "Menlo" if "mono" in name else "Helvetica"
        try:
            fonts[name] = ImageFont.truetype(family, size)
        except OSError:
            fonts[name] = ImageFont.load_default()
    return fonts


def draw_gradient_bg(draw):
    """Draw subtle gradient background."""
    for y in range(H):
        t = y / H
        r = int(10 + t * 7)
        g = int(10 + t * 7)
        b = int(15 + t * 9)
        draw.line([(0, y), (W, y)], fill=(r, g, b))


def draw_brand(draw, fonts):
    """Draw site branding at bottom."""
    y = H - 48
    draw.line([(40, y), (W - 40, y)], fill=BORDER, width=1)
    draw.text((40, y + 12), "DETENTION PIPELINE", fill=TEXT_DIM, font=fonts["mono_sm"])
    draw.text((W - 40, y + 12), "EARLY WARNING SYSTEM", fill=ACCENT, font=fonts["mono_sm"], anchor="ra")


def draw_signal_dots(draw, fonts, signals, x, y):
    """Draw signal type dots with labels."""
    cx = x
    for sig_type, info in signals.items():
        if cx > W - 100:
            break
        color = SIGNAL_COLORS.get(sig_type, TEXT_DIM)
        label = SIGNAL_LABELS.get(sig_type, sig_type)
        # Dot
        draw.ellipse([cx, y + 3, cx + 8, y + 11], fill=color)
        # Label + count
        text = f"{label} {info['count']}"
        draw.text((cx + 14, y), text, fill=TEXT_MID, font=fonts["mono_sm"])
        bbox = fonts["mono_sm"].getbbox(text)
        cx += 14 + (bbox[2] - bbox[0]) + 20


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
    draw.text((x, y + 62), "HEAT SCORE", fill=TEXT_DIM, font=fonts["mono_sm"])
    return tw + 30


def generate_default_card(fonts, output_path):
    """Generate the default site-wide OG card."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    # Title
    draw.text((40, 80), "Detention Pipeline", fill=TEXT_LIGHT, font=fonts["title"])
    draw.text((40, 135), "Early Warning System", fill=ACCENT, font=fonts["mono_lg"])

    # Subtitle
    draw.text((40, 200), "Tracking signal convergence across U.S. counties", fill=TEXT_MID, font=fonts["sans_lg"])
    draw.text((40, 240), "to detect ICE detention expansion before it happens.", fill=TEXT_MID, font=fonts["sans_lg"])

    # Stats
    stats_y = 320
    stats = [("4,500+", "Pages"), ("986", "Counties"), ("10", "Signal Types"), ("495", "Facilities")]
    sx = 40
    for val, label in stats:
        draw.text((sx, stats_y), val, fill=TEXT_LIGHT, font=fonts["mono_xl"])
        bbox = fonts["mono_xl"].getbbox(val)
        draw.text((sx, stats_y + 42), label.upper(), fill=TEXT_DIM, font=fonts["mono_sm"])
        sx += (bbox[2] - bbox[0]) + 60

    # Accent line
    draw.rectangle([40, 440, 300, 443], fill=ACCENT)

    draw.text((40, 460), "detention-pipeline.transparencycascade.org", fill=TEXT_DIM, font=fonts["mono"])

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_county_card(fonts, county_data, output_path):
    """Generate OG card for a county page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    name = county_data["county"]
    score = county_data["score"]
    signal_types = county_data["signal_types"]
    signals = county_data["signals"]

    # Score badge (large, left side)
    sw = draw_score_badge(draw, fonts, score, 40, 60)

    # County name
    draw.text((40 + sw, 70), name, fill=TEXT_LIGHT, font=fonts["sans_xl"])

    # Signal type count
    draw.text((40 + sw, 112), f"{signal_types} independent signal types converging", fill=TEXT_MID, font=fonts["sans"])

    # Signal dots
    draw_signal_dots(draw, fonts, signals, 40, 180)

    # Signal detail list
    y = 220
    for sig_type, info in signals.items():
        if y > 480:
            break
        color = SIGNAL_COLORS.get(sig_type, TEXT_DIM)
        label = SIGNAL_LABELS.get(sig_type, sig_type)
        draw.ellipse([40, y + 4, 50, y + 14], fill=color)
        draw.text((58, y), f"{label}", fill=TEXT_LIGHT, font=fonts["mono"])
        draw.text((250, y), str(info["count"]), fill=TEXT_MID, font=fonts["mono"])
        # Show first entry name truncated
        if info.get("entries"):
            entry_text = info["entries"][0][:60]
            draw.text((300, y), entry_text, fill=(80, 78, 86), font=fonts["mono_sm"])
        y += 28

    # Accent line
    draw.rectangle([40, y + 10, 200, y + 13], fill=ACCENT if score >= 70 else ACCENT_WARM)

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_state_card(fonts, state_abbr, state_name, counties, output_path):
    """Generate OG card for a state page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    max_score = max((c["score"] for c in counties), default=0)
    total_entries = sum(sum(s["count"] for s in c["signals"].values()) for c in counties)

    # State name large
    draw.text((40, 60), state_name, fill=TEXT_LIGHT, font=fonts["title"])
    draw.text((40, 115), f"{state_abbr} — Detention Pipeline", fill=TEXT_DIM, font=fonts["mono_lg"])

    # Stats row
    stats_y = 180
    stats = [
        (str(len(counties)), "Counties Tracked"),
        (str(total_entries), "Entries"),
        (str(max_score), "Max Heat Score"),
    ]
    sx = 40
    for val, label in stats:
        draw.text((sx, stats_y), val, fill=TEXT_LIGHT, font=fonts["mono_xl"])
        bbox = fonts["mono_xl"].getbbox(val)
        draw.text((sx, stats_y + 42), label.upper(), fill=TEXT_DIM, font=fonts["mono_sm"])
        sx += max((bbox[2] - bbox[0]) + 50, 200)

    # Top counties
    draw.text((40, 310), "HOTTEST COUNTIES", fill=TEXT_DIM, font=fonts["mono_sm"])
    y = 340
    top = sorted(counties, key=lambda c: -c["score"])[:6]
    for c in top:
        if y > 500:
            break
        sc = c["score"]
        color = ACCENT if sc >= 70 else ACCENT_WARM if sc >= 40 else TEXT_MID
        draw.text((40, y), str(sc), fill=color, font=fonts["mono"])
        draw.text((100, y), c["county"], fill=TEXT_LIGHT, font=fonts["sans"])
        draw.text((600, y), f"{c['signal_types']} signals", fill=TEXT_DIM, font=fonts["mono_sm"])
        y += 28

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_fight_card(fonts, title, summary, status, state, output_path):
    """Generate OG card for a county fight page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    # Status badge
    status_color = ACCENT if status == "contested" else (42, 138, 90)
    draw.rectangle([40, 60, 40 + 10, 60 + 40], fill=status_color)
    draw.text((60, 60), "COUNTY FIGHT", fill=TEXT_DIM, font=fonts["mono"])
    draw.text((220, 60), (status or "").upper(), fill=status_color, font=fonts["mono"])

    # Title (may need wrapping)
    title_clean = title.split(" — ")[0] if " — " in title else title
    if len(title_clean) > 40:
        # Wrap
        words = title_clean.split()
        line1 = ""
        line2 = ""
        for w in words:
            if len(line1 + " " + w) < 40:
                line1 = (line1 + " " + w).strip()
            else:
                line2 = (line2 + " " + w).strip()
        draw.text((40, 120), line1, fill=TEXT_LIGHT, font=fonts["sans_xl"])
        draw.text((40, 160), line2, fill=TEXT_LIGHT, font=fonts["sans_xl"])
        summary_y = 220
    else:
        draw.text((40, 120), title_clean, fill=TEXT_LIGHT, font=fonts["sans_xl"])
        summary_y = 180

    # Summary (wrapped)
    if summary:
        words = summary.split()
        lines = []
        current = ""
        for w in words:
            if len(current + " " + w) < 70:
                current = (current + " " + w).strip()
            else:
                lines.append(current)
                current = w
        if current:
            lines.append(current)
        for i, line in enumerate(lines[:4]):
            draw.text((40, summary_y + i * 24), line, fill=TEXT_MID, font=fonts["sans"])

    # Accent
    draw.rectangle([40, H - 100, 200, H - 97], fill=status_color)
    draw.text((40, H - 85), "detention-pipeline.transparencycascade.org", fill=TEXT_DIM, font=fonts["mono_sm"])

    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def generate_player_card(fonts, title, summary, player_type, signal_color_hex, output_path):
    """Generate OG card for a player/organization page."""
    img = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)
    draw_gradient_bg(draw)

    # Parse hex color
    try:
        hc = signal_color_hex.lstrip("#")
        color = tuple(int(hc[i:i+2], 16) for i in (0, 2, 4))
    except (ValueError, IndexError):
        color = ACCENT_WARM

    # Type label
    type_label = (player_type or "player").upper().replace("-", " ")
    draw.rectangle([40, 60, 40 + 10, 60 + 30], fill=color)
    draw.text((60, 62), type_label, fill=TEXT_DIM, font=fonts["mono"])

    # Title
    title_clean = title.split(" — ")[0] if " — " in title else title
    draw.text((40, 120), title_clean, fill=TEXT_LIGHT, font=fonts["sans_xl"])

    # Summary
    if summary:
        words = summary.split()
        lines = []
        current = ""
        for w in words:
            if len(current + " " + w) < 65:
                current = (current + " " + w).strip()
            else:
                lines.append(current)
                current = w
        if current:
            lines.append(current)
        for i, line in enumerate(lines[:5]):
            draw.text((40, 200 + i * 26), line, fill=TEXT_MID, font=fonts["sans"])

    draw.rectangle([40, H - 100, 200, H - 97], fill=color)
    draw_brand(draw, fonts)
    img.save(output_path, "PNG", optimize=True)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate OG social card images")
    parser.add_argument("--type", choices=["default", "county", "state", "fight", "player"], help="Generate only this type")
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

    # Default card
    if not args.type or args.type == "default":
        generate_default_card(fonts, out / "default.png")
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
                                       out / f"fight-{md.stem}.png")
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
                                            out / f"player-{md.stem}.png")
                        generated += 1
            print(f"  Generated player cards")

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
