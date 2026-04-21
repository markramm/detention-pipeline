#!/usr/bin/env python3
"""
Summarize what an ingest run changed relative to HEAD.

Intended for the weekly-ingest PR body. Shells out to `git` to enumerate
added / modified / deleted entries under kb/<signal>/ and groups them by
signal type. Also reports heat-score deltas (top gainers/losers) if
docs/heat_data.json is part of the diff.

Usage:
    python3 ingest_diff.py                      # summarize working-tree changes
    python3 ingest_diff.py --base origin/main   # compare against a ref
    python3 ingest_diff.py --format markdown    # (default) markdown for PR body
"""

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

KB_ROOT = Path(__file__).parent.parent.parent

SIGNAL_DIRS = [
    "287g", "anc", "budget", "commission", "comms", "ice-contracts",
    "jobs", "legislative", "real-estate", "sheriff", "facilities",
]


def git(*args, check=True):
    result = subprocess.run(
        ["git"] + list(args),
        cwd=KB_ROOT,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)
    return result.stdout


def changed_entries(base):
    """Return dict: status -> list of paths under kb/<signal>/."""
    if base:
        out = git("diff", "--name-status", base, "--", "kb/")
    else:
        # Working tree vs HEAD — includes staged, unstaged, and untracked.
        tracked = git("diff", "--name-status", "HEAD", "--", "kb/")
        untracked = git("ls-files", "--others", "--exclude-standard", "--", "kb/")
        out = tracked
        for line in untracked.strip().splitlines():
            if line:
                out += f"A\t{line}\n"

    by_status = defaultdict(list)
    for line in out.strip().splitlines():
        if not line:
            continue
        parts = line.split("\t")
        status = parts[0][0]  # first char: A/M/D/R
        path = parts[-1]
        if path.endswith(".md") and any(f"/{d}/" in path for d in SIGNAL_DIRS):
            by_status[status].append(path)
    return by_status


def group_by_signal(paths):
    counts = defaultdict(int)
    for p in paths:
        for d in SIGNAL_DIRS:
            if f"/{d}/" in p:
                counts[d] += 1
                break
    return counts


def heat_delta(before_path, after_path, top_n=10):
    """Return (gainers, losers) comparing two heat_data.json snapshots."""
    if not before_path or not after_path:
        return [], []
    bp, ap = Path(before_path), Path(after_path)
    if not (bp.exists() and ap.exists()):
        return [], []

    old_scores = {r["fips"]: r for r in json.loads(bp.read_text())}
    new_scores = {r["fips"]: r for r in json.loads(ap.read_text())}

    deltas = []
    all_fips = set(old_scores) | set(new_scores)
    for fips in all_fips:
        old_row = old_scores.get(fips)
        new_row = new_scores.get(fips)
        old_score = old_row["score"] if old_row else 0
        new_score = new_row["score"] if new_row else 0
        delta = new_score - old_score
        if delta != 0:
            ref = new_row or old_row
            deltas.append((delta, ref["county"], ref["state"], old_score, new_score))

    deltas.sort(reverse=True)
    gainers = [d for d in deltas[:top_n] if d[0] > 0]
    losers = [d for d in sorted(deltas)[:top_n] if d[0] < 0]
    return gainers, losers


def render_markdown(by_status, gainers, losers):
    out = []
    added = by_status.get("A", [])
    modified = by_status.get("M", [])
    deleted = by_status.get("D", [])

    out.append(f"**Entries:** {len(added)} added, {len(modified)} modified, {len(deleted)} deleted")
    out.append("")

    if added or modified or deleted:
        out.append("### By signal type")
        out.append("")
        out.append("| Signal | Added | Modified | Deleted |")
        out.append("| --- | ---: | ---: | ---: |")
        all_sigs = set()
        a_counts = group_by_signal(added)
        m_counts = group_by_signal(modified)
        d_counts = group_by_signal(deleted)
        all_sigs.update(a_counts, m_counts, d_counts)
        for sig in sorted(all_sigs):
            out.append(f"| {sig} | {a_counts.get(sig, 0)} | {m_counts.get(sig, 0)} | {d_counts.get(sig, 0)} |")
        out.append("")

    if gainers:
        out.append("### Top heat-score gainers")
        out.append("")
        out.append("| County | State | Old → New | Δ |")
        out.append("| --- | --- | ---: | ---: |")
        for delta, county, state, old, new in gainers:
            out.append(f"| {county} | {state} | {old} → {new} | +{delta} |")
        out.append("")

    if losers:
        out.append("### Top heat-score losers")
        out.append("")
        out.append("| County | State | Old → New | Δ |")
        out.append("| --- | --- | ---: | ---: |")
        for delta, county, state, old, new in losers:
            out.append(f"| {county} | {state} | {old} → {new} | {delta} |")
        out.append("")

    if not (added or modified or deleted):
        out.append("_No KB entries changed in this run._")

    return "\n".join(out)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="", help="Git ref to diff against (default: working tree vs HEAD)")
    p.add_argument("--heat-before", default="", help="Pre-ingest heat_data.json snapshot")
    p.add_argument("--heat-after", default="", help="Post-ingest heat_data.json snapshot")
    p.add_argument("--format", choices=["markdown"], default="markdown")
    args = p.parse_args()

    by_status = changed_entries(args.base)
    gainers, losers = heat_delta(args.heat_before, args.heat_after)
    print(render_markdown(by_status, gainers, losers))


if __name__ == "__main__":
    main()
