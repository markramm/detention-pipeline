#!/usr/bin/env python3
"""
The weekly ingest's validation gate and run report.

Until 2026-10 a batch that added any validation error was thrown away whole:
run_ingest_ci.sh compared the error count before and after conversion and,
on any increase, reverted kb/ to HEAD. Four invalid entries cost the 1,197
valid ones beside them (runs of Sept 8, 22, 29 and Oct 6), and the workflow
then filed "All ingest sources failed", which was not what happened.

The gate works per entry instead:

  baseline   record every validation ERROR on HEAD, before the batch lands.
  gate       after conversion, find the errors that are not in the baseline,
             pin each to the file this batch wrote (json_to_entries
             --manifest), and quarantine those files: their content goes to
             the quarantine directory (outside kb/, so the site never builds
             it), and kb/ gets back what HEAD had for that path. Everything
             else in the batch stays. Errors already on HEAD are neither
             blocking nor blamed on the batch.
  report     write the run report (markdown for the PR or issue body, plus a
             status JSON) from the per-source results and the gate's result.

Why quarantine lives outside kb/: hugo/generate_content.py publishes every
.md under kb/ and the pre-commit hook validates every .md under kb/, so a
quarantine directory inside kb/ would be published and would break commits.
The sources re-emit the same records every week, so nothing is lost by not
committing them: the report lists each one with its error and source, and
the workflow uploads the files as the run's `ingest-quarantine` artifact.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import validate_entries  # noqa: E402
from frontmatter import parse as parse_frontmatter  # noqa: E402

KB_ROOT = Path(__file__).parent.parent

DUP_RE = re.compile(r"^duplicate ID \(also in (.+)\)$")

# Lines in a source's log that mean part of it failed even though the
# script exited 0: one Legistar client returning HTTP 500, BLS refusing the
# unemployment download, one USAspending page erroring.
PROBLEM_RE = re.compile(
    r"(api error|error fetching|error parsing|download failed|failed to download"
    r"|http [45]\d\d|skipped \(|traceback|exception)",
    re.IGNORECASE,
)
MAX_PROBLEMS_PER_SOURCE = 10
MAX_DETAILS = 10  # entries whose full file is inlined in the report
MAX_REPORT_CHARS = 45000  # a PR body is capped at 65,536; leave room for the diff summary


# ── gate ────────────────────────────────────────────────────────────────


def new_errors(baseline, post):
    """Errors in `post` that `baseline` does not have, as [path, message]
    pairs (a multiset difference, so a second identical error still counts)."""
    left = Counter(tuple(e) for e in baseline)
    out = []
    for e in post:
        key = tuple(e)
        if left[key]:
            left[key] -= 1
        else:
            out.append([key[0], key[1]])
    return out


def attribute(errors, touched):
    """Pin each new error to a file this batch wrote.

    `touched` maps kb-relative path -> manifest record. A duplicate-ID error
    is reported on whichever of the two files the validator saw second; when
    that is an untouched file, the batch's file is the other one, named in
    the message. Returns ({path: [message, ...]}, [unattributed errors]).
    """
    culprits: dict[str, list[str]] = {}
    unattributed = []
    for path, msg in errors:
        if path in touched:
            culprits.setdefault(path, []).append(msg)
            continue
        m = DUP_RE.match(msg)
        if m and m.group(1) in touched:
            culprits.setdefault(m.group(1), []).append(f"duplicate ID (already in {path})")
            continue
        unattributed.append([path, msg])
    return culprits, unattributed


def _git(repo, *args, check=True):
    return subprocess.run(["git", "-C", str(repo), *args], check=check,
                          capture_output=True, text=True)


def _in_head(repo, repo_path):
    return _git(repo, "cat-file", "-e", f"HEAD:{repo_path}", check=False).returncode == 0


def _restore(kb_root, repo, rel):
    """Give kb/<rel> back what HEAD had: the old file, or nothing."""
    repo_path = (kb_root / rel).resolve().relative_to(repo.resolve()).as_posix()
    if _in_head(repo, repo_path):
        _git(repo, "checkout", "HEAD", "--", repo_path)
    else:
        (kb_root / rel).unlink(missing_ok=True)


def _describe(text):
    parsed = parse_frontmatter(text) if text else None
    fields = parsed.fields if parsed else {}
    return {k: str(fields.get(k, "") or "") for k in ("id", "title", "type", "source_url", "state", "county", "fips")}


def quarantine(kb_root, culprits, touched, qdir):
    """Move each culprit out of kb/ into `qdir` and restore HEAD's version."""
    kb_root = Path(kb_root)
    repo = Path(_git(kb_root, "rev-parse", "--show-toplevel").stdout.strip())
    out = []
    for rel, messages in sorted(culprits.items()):
        src = kb_root / rel
        text = src.read_text(encoding="utf-8") if src.exists() else ""
        dest = Path(qdir) / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text, encoding="utf-8")
        rec = touched[rel]
        _restore(kb_root, repo, rel)
        if rec.get("removed"):
            _restore(kb_root, repo, rec["removed"])
        out.append({
            "path": rel,
            "status": rec.get("status", ""),
            "source": rec.get("source", ""),
            "errors": messages,
            "quarantine_file": str(dest),
            **_describe(text),
        })
    return out


def load_manifest(path):
    """kb-relative path -> record; a file two sources wrote names both."""
    touched: dict[str, dict] = {}
    for rec in json.loads(Path(path).read_text(encoding="utf-8")):
        prev = touched.get(rec["path"])
        if prev and prev.get("source") != rec.get("source"):
            rec = {**rec, "source": f"{prev['source']}, {rec['source']}"}
        if prev and prev.get("status") == "created":
            rec = {**rec, "status": "created"}
        touched[rec["path"]] = rec
    return touched


def run_gate(kb_root, baseline, touched, qdir, max_rounds=5):
    """Quarantine until the batch adds no error to the baseline. Returns
    {"quarantined": [...], "unattributed": [...], "post_errors": n}.
    Unattributed errors are left for the caller, which fails closed."""
    quarantined = []
    unattributed = []
    post = validate_entries.error_records(kb_root)
    for _ in range(max_rounds):
        errors = new_errors(baseline, post)
        if not errors:
            unattributed = []
            break
        live = {p: r for p, r in touched.items() if p not in {q["path"] for q in quarantined}}
        culprits, unattributed = attribute(errors, live)
        if not culprits:
            break
        quarantined += quarantine(kb_root, culprits, live, qdir)
        post = validate_entries.error_records(kb_root)
    return {
        "quarantined": quarantined,
        "unattributed": unattributed,
        "baseline_errors": len(baseline),
        "post_errors": len(post),
    }


# ── report ──────────────────────────────────────────────────────────────


def read_sources(path):
    """Parse the runner's sources.tsv: name, status, entries, log path."""
    sources = []
    p = Path(path)
    if not p.exists():
        return sources
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        name, status, count, log = (line.split("\t") + ["", "", ""])[:4]
        problems = []
        if log and Path(log).exists():
            seen = set()
            for raw in Path(log).read_text(encoding="utf-8", errors="replace").splitlines():
                text = raw.strip()
                if PROBLEM_RE.search(text) and text not in seen:
                    seen.add(text)
                    problems.append(text[:300])
        sources.append({
            "name": name,
            "status": status,
            "entries": int(count) if count.isdigit() else 0,
            "problems": problems,
        })
    return sources


def summarize(sources, gate, aborted=None):
    """The facts the PR body, the issue title and the workflow branch on."""
    failed = [s["name"] for s in sources if s["status"] != "ok"]
    partial = [s["name"] for s in sources if s["status"] == "ok" and s["problems"]]
    quarantined = len((gate or {}).get("quarantined", []))
    return {
        "sources_run": len(sources),
        "failed_sources": failed,
        "partial_sources": partial,
        "all_sources_failed": bool(sources) and len(failed) == len(sources),
        "quarantined": quarantined,
        "baseline_errors": (gate or {}).get("baseline_errors"),
        "aborted": aborted,
    }


def headline(status):
    if status["aborted"]:
        return f"Ingest stopped at {status['aborted']}; nothing from this run landed."
    if status["all_sources_failed"]:
        return f"All {status['sources_run']} ingest sources failed; nothing to land."
    parts = []
    if status["failed_sources"]:
        parts.append(f"{len(status['failed_sources'])} of {status['sources_run']} sources failed "
                     f"({', '.join(status['failed_sources'])})")
    if status["partial_sources"]:
        parts.append(f"{len(status['partial_sources'])} reported errors for part of their data "
                     f"({', '.join(status['partial_sources'])})")
    if status["quarantined"]:
        parts.append(f"{status['quarantined']} entries failed validation and were quarantined")
    if not parts:
        return "All sources ran cleanly; every entry passed validation."
    return "; ".join(parts) + "."


def issue_title(status, date):
    if status["aborted"]:
        return f"Weekly ingest stopped {date}: {status['aborted']}"
    if status["all_sources_failed"]:
        return f"Weekly ingest failed {date}: all sources failed"
    return f"Weekly ingest {date}: no changes landed"


def render(sources, gate, status, run_url=""):
    lines = [f"**Ingest run:** {headline(status)}", ""]
    if status["aborted"] and status.get("aborted_reason"):
        lines += [f"> {status['aborted_reason']}", ""]

    if sources:
        lines += ["| Source | Result | Entries | Errors reported |", "|---|---|---|---|"]
        for s in sources:
            result = {"ok": "ok", "failed": "**failed**", "malformed": "**malformed JSON**"}.get(
                s["status"], s["status"])
            lines.append(f"| {s['name']} | {result} | {s['entries']} | {len(s['problems'])} |")
        lines.append("")
        problem_sources = [s for s in sources if s["problems"]]
        if problem_sources:
            lines += ["### Source errors", ""]
            for s in problem_sources:
                lines.append(f"- **{s['name']}**")
                for p in s["problems"][:MAX_PROBLEMS_PER_SOURCE]:
                    lines.append(f"  - `{p.replace('`', chr(39))}`")
                extra = len(s["problems"]) - MAX_PROBLEMS_PER_SOURCE
                if extra > 0:
                    lines.append(f"  - ... and {extra} more (see the run log)")
            lines.append("")

    q = (gate or {}).get("quarantined", [])
    if q:
        lines += [
            f"### Quarantined entries ({len(q)})",
            "",
            "These entries failed validation and were kept out of `kb/`; the rest of the batch "
            "is in this PR. Each needs a human: fix the ingest mapping (the source re-emits the "
            "record every week), or add the file by hand once corrected, or drop it. The full "
            "files are in the run's `ingest-quarantine` artifact"
            + (f" ([run]({run_url}))" if run_url else "") + ".",
            "",
            "| Entry | Source | Validation error | Upstream |",
            "|---|---|---|---|",
        ]
        for e in q:
            label = e.get("title") or e.get("id") or e["path"]
            up = f"[link]({e['source_url']})" if e.get("source_url") else ""
            lines.append(f"| `{e['path']}` ({e['status']}): {label.replace('|', '/')} | {e['source']} | "
                         f"{'; '.join(e['errors']).replace('|', '/')} | {up} |")
        lines.append("")
        for e in q[:MAX_DETAILS]:
            try:
                body = Path(e["quarantine_file"]).read_text(encoding="utf-8")
            except OSError:
                continue
            lines += [f"<details><summary><code>{e['path']}</code></summary>", "", "```markdown",
                      body.rstrip()[:2000].replace("```", "'''"), "```", "", "</details>"]
        lines.append("")

    if gate and gate.get("unattributed"):
        lines += ["### Errors the gate could not pin to this batch", ""]
        for path, msg in gate["unattributed"][:20]:
            lines.append(f"- `{path}`: {msg}")
        lines.append("")

    if gate and gate.get("baseline_errors"):
        lines += [f"_{gate['baseline_errors']} validation errors were already on `main` before "
                  "this run; they are not from this batch and did not block it._", ""]
    text = "\n".join(lines)
    if len(text) > MAX_REPORT_CHARS:
        text = (text[:MAX_REPORT_CHARS].rsplit("\n", 1)[0]
                + "\n\n_Report truncated; the full report.md is in the run's "
                "`ingest-quarantine` artifact._\n")
    return text


# ── CLI ─────────────────────────────────────────────────────────────────


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("baseline", help="record HEAD's validation errors")
    b.add_argument("--out", required=True)

    g = sub.add_parser("gate", help="quarantine the batch's invalid entries")
    g.add_argument("--baseline", required=True)
    g.add_argument("--manifest", required=True)
    g.add_argument("--quarantine-dir", required=True)
    g.add_argument("--out", required=True)

    r = sub.add_parser("report", help="write the run report")
    r.add_argument("--sources", required=True)
    r.add_argument("--gate")
    r.add_argument("--aborted", default="")
    r.add_argument("--aborted-reason", default="")
    r.add_argument("--run-url", default="")
    r.add_argument("--date", default="")
    r.add_argument("--out-md", required=True)
    r.add_argument("--out-status", required=True)

    args = p.parse_args(argv)
    kb_root = KB_ROOT

    if args.cmd == "baseline":
        errors = validate_entries.error_records(kb_root)
        Path(args.out).write_text(json.dumps(errors), encoding="utf-8")
        print(f"  HEAD has {len(errors)} validation errors.")
        return 0

    if args.cmd == "gate":
        baseline = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
        manifest = Path(args.manifest)
        touched = load_manifest(manifest) if manifest.exists() else {}
        result = run_gate(kb_root, baseline, touched, args.quarantine_dir)
        Path(args.out).write_text(json.dumps(result, indent=1), encoding="utf-8")
        print(f"  Baseline {result['baseline_errors']} errors; after the gate {result['post_errors']}.")
        for e in result["quarantined"]:
            print(f"  QUARANTINE: {e['path']} ({e['source']}): {'; '.join(e['errors'])}")
        if result["unattributed"]:
            for path, msg in result["unattributed"]:
                print(f"  UNATTRIBUTED: {path}: {msg}", file=sys.stderr)
            return 1
        return 0

    if args.cmd == "report":
        sources = read_sources(args.sources)
        gate = None
        if args.gate and Path(args.gate).exists():
            gate = json.loads(Path(args.gate).read_text(encoding="utf-8"))
        status = summarize(sources, gate, aborted=args.aborted or None)
        status["aborted_reason"] = args.aborted_reason or None
        status["issue_title"] = issue_title(status, args.date)
        Path(args.out_md).write_text(render(sources, gate, status, args.run_url), encoding="utf-8")
        Path(args.out_status).write_text(json.dumps(status, indent=1), encoding="utf-8")
        print(f"  {headline(status)}")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
