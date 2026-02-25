#!/usr/bin/env python3
"""Generate NotebookLM chapter infographics from ranked chapters."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from infographic_engine import (
    default_out_dir,
    load_ranked_chapters,
    load_source_map_json,
    parse_csv_ids,
    run_generation,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="NotebookLM per-chapter infographic generator")
    ap.add_argument("--ranked-json", required=True)
    ap.add_argument("--notebook-id", required=True)
    ap.add_argument("--chapter-ids", default="", help="Optional comma-separated chapter IDs")
    ap.add_argument("--source-map-json", default="", help="Optional source map JSON or run_manifest.json")
    ap.add_argument("--profile", default="default")
    ap.add_argument("--chars-per-chapter", type=int, default=6000)
    ap.add_argument("--max-chapters", type=int, default=0)
    ap.add_argument("--poll-seconds", type=int, default=15)
    ap.add_argument("--max-polls", type=int, default=60)
    ap.add_argument("--run-id", default="")
    ap.add_argument(
        "--workspace-root",
        default="",
        help="Base output root. Defaults to NOTEBOOKLM_CHAPTER_MENU_ROOT or this skill directory.",
    )
    ap.add_argument(
        "--out-dir",
        default="",
        help="Defaults to <workspace-root>/tmp/notebooklm_poc/chapter-infographic-artifacts",
    )
    args = ap.parse_args()

    out_dir = Path(args.out_dir).expanduser() if args.out_dir else default_out_dir(
        args.workspace_root,
        "tmp/notebooklm_poc/chapter-infographic-artifacts",
    )

    selected = load_ranked_chapters(args.ranked_json)
    source_map = load_source_map_json(args.source_map_json)
    chapter_ids = parse_csv_ids(args.chapter_ids)

    result = run_generation(
        notebook_id=args.notebook_id,
        profile=args.profile,
        out_dir=out_dir,
        selected_chapters=selected,
        chapter_ids=chapter_ids or None,
        max_chapters=args.max_chapters,
        chars_per_chapter=args.chars_per_chapter,
        poll_seconds=args.poll_seconds,
        max_polls=args.max_polls,
        source_map=source_map,
        run_id=args.run_id or None,
    )

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
