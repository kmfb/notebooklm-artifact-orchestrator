#!/usr/bin/env python3
"""
Run local image-first prep pipeline for one EPUB:
1) extract chapters
2) rank/select chapters
3) build image prompts
4) build NotebookLM batch manifest
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPTS_DIR.parents[1]


def resolve_workspace_root(workspace_root: str) -> Path:
    raw = workspace_root.strip() or str(SKILL_ROOT)
    return Path(raw).expanduser().resolve()


def detect_issue_label(epub_path: Path) -> str:
    m = re.search(r"第(\d+)期(20\d{2})", epub_path.name)
    if m:
        issue, year = m.group(1), m.group(2)
        return f"{year}-{issue}"
    return epub_path.stem


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True, check=True)
    lines = [line.strip() for line in (p.stdout or "").splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"No output from command: {cmd}")
    return json.loads(lines[-1])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run NotebookLM image-first prep pipeline")
    parser.add_argument("--epub", required=True)
    parser.add_argument("--issue-label", default=None)
    parser.add_argument("--top-n", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument(
        "--workspace-root",
        default="",
        help="Base output root for downstream defaults. Defaults to this skill directory.",
    )

    # rank/select controls
    parser.add_argument("--select-mode", choices=["score", "random"], default="score")
    parser.add_argument("--w-len", type=float, default=1.0)
    parser.add_argument("--w-topic", type=float, default=1.0)
    parser.add_argument("--w-visual", type=float, default=1.0)
    parser.add_argument("--max-per-bucket", type=int, default=0)
    parser.add_argument("--random-pool-size", type=int, default=0)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    workspace_root = resolve_workspace_root(args.workspace_root)
    epub_path = Path(args.epub).expanduser().resolve()
    scripts_dir = SCRIPTS_DIR
    required_scripts = (
        "epub_extract.py",
        "chapter_ranker.py",
        "image_prompt_builder.py",
        "notebooklm_batch_plan.py",
    )
    missing = [name for name in required_scripts if not (scripts_dir / name).exists()]
    if missing:
        raise SystemExit(f"Missing local pipeline scripts: {', '.join(missing)}")

    issue_label = args.issue_label or detect_issue_label(epub_path)
    extract_out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else workspace_root / "data" / "notebooklm_pipeline" / issue_label
    )

    extract_cmd = [
        "python3",
        str(scripts_dir / "epub_extract.py"),
        "--epub",
        str(epub_path),
        "--min-chars",
        "500",
        "--out-dir",
        str(extract_out_dir),
    ]
    if args.issue_label:
        extract_cmd += ["--issue-label", args.issue_label]

    step1 = run(extract_cmd)
    out_dir = Path(step1["out_dir"])

    rank_cmd = [
        "python3",
        str(scripts_dir / "chapter_ranker.py"),
        "--input",
        str(out_dir / "chapters.json"),
        "--top-n",
        str(args.top_n),
        "--batch-size",
        str(args.batch_size),
        "--select-mode",
        args.select_mode,
        "--w-len",
        str(args.w_len),
        "--w-topic",
        str(args.w_topic),
        "--w-visual",
        str(args.w_visual),
        "--max-per-bucket",
        str(args.max_per_bucket),
        "--random-pool-size",
        str(args.random_pool_size),
    ]
    if args.seed is not None:
        rank_cmd += ["--seed", str(args.seed)]

    step2 = run(rank_cmd)
    ranked_path = step2["output"]

    step3 = run(
        [
            "python3",
            str(scripts_dir / "image_prompt_builder.py"),
            "--input",
            ranked_path,
        ]
    )

    step4 = run(
        [
            "python3",
            str(scripts_dir / "notebooklm_batch_plan.py"),
            "--input",
            ranked_path,
        ]
    )

    print(
        json.dumps(
            {
                "status": "ok",
                "issue_label": step1["issue_label"],
                "workspace_root": str(workspace_root),
                "out_dir": str(out_dir),
                "steps": {
                    "extract": step1,
                    "rank": step2,
                    "prompts": step3,
                    "batches": step4,
                },
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
