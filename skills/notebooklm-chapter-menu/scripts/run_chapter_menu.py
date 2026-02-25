#!/usr/bin/env python3
"""
Deterministic chapter-menu runner for NotebookLM chapter infographic flow.

Behavior:
1) Prepare chapter artifacts (extract/rank/prompts/batches) from EPUB, unless --ranked-json is provided.
2) Emit a compact chapter menu from selected_chapters.
3) Optionally generate selected chapter infographics when --notebook-id is provided.
4) Optionally publish generation outputs to Obsidian/Notion.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Dict, List


SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent
PIPELINE_DIR = SCRIPT_DIR / "pipeline"
DEFAULT_CONFIG_PATH = SKILL_ROOT / "config" / "defaults.json"
ENV_CONFIG_PATH = "NOTEBOOKLM_CHAPTER_MENU_CONFIG"


def _load_defaults(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        return {}
    data = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a JSON object: {config_path}")
    return data


def _str_choice(*values: Any) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _bool_choice(*values: Any, fallback: bool = False) -> bool:
    for value in values:
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "on"}:
            return True
        if isinstance(value, str) and value.strip().lower() in {"0", "false", "no", "off"}:
            return False
    return fallback


def resolve_output_root(workspace_root: str) -> Path:
    raw = (
        workspace_root.strip()
        or os.environ.get("NOTEBOOKLM_CHAPTER_MENU_ROOT", "").strip()
        or str(SKILL_ROOT)
    )
    return Path(raw).expanduser().resolve()


def _run_json(cmd: List[str], timeout: int) -> Dict[str, Any]:
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    stdout = (p.stdout or "").strip()
    stderr = (p.stderr or "").strip()

    if p.returncode != 0:
        raise RuntimeError(
            json.dumps(
                {
                    "cmd": cmd,
                    "returncode": p.returncode,
                    "stdout": stdout[-1200:],
                    "stderr": stderr[-1200:],
                },
                ensure_ascii=False,
            )
        )

    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("{") or line.startswith("["):
            try:
                parsed = json.loads(line)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                continue

    raise RuntimeError(f"No JSON output returned from command: {cmd}")


def _parse_chapter_ids(raw: str) -> List[str]:
    out = []
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        out.append(str(int(token)) if token.isdigit() else token)
    return out


def _build_menu(ranked_json_path: Path) -> List[Dict[str, Any]]:
    data = json.loads(ranked_json_path.read_text(encoding="utf-8"))
    selected = data.get("selected_chapters", [])
    menu = []
    for chapter in selected:
        menu.append(
            {
                "chapter_id": str(chapter.get("chapter_id", "")).strip(),
                "title": chapter.get("title", ""),
                "score": chapter.get("score"),
                "char_count": chapter.get("char_count"),
            }
        )
    return menu


def main() -> None:
    parser = argparse.ArgumentParser(description="NotebookLM chapter-menu runner")
    parser.add_argument("--epub", default="", help="EPUB path for prep step")
    parser.add_argument("--config", default="", help="JSON config path (default: skills/notebooklm-chapter-menu/config/defaults.json)")
    parser.add_argument("--ranked-json", default="", help="Skip prep and use existing chapters_ranked.json")
    parser.add_argument("--issue-label", default="")
    parser.add_argument("--out-dir", default="")
    parser.add_argument(
        "--workspace-root",
        default="",
        help="Base output root. Defaults to NOTEBOOKLM_CHAPTER_MENU_ROOT or this skill directory.",
    )

    parser.add_argument("--top-n", type=int, default=6)
    parser.add_argument("--batch-size", type=int, default=3)
    parser.add_argument("--select-mode", choices=["score", "random"], default="score")
    parser.add_argument("--allow-random", action="store_true", help="Required when --select-mode random is used")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-per-bucket", type=int, default=1)
    parser.add_argument("--random-pool-size", type=int, default=12)
    parser.add_argument("--w-len", type=float, default=0.2)
    parser.add_argument("--w-topic", type=float, default=0.45)
    parser.add_argument("--w-visual", type=float, default=0.35)

    parser.add_argument("--notebook-id", default="")
    parser.add_argument("--chapter-ids", default="", help="Comma-separated IDs; default uses selected_chapters")
    parser.add_argument("--profile", default="default")
    parser.add_argument("--source-map-json", default="", help="Optional source map JSON or previous run_manifest.json")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--poll-seconds", type=int, default=8)
    parser.add_argument("--max-polls", type=int, default=36)
    parser.add_argument("--chars-per-chapter", type=int, default=6000)
    parser.add_argument("--max-chapters", type=int, default=0)
    parser.add_argument("--infographic-out-dir", default="")

    parser.add_argument("--publish-after-generate", action="store_true")
    parser.add_argument("--obsidian-vault-path", default="")
    parser.add_argument("--obsidian-subdir", default="")
    parser.add_argument("--notion-data-source-id", default="")
    parser.add_argument("--notion-api-key-file", default="")

    # Google Drive publishing for large artifacts (resolved from config/env unless overridden)
    parser.add_argument("--gdrive-enabled", dest="gdrive_enabled", action="store_true")
    parser.add_argument("--no-gdrive", dest="gdrive_enabled", action="store_false")
    parser.set_defaults(gdrive_enabled=None)
    parser.add_argument("--gdrive-folder-id", default="")
    parser.add_argument("--gdrive-client-secrets", default="")
    parser.add_argument("--gdrive-token-file", default="")

    args = parser.parse_args()

    config_path = Path(
        _str_choice(args.config, os.environ.get(ENV_CONFIG_PATH, ""))
        or str(DEFAULT_CONFIG_PATH)
    ).expanduser().resolve()
    defaults = _load_defaults(config_path)

    obsidian_vault_path = _str_choice(
        args.obsidian_vault_path,
        os.environ.get("OBSIDIAN_VAULT_PATH", ""),
        defaults.get("obsidian_vault_path"),
    )
    obsidian_subdir = _str_choice(
        args.obsidian_subdir,
        os.environ.get("OBSIDIAN_SUBDIR", ""),
        defaults.get("obsidian_subdir"),
        "NotebookLM/Infographics",
    )
    notion_data_source_id = _str_choice(
        args.notion_data_source_id,
        os.environ.get("NOTION_DATA_SOURCE_ID", ""),
        defaults.get("notion_data_source_id"),
    )
    notion_api_key_file = _str_choice(
        args.notion_api_key_file,
        os.environ.get("NOTION_API_KEY_FILE", ""),
        defaults.get("notion_api_key_file"),
        "~/.config/notion/api_key",
    )
    gdrive_enabled = _bool_choice(
        args.gdrive_enabled,
        os.environ.get("GDRIVE_ENABLED", ""),
        defaults.get("gdrive_enabled"),
        fallback=True,
    )
    gdrive_folder_id = _str_choice(
        args.gdrive_folder_id,
        os.environ.get("GDRIVE_FOLDER_ID", ""),
        defaults.get("gdrive_folder_id"),
    )
    gdrive_client_secrets = _str_choice(
        args.gdrive_client_secrets,
        os.environ.get("GDRIVE_CLIENT_SECRETS", ""),
        defaults.get("gdrive_client_secrets"),
    )
    gdrive_token_file = _str_choice(
        args.gdrive_token_file,
        os.environ.get("GDRIVE_TOKEN_FILE", ""),
        defaults.get("gdrive_token_file"),
    )

    if args.select_mode == "random" and not args.allow_random:
        raise SystemExit("--select-mode random requires --allow-random")

    output_root = resolve_output_root(_str_choice(args.workspace_root, defaults.get("workspace_root")))
    pipeline_dir = PIPELINE_DIR
    if not (pipeline_dir / "run_image_first_pipeline.py").exists():
        raise SystemExit(f"Pipeline scripts not found under skill: {pipeline_dir}")

    infographic_out_dir = args.infographic_out_dir or str(output_root / "tmp" / "notebooklm_poc" / "chapter-menu")

    result: Dict[str, Any] = {
        "status": "started",
        "workspace_root": str(output_root),
        "config_path": str(config_path),
        "ranked_json": None,
        "menu": [],
        "steps": {},
    }

    ranked_json_path: Path
    if args.ranked_json:
        ranked_json_path = Path(args.ranked_json).expanduser().resolve()
        if not ranked_json_path.exists():
            raise SystemExit(f"ranked-json not found: {ranked_json_path}")
    else:
        if not args.epub:
            raise SystemExit("Either --epub or --ranked-json is required")

        prep_cmd = [
            "python3",
            str(pipeline_dir / "run_image_first_pipeline.py"),
            "--epub",
            args.epub,
            "--top-n",
            str(args.top_n),
            "--batch-size",
            str(args.batch_size),
            "--select-mode",
            args.select_mode,
            "--seed",
            str(args.seed),
            "--max-per-bucket",
            str(args.max_per_bucket),
            "--random-pool-size",
            str(args.random_pool_size),
            "--w-len",
            str(args.w_len),
            "--w-topic",
            str(args.w_topic),
            "--w-visual",
            str(args.w_visual),
            "--workspace-root",
            str(output_root),
        ]
        if args.issue_label:
            prep_cmd += ["--issue-label", args.issue_label]
        if args.out_dir:
            prep_cmd += ["--out-dir", args.out_dir]

        prep = _run_json(prep_cmd, timeout=1800)
        result["steps"]["prepare"] = prep

        ranked_output = prep.get("steps", {}).get("rank", {}).get("output")
        if not ranked_output:
            raise RuntimeError("Prep completed but rank output was missing")
        ranked_json_path = Path(ranked_output).expanduser().resolve()

    menu = _build_menu(ranked_json_path)
    result["ranked_json"] = str(ranked_json_path)
    result["menu"] = menu

    if not args.notebook_id:
        result["status"] = "prepared"
        print(json.dumps(result, ensure_ascii=False))
        return

    chapter_ids = _parse_chapter_ids(args.chapter_ids) if args.chapter_ids else [x["chapter_id"] for x in menu if x["chapter_id"]]
    if not chapter_ids:
        raise RuntimeError("No chapter IDs available for generation")

    gen_cmd = [
        "python3",
        str(pipeline_dir / "notebooklm_chapter_infographic_run.py"),
        "--ranked-json",
        str(ranked_json_path),
        "--notebook-id",
        args.notebook_id,
        "--chapter-ids",
        ",".join(chapter_ids),
        "--profile",
        args.profile,
        "--out-dir",
        infographic_out_dir,
        "--poll-seconds",
        str(args.poll_seconds),
        "--max-polls",
        str(args.max_polls),
        "--chars-per-chapter",
        str(args.chars_per_chapter),
        "--max-chapters",
        str(args.max_chapters),
        "--workspace-root",
        str(output_root),
    ]
    if args.source_map_json:
        gen_cmd += ["--source-map-json", args.source_map_json]
    if args.run_id:
        gen_cmd += ["--run-id", args.run_id]

    generated = _run_json(gen_cmd, timeout=7200)
    result["steps"]["generate"] = generated
    result["status"] = "ok"

    if args.publish_after_generate:
        manifest_path = str(generated.get("manifest_path") or "")
        if not manifest_path:
            result["steps"]["publish"] = {
                "status": "failed",
                "error": "manifest_path missing from generate step",
            }
            result["status"] = "partial"
        else:
            pub_cmd = [
                "python3",
                str(pipeline_dir / "notebooklm_publish_run.py"),
                "--config",
                str(config_path),
                "--manifest-json",
                manifest_path,
                "--obsidian-subdir",
                obsidian_subdir,
                "--notion-api-key-file",
                notion_api_key_file,
            ]
            if obsidian_vault_path:
                pub_cmd += ["--obsidian-vault-path", obsidian_vault_path]
            if notion_data_source_id:
                pub_cmd += ["--notion-data-source-id", notion_data_source_id]

            if gdrive_enabled:
                pub_cmd.append("--gdrive-enabled")
            else:
                pub_cmd.append("--no-gdrive")
            if gdrive_folder_id:
                pub_cmd += ["--gdrive-folder-id", gdrive_folder_id]
            if gdrive_client_secrets:
                pub_cmd += ["--gdrive-client-secrets", gdrive_client_secrets]
            if gdrive_token_file:
                pub_cmd += ["--gdrive-token-file", gdrive_token_file]

            publish = _run_json(pub_cmd, timeout=300)
            result["steps"]["publish"] = publish
            if publish.get("status") in ("partial", "failed"):
                result["status"] = "partial"

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
