#!/usr/bin/env python3
"""Thin orchestrator for book -> chapter menu -> artifact generation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from bookflow.adapters.chapter_menu import ChapterMenuAdapter
from bookflow.adapters.guarded_gen import GuardedGeneratorAdapter
from bookflow.adapters.notebooklm_sources import resolve_source_ids
from bookflow.adapters.runner import AdapterError, run_command, run_json_any
from bookflow.adapters.telegram_fetch import TelegramFetchAdapter
from bookflow.core.config import (
    BOOKFLOW_TMP_DIRNAME,
    DEFAULT_BOOKFLOW_CONFIG,
    DEFAULT_TELEGRAM_SESSION_PATH,
    DEFAULT_WORKSPACE_ROOT,
    ENV_BOOKFLOW_CONFIG,
    ENV_TELEGRAM_SESSION,
    ENV_WORKSPACE_ROOT,
    EVENTS_FILENAME,
    MANIFEST_FILENAME,
)
from bookflow.core.io import append_event, write_manifest
from bookflow.core.models import ArtifactRecord, RunManifest
from bookflow.core.state_machine import (
    STATE_AWAITING_CHAPTER_SELECTION,
    STATE_COMPLETED,
    STATE_FAILED,
    STATE_FETCHED,
    STATE_GENERATING,
    STATE_PARTIAL,
    STATE_PREPARED,
    transition,
)
from bookflow.store import BookflowStore


@dataclass
class AssetIdentity:
    asset_id: str
    asset_hash: str
    asset_kind: str
    asset_ref: str
    book_title: str


def _parse_ids(raw: str) -> List[str]:
    out: List[str] = []
    for part in (raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        out.append(str(int(token)) if token.isdigit() else token)
    return out


def _normalize_plan(raw: str) -> List[str]:
    alias = {
        "podcast": "audio",
        "podcasts": "audio",
        "slide": "slides",
        "slide_deck": "slides",
        "deck": "slides",
        "infographics": "infographic",
    }
    out: List[str] = []
    for part in (raw or "").split(","):
        token = part.strip().lower()
        if not token:
            continue
        normalized = alias.get(token, token)
        if normalized not in out:
            out.append(normalized)
    return out


def _load_defaults(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        return {}
    data = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a JSON object: {config_path}")
    return data


def _resolve_config_path(raw: str, workspace_root: Path) -> Path:
    explicit = raw.strip() or os.environ.get(ENV_BOOKFLOW_CONFIG, "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    return (workspace_root / DEFAULT_BOOKFLOW_CONFIG).resolve()


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


def _workspace_root(raw: str, defaults: Dict[str, Any]) -> Path:
    value = _str_choice(raw, os.environ.get(ENV_WORKSPACE_ROOT, ""), defaults.get("workspace_root"), DEFAULT_WORKSPACE_ROOT)
    if value:
        return Path(value).expanduser().resolve()
    return Path.cwd().resolve()


def _resolve_tg_session_file(explicit: str, defaults: Dict[str, Any]) -> str:
    value = _str_choice(explicit, os.environ.get(ENV_TELEGRAM_SESSION, ""), defaults.get("tg_session_file"), DEFAULT_TELEGRAM_SESSION_PATH)
    return str(Path(value).expanduser())


def _new_run_id() -> str:
    return f"bookflow-{uuid.uuid4()}"


def _save_manifest(manifest: RunManifest, manifest_path: Path) -> None:
    write_manifest(manifest_path, manifest)


def _record_stage(manifest: RunManifest, manifest_path: Path, events_path: Path, stage: str, payload: Dict[str, Any]) -> None:
    manifest.stages[stage] = payload
    _save_manifest(manifest, manifest_path)
    append_event(events_path, stage, payload)


def _record_error(manifest: RunManifest, message: str) -> None:
    manifest.errors.append(message)


_META_TITLE_PATTERNS = [
    r"目录", r"目次", r"自\s*序", r"前言", r"后记", r"附录", r"参考书目", r"推荐", r"纪事", r"读法", r"导读", r"出版说明", r"大历史观", r"神宗实录", r"欢呼", r"倒彩",
]


def _is_meta_title(title: str) -> bool:
    t = (title or "").strip()
    if not t:
        return False
    return any(re.search(pat, t, re.I) for pat in _META_TITLE_PATTERNS)


def _humanize_title(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return ""
    # remove long ALL-CAPS transliteration tails like "... LI ZHI ..."
    t = re.sub(r"\s+[A-Z][A-Z0-9\s\-—]{10,}$", "", t).strip()
    t = re.sub(r"\s{2,}", " ", t)
    return t


def _estimate_read_minutes(char_count: int) -> int:
    return max(3, int(round(max(char_count, 0) / 450)))


def _build_chapter_selection_guide(manifest: RunManifest) -> Dict[str, Any]:
    options: List[Dict[str, Any]] = []
    for item in manifest.menu:
        char_count = int(item.char_count or 0)
        options.append(
            {
                "chapter_id": item.chapter_id,
                "title": _humanize_title(item.title),
                "score": item.score,
                "char_count": char_count,
                "est_read_minutes": _estimate_read_minutes(char_count),
                "is_meta": _is_meta_title(item.title),
            }
        )

    core = [x for x in options if not x["is_meta"]]
    pool = core or options

    def _ids(rows: List[Dict[str, Any]], n: int) -> List[str]:
        return [x["chapter_id"] for x in rows[:n] if str(x.get("chapter_id", "")).strip()]

    def _ordered(ids: List[str]) -> List[str]:
        def _key(v: str):
            s = str(v).strip()
            if s.isdigit():
                return (0, int(s))
            return (1, s)

        return sorted(ids, key=_key)

    quick = _ids(pool, 2)
    standard = _ids(pool, 3)
    deep = _ids(pool, 5)

    presets = [
        {"name": "快速版（2章）", "chapter_ids": quick, "ordered_chapter_ids": _ordered(quick)},
        {"name": "标准版（3章）", "chapter_ids": standard, "ordered_chapter_ids": _ordered(standard)},
        {"name": "深读版（5章）", "chapter_ids": deep, "ordered_chapter_ids": _ordered(deep)},
    ]

    return {
        "options": options,
        "presets": presets,
        "reply_hint": "Reply with chapter IDs, e.g. 11,10,9 (or ordered: 9,10,11)",
    }


def _extract_infographic_artifacts(stage_payload: Dict[str, Any]) -> List[ArtifactRecord]:
    records: List[ArtifactRecord] = []

    generated = None
    if isinstance(stage_payload.get("steps"), dict):
        generated = stage_payload["steps"].get("generate")

    if isinstance(generated, dict):
        artifacts = generated.get("artifacts")
        if isinstance(artifacts, list):
            for row in artifacts:
                if isinstance(row, dict):
                    row = dict(row)
                    row.setdefault("artifact_type", "infographic")
                    records.append(ArtifactRecord.from_dict(row))

    return records


def _extract_non_infographic_artifacts(stage_payload: Dict[str, Any]) -> List[ArtifactRecord]:
    records: List[ArtifactRecord] = []
    attempts = stage_payload.get("attempts")
    if isinstance(attempts, list):
        for row in attempts:
            if isinstance(row, dict):
                records.append(ArtifactRecord.from_dict(row))
    return records


def _final_generation_state(manifest: RunManifest) -> str:
    inf = manifest.stages.get("infographic")
    non_inf = manifest.stages.get("non_infographic")

    statuses: List[str] = []
    for stage in (inf, non_inf):
        if isinstance(stage, dict):
            status = stage.get("status")
            if isinstance(status, str) and status.strip():
                statuses.append(status.strip().lower())

    if not statuses:
        return STATE_FAILED

    success_states = {"ok", "completed", "prepared", "dry_run_ok"}
    partial_states = {"partial", "degraded"}

    if all(status in success_states for status in statuses):
        return STATE_COMPLETED
    if any(status in success_states for status in statuses) and any(
        status in partial_states or status == "failed" for status in statuses
    ):
        return STATE_PARTIAL
    if any(status in partial_states for status in statuses):
        return STATE_PARTIAL
    return STATE_FAILED


def _sha256_text(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_asset_identity(book_title: str, epub: str, ranked_json: str) -> Optional[AssetIdentity]:
    # DB object key should be stable by EPUB content hash whenever EPUB is available.
    epub_path_raw = epub.strip()
    if epub_path_raw:
        epub_path = Path(epub_path_raw).expanduser().resolve()
        asset_hash = _sha256_file(epub_path) if epub_path.exists() else _sha256_text(str(epub_path))
        return AssetIdentity(
            asset_id=asset_hash,
            asset_hash=asset_hash,
            asset_kind="epub",
            asset_ref=str(epub_path),
            book_title=book_title.strip(),
        )

    ranked = ranked_json.strip()
    if ranked:
        ranked_path = Path(ranked).expanduser().resolve()
        asset_hash = _sha256_file(ranked_path) if ranked_path.exists() else _sha256_text(str(ranked_path))
        return AssetIdentity(
            asset_id=asset_hash,
            asset_hash=asset_hash,
            asset_kind="ranked_json",
            asset_ref=str(ranked_path),
            book_title=book_title.strip(),
        )

    title = book_title.strip()
    if title:
        normalized = " ".join(title.lower().split())
        asset_hash = _sha256_text(normalized)
        return AssetIdentity(
            asset_id=asset_hash,
            asset_hash=asset_hash,
            asset_kind="title",
            asset_ref=title,
            book_title=title,
        )

    return None


def _sync_store_run(
    store: BookflowStore,
    manifest: RunManifest,
    asset: Optional[AssetIdentity],
) -> None:
    if asset:
        store.upsert_asset(
            asset_id=asset.asset_id,
            asset_hash=asset.asset_hash,
            asset_kind=asset.asset_kind,
            asset_ref=asset.asset_ref,
            book_title=asset.book_title,
        )

    store.upsert_run(
        run_id=manifest.run_id,
        asset_id=asset.asset_id if asset else None,
        status=manifest.status,
        workspace_root=manifest.workspace_root,
        plan=manifest.plan,
        book_title=manifest.book_title,
        ranked_json=manifest.ranked_json,
        notebook_strategy=manifest.notebook_strategy,
        active_notebook_id=manifest.notebook_id,
        object_notebook_id=manifest.object_notebook_id,
        run_notebook_id=manifest.run_notebook_id,
        selected_chapter_ids=manifest.selected_chapter_ids,
        selected_source_ids=manifest.selected_source_ids,
        errors=manifest.errors,
        created_at=manifest.created_at,
        updated_at=manifest.updated_at,
    )


def _sync_store_sources(store: BookflowStore, manifest: RunManifest) -> None:
    store.replace_run_sources(
        run_id=manifest.run_id,
        chapter_ids=manifest.selected_chapter_ids,
        source_map=manifest.source_map,
        selected_source_ids=manifest.selected_source_ids,
    )


def _sync_store_artifacts(store: BookflowStore, manifest: RunManifest) -> None:
    store.replace_artifacts(run_id=manifest.run_id, artifacts=manifest.artifacts)


def _extract_notebook_id(payload: Any) -> str:
    queue: List[Any] = [payload]
    while queue:
        current = queue.pop(0)
        if isinstance(current, dict):
            for key in ("notebook_id", "notebookId", "id"):
                value = current.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            for value in current.values():
                if isinstance(value, (dict, list)):
                    queue.append(value)
        elif isinstance(current, list):
            for value in current:
                if isinstance(value, (dict, list)):
                    queue.append(value)
    return ""


def _safe_notebook_title(raw: str, fallback: str) -> str:
    cleaned = " ".join((raw or "").split()).strip()
    if not cleaned:
        cleaned = fallback
    return cleaned[:96]


def _parse_notebook_id_from_text(raw: str) -> str:
    text = raw or ""
    m = re.search(r"\bID:\s*([0-9a-fA-F-]{36})\b", text)
    if m:
        return m.group(1)

    m = re.search(r"\b([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\b", text)
    if m:
        return m.group(1)

    return ""


def _create_notebook(*, profile: str, title: str) -> str:
    # Compatibility: some nlm versions support --json, others don't.
    try:
        payload = run_json_any(["nlm", "notebook", "create", title, "--json", "--profile", profile], timeout=300)
        notebook_id = _extract_notebook_id(payload)
        if notebook_id:
            return notebook_id
    except AdapterError:
        pass

    proc = run_command(["nlm", "notebook", "create", title, "--profile", profile], timeout=300)
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    if proc.returncode != 0:
        raise AdapterError(
            json.dumps(
                {
                    "cmd": ["nlm", "notebook", "create", title, "--profile", profile],
                    "returncode": proc.returncode,
                    "stdout": stdout[-1200:],
                    "stderr": stderr[-1200:],
                },
                ensure_ascii=False,
            )
        )

    notebook_id = _parse_notebook_id_from_text(f"{stdout}\n{stderr}")
    if not notebook_id:
        raise AdapterError(f"Unable to parse notebook id from create output: {(stdout + stderr)[-1200:]}")
    return notebook_id


def _resolve_notebooks_for_run(
    *,
    store: BookflowStore,
    run_id: str,
    strategy: str,
    profile: str,
    asset: Optional[AssetIdentity],
    object_notebook_id: str,
    run_notebook_id: str,
) -> Dict[str, str]:
    object_id = object_notebook_id.strip()
    run_id_value = run_notebook_id.strip()

    if strategy in {"object", "hybrid"}:
        if not asset:
            raise AdapterError("Asset identity is required for object notebook strategy")
        if not object_id:
            object_id = store.get_object_notebook_id(asset.asset_id) or ""
        if not object_id:
            title = _safe_notebook_title(asset.book_title, f"bookflow-object-{asset.asset_id[:8]}")
            object_id = _create_notebook(profile=profile, title=title)
        store.upsert_object_notebook(asset_id=asset.asset_id, notebook_id=object_id, profile=profile)

    if strategy in {"run", "hybrid"}:
        if not run_id_value:
            title = _safe_notebook_title(f"bookflow-run-{run_id}", "bookflow-run")
            run_id_value = _create_notebook(profile=profile, title=title)
        store.upsert_run_notebook(run_id=run_id, notebook_id=run_id_value, profile=profile)

    active = ""
    if strategy == "object":
        active = object_id
    elif strategy == "run":
        active = run_id_value
    else:
        active = run_id_value or object_id

    if not active:
        raise AdapterError("Unable to resolve active notebook id")

    return {
        "active_notebook_id": active,
        "object_notebook_id": object_id,
        "run_notebook_id": run_id_value,
    }


def _build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Book to artifact orchestrator")
    ap.add_argument("--book-title", default="", help="Book title for Telegram fetch")
    ap.add_argument("--config", default="", help="JSON config path (default: skills/book-to-artifact/config/defaults.json)")
    ap.add_argument("--epub", default="")
    ap.add_argument("--ranked-json", default="")
    ap.add_argument("--object-notebook-id", default="")
    ap.add_argument("--run-notebook-id", default="")
    ap.add_argument("--notebook-strategy", choices=["run", "object", "hybrid"], default="run")
    ap.add_argument("--chapter-ids", default="", help="comma-separated IDs")
    ap.add_argument("--artifact-plan", default="infographic,slides,report,audio")
    ap.add_argument("--profile", default="default")
    ap.add_argument("--workspace-root", default="")
    ap.add_argument("--run-id", default="")

    ap.add_argument("--tg-bot", default="@BookLib7890Bot")
    ap.add_argument("--tg-session-file", default="")
    ap.add_argument("--tg-output-root", default="")

    ap.add_argument("--issue-label", default="")
    ap.add_argument("--out-dir", default="")
    ap.add_argument("--top-n", type=int, default=6)
    ap.add_argument("--batch-size", type=int, default=3)
    ap.add_argument("--select-mode", choices=["score", "random"], default="score")
    ap.add_argument("--allow-random", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-per-bucket", type=int, default=0)
    ap.add_argument("--random-pool-size", type=int, default=12)
    ap.add_argument("--w-len", type=float, default=0.2)
    ap.add_argument("--w-topic", type=float, default=0.45)
    ap.add_argument("--w-visual", type=float, default=0.35)

    ap.add_argument("--poll-seconds", type=int, default=8)
    ap.add_argument("--max-polls", type=int, default=36)
    ap.add_argument("--chars-per-chapter", type=int, default=6000)
    ap.add_argument("--max-chapters", type=int, default=0)
    ap.add_argument("--infographic-out-dir", default="")

    ap.add_argument("--publish-after-generate", dest="publish_after_generate", action="store_true", default=True)
    ap.add_argument("--no-publish-after-generate", dest="publish_after_generate", action="store_false")
    ap.add_argument("--obsidian-vault-path", default="")
    ap.add_argument("--notion-data-source-id", default="")

    # Google Drive publishing for large artifacts (default resolved from config/env)
    ap.add_argument("--gdrive-enabled", dest="gdrive_enabled", action="store_true")
    ap.add_argument("--no-gdrive", dest="gdrive_enabled", action="store_false")
    ap.set_defaults(gdrive_enabled=None)
    ap.add_argument("--gdrive-folder-id", default="")
    return ap


def main() -> None:
    args = _build_arg_parser().parse_args()

    bootstrap_root = Path(
        _str_choice(args.workspace_root, os.environ.get(ENV_WORKSPACE_ROOT, "")) or os.getcwd()
    ).expanduser().resolve()
    config_path = _resolve_config_path(args.config, bootstrap_root)
    defaults = _load_defaults(config_path)

    workspace_root = _workspace_root(args.workspace_root, defaults)
    if not (args.config.strip() or os.environ.get(ENV_BOOKFLOW_CONFIG, "").strip()):
        implicit_config = (workspace_root / DEFAULT_BOOKFLOW_CONFIG).resolve()
        if implicit_config != config_path:
            config_path = implicit_config
            defaults = _load_defaults(config_path)
            workspace_root = _workspace_root(args.workspace_root, defaults)

    notion_data_source_id = _str_choice(
        args.notion_data_source_id,
        os.environ.get("NOTION_DATA_SOURCE_ID", ""),
        defaults.get("notion_data_source_id"),
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

    plan = _normalize_plan(args.artifact_plan)
    notebook_strategy = args.notebook_strategy.strip() or "run"

    run_id = args.run_id.strip() or _new_run_id()
    run_dir = workspace_root / "tmp" / BOOKFLOW_TMP_DIRNAME / run_id
    manifest_path = run_dir / MANIFEST_FILENAME
    events_path = run_dir / EVENTS_FILENAME

    epub = args.epub.strip()
    ranked_json = args.ranked_json.strip()
    asset_identity = _resolve_asset_identity(args.book_title, epub, ranked_json)

    store = BookflowStore()
    manifest = RunManifest(
        run_id=run_id,
        workspace_root=str(workspace_root),
        plan=plan,
        book_title=args.book_title.strip(),
        notebook_strategy=notebook_strategy,
        object_notebook_id=args.object_notebook_id.strip(),
        run_notebook_id=args.run_notebook_id.strip(),
    )
    if notebook_strategy == "object":
        manifest.notebook_id = manifest.object_notebook_id
    else:
        manifest.notebook_id = manifest.run_notebook_id or manifest.object_notebook_id

    _save_manifest(manifest, manifest_path)
    append_event(events_path, "run_started", {"run_id": run_id, "plan": plan, "notebook_strategy": notebook_strategy})
    _sync_store_run(store, manifest, asset_identity)

    try:
        chapter_menu = ChapterMenuAdapter(workspace_root)

        if not ranked_json and not epub and args.book_title.strip():
            fetcher = TelegramFetchAdapter(workspace_root)
            fetch_payload = fetcher.fetch(
                book_title=args.book_title,
                tg_bot=args.tg_bot,
                tg_session_file=_resolve_tg_session_file(args.tg_session_file, defaults),
                tg_output_root=args.tg_output_root,
            )
            _record_stage(manifest, manifest_path, events_path, "fetch", fetch_payload)
            _sync_store_run(store, manifest, asset_identity)

            if fetch_payload.get("status") != "ok":
                transition(manifest, STATE_FAILED)
                _record_error(manifest, "telegram fetch stage returned non-ok status")
                _save_manifest(manifest, manifest_path)
                _sync_store_run(store, manifest, asset_identity)
                print(json.dumps(manifest.to_dict(), ensure_ascii=False))
                return

            transition(manifest, STATE_FETCHED)
            epub = str(fetch_payload.get("downloaded_path") or "").strip()
            asset_identity = _resolve_asset_identity(manifest.book_title, epub, ranked_json)
            _save_manifest(manifest, manifest_path)
            _sync_store_run(store, manifest, asset_identity)

        if not ranked_json and not epub:
            raise SystemExit("Provide --ranked-json or --epub, or use --book-title to fetch.")

        prepare_payload = chapter_menu.prepare(
            epub=epub,
            ranked_json=ranked_json,
            top_n=args.top_n,
            batch_size=args.batch_size,
            select_mode=args.select_mode,
            allow_random=args.allow_random,
            seed=args.seed,
            max_per_bucket=args.max_per_bucket,
            random_pool_size=args.random_pool_size,
            w_len=args.w_len,
            w_topic=args.w_topic,
            w_visual=args.w_visual,
            issue_label=args.issue_label,
            out_dir=args.out_dir,
        )
        _record_stage(manifest, manifest_path, events_path, "prepare", prepare_payload)

        transition(manifest, STATE_PREPARED)
        manifest.ranked_json = str(prepare_payload.get("ranked_json") or ranked_json)
        manifest.menu = chapter_menu.parse_menu(prepare_payload.get("menu"))
        asset_identity = _resolve_asset_identity(manifest.book_title, epub, manifest.ranked_json)
        _save_manifest(manifest, manifest_path)
        _sync_store_run(store, manifest, asset_identity)

        if args.chapter_ids:
            chapter_ids = _parse_ids(args.chapter_ids)
        else:
            transition(manifest, STATE_AWAITING_CHAPTER_SELECTION)
            manifest.selected_chapter_ids = []
            selection_guide = _build_chapter_selection_guide(manifest)
            manifest.next_action = "Provide --chapter-ids to continue artifact generation (example: 11,10,9)."
            _record_stage(manifest, manifest_path, events_path, "chapter_selection_guide", selection_guide)
            append_event(events_path, "awaiting_chapter_selection", {"run_id": run_id, "ranked_json": manifest.ranked_json})
            _sync_store_run(store, manifest, asset_identity)
            print(json.dumps(manifest.to_dict(), ensure_ascii=False))
            return

        manifest.selected_chapter_ids = chapter_ids
        _save_manifest(manifest, manifest_path)
        _sync_store_run(store, manifest, asset_identity)

        if not chapter_ids:
            transition(manifest, STATE_FAILED)
            _record_error(manifest, "No chapter IDs resolved")
            _save_manifest(manifest, manifest_path)
            _sync_store_run(store, manifest, asset_identity)
            print(json.dumps(manifest.to_dict(), ensure_ascii=False))
            return

        if not plan:
            transition(manifest, STATE_COMPLETED)
            _save_manifest(manifest, manifest_path)
            _sync_store_run(store, manifest, asset_identity)
            print(json.dumps(manifest.to_dict(), ensure_ascii=False))
            return

        notebook_ids = _resolve_notebooks_for_run(
            store=store,
            run_id=manifest.run_id,
            strategy=notebook_strategy,
            profile=args.profile,
            asset=asset_identity,
            object_notebook_id=manifest.object_notebook_id,
            run_notebook_id=manifest.run_notebook_id,
        )
        manifest.notebook_id = notebook_ids["active_notebook_id"]
        manifest.object_notebook_id = notebook_ids["object_notebook_id"]
        manifest.run_notebook_id = notebook_ids["run_notebook_id"]
        _record_stage(
            manifest,
            manifest_path,
            events_path,
            "notebook_resolution",
            {
                "strategy": notebook_strategy,
                "active_notebook_id": manifest.notebook_id,
                "object_notebook_id": manifest.object_notebook_id,
                "run_notebook_id": manifest.run_notebook_id,
            },
        )
        _sync_store_run(store, manifest, asset_identity)

        transition(manifest, STATE_GENERATING)
        _save_manifest(manifest, manifest_path)
        _sync_store_run(store, manifest, asset_identity)

        non_infographic_plan = [item for item in plan if item != "infographic"]

        if "infographic" in plan:
            inf_payload = chapter_menu.generate_infographics(
                ranked_json=manifest.ranked_json,
                notebook_id=manifest.notebook_id,
                chapter_ids=chapter_ids,
                profile=args.profile,
                poll_seconds=args.poll_seconds,
                max_polls=args.max_polls,
                chars_per_chapter=args.chars_per_chapter,
                max_chapters=args.max_chapters,
                infographic_out_dir=args.infographic_out_dir,
                publish_after_generate=args.publish_after_generate,
                obsidian_vault_path=args.obsidian_vault_path,
                notion_data_source_id=notion_data_source_id,
                gdrive_enabled=gdrive_enabled,
                gdrive_folder_id=gdrive_folder_id,
            )
            _record_stage(manifest, manifest_path, events_path, "infographic", inf_payload)
            manifest.artifacts.extend(_extract_infographic_artifacts(inf_payload))
            _save_manifest(manifest, manifest_path)
            _sync_store_artifacts(store, manifest)
            _sync_store_run(store, manifest, asset_identity)

        if non_infographic_plan:
            cached_source_map: Dict[str, str] = {}
            if asset_identity:
                cached_source_map = store.get_cached_source_map(
                    asset_id=asset_identity.asset_id,
                    notebook_id=manifest.notebook_id,
                    chapter_ids=chapter_ids,
                )

            unresolved_chapter_ids = [
                chapter_id for chapter_id in chapter_ids if not str(cached_source_map.get(chapter_id, "")).strip()
            ]

            live_source_map: Dict[str, str] = {}
            if unresolved_chapter_ids:
                source_resolution = resolve_source_ids(
                    notebook_id=manifest.notebook_id,
                    profile=args.profile,
                    chapter_ids=unresolved_chapter_ids,
                )
                for chapter_id in unresolved_chapter_ids:
                    source_id = str(source_resolution.source_map.get(chapter_id, "")).strip()
                    if source_id:
                        live_source_map[chapter_id] = source_id

            effective_source_map = dict(cached_source_map)
            effective_source_map.update(live_source_map)

            ordered_source_ids: List[str] = []
            seen_source_ids = set()
            missing_chapter_ids: List[str] = []
            for chapter_id in chapter_ids:
                source_id = str(effective_source_map.get(chapter_id, "")).strip()
                if not source_id:
                    missing_chapter_ids.append(chapter_id)
                    continue
                if source_id in seen_source_ids:
                    continue
                seen_source_ids.add(source_id)
                ordered_source_ids.append(source_id)

            manifest.source_map = effective_source_map
            manifest.selected_source_ids = ordered_source_ids
            _record_stage(
                manifest,
                manifest_path,
                events_path,
                "source_resolution",
                {
                    "db_first": True,
                    "cached_hits": [chapter_id for chapter_id in chapter_ids if chapter_id in cached_source_map],
                    "live_lookup_chapter_ids": unresolved_chapter_ids,
                    "selected_source_ids": ordered_source_ids,
                    "missing_chapter_ids": missing_chapter_ids,
                    "source_map_preview": {
                        chapter_id: effective_source_map.get(chapter_id, "") for chapter_id in chapter_ids
                    },
                },
            )
            _sync_store_sources(store, manifest)
            _sync_store_run(store, manifest, asset_identity)

            if not ordered_source_ids:
                _record_error(manifest, "No source IDs resolved for selected chapter IDs")
                transition(manifest, STATE_FAILED if "infographic" not in plan else STATE_PARTIAL)
                _save_manifest(manifest, manifest_path)
                _sync_store_run(store, manifest, asset_identity)
                print(json.dumps(manifest.to_dict(), ensure_ascii=False))
                return

            guarded = GuardedGeneratorAdapter(workspace_root)
            state_file = run_dir / "guarded_state.json"
            guarded_events = run_dir / "guarded_events.jsonl"
            non_inf_payload = guarded.generate(
                notebook_id=manifest.notebook_id,
                source_ids=ordered_source_ids,
                profile=args.profile,
                artifact_plan=non_infographic_plan,
                max_success=len(non_infographic_plan),
                poll_seconds=args.poll_seconds,
                max_polls=args.max_polls,
                state_file=state_file,
                events_file=guarded_events,
            )
            _record_stage(manifest, manifest_path, events_path, "non_infographic", non_inf_payload)
            manifest.artifacts.extend(_extract_non_infographic_artifacts(non_inf_payload))
            _save_manifest(manifest, manifest_path)
            _sync_store_artifacts(store, manifest)
            _sync_store_run(store, manifest, asset_identity)

        transition(manifest, _final_generation_state(manifest))
        _save_manifest(manifest, manifest_path)
        _sync_store_run(store, manifest, asset_identity)
        print(json.dumps(manifest.to_dict(), ensure_ascii=False))

    except (AdapterError, RuntimeError, ValueError) as exc:
        _record_error(manifest, str(exc))
        try:
            transition(manifest, STATE_FAILED)
        except ValueError:
            manifest.status = STATE_FAILED
            manifest.touch()
        _save_manifest(manifest, manifest_path)
        _sync_store_run(store, manifest, asset_identity)
        append_event(events_path, "run_failed", {"error": str(exc)})
        print(json.dumps(manifest.to_dict(), ensure_ascii=False))
    finally:
        store.close()


if __name__ == "__main__":
    main()
