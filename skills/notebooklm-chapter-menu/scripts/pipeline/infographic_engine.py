#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

SCHEMA_VERSION = "1.0"
SUCCESS_ARTIFACT_STATES = {"completed", "ready", "done", "succeeded", "success"}
FAILED_ARTIFACT_STATES = {"error", "failed"}
TERMINAL_ARTIFACT_STATES = SUCCESS_ARTIFACT_STATES | FAILED_ARTIFACT_STATES
SCRIPTS_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPTS_DIR.parents[1]


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_workspace_root(workspace_root: Optional[str]) -> Path:
    raw = (
        (workspace_root or "").strip()
        or os.environ.get("NOTEBOOKLM_CHAPTER_MENU_ROOT", "").strip()
        or str(SKILL_ROOT)
    )
    return Path(raw).expanduser().resolve()


def default_out_dir(workspace_root: Optional[str], relative_path: str) -> Path:
    return resolve_workspace_root(workspace_root) / relative_path


def parse_csv_ids(raw: str) -> List[str]:
    out: List[str] = []
    for part in (raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        out.append(str(int(token)) if token.isdigit() else token)
    return out


def sanitize_filename(name: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]", "_", name)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:80] or "chapter"


def run(cmd: List[str], timeout: int = 240) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=os.environ.copy())


def is_auth_error(p: subprocess.CompletedProcess) -> bool:
    msg = ((p.stderr or "") + "\n" + (p.stdout or "")).lower()
    keys = [
        "no authentication found",
        "please run: nlm login",
        "authentication expired",
        "profile not found",
    ]
    return any(k in msg for k in keys)


def refresh_auth_from_cdp(profile: str = "default") -> bool:
    login = run(
        [
            "nlm",
            "login",
            "--profile",
            profile,
            "--provider",
            "openclaw",
            "--cdp-url",
            "http://127.0.0.1:18800",
        ],
        timeout=180,
    )
    if login.returncode != 0:
        return False
    chk = run(["nlm", "login", "--check", "--profile", profile], timeout=120)
    return chk.returncode == 0


def is_transient_net_error(p: subprocess.CompletedProcess) -> bool:
    msg = ((p.stderr or "") + "\n" + (p.stdout or "")).lower()
    keys = [
        "unexpected_eof_while_reading",
        "connecterror",
        "connection reset",
        "timed out",
        "temporary failure",
        "network is unreachable",
    ]
    return any(k in msg for k in keys)


def run_nlm(cmd: List[str], timeout: int = 240, profile: str = "default") -> subprocess.CompletedProcess:
    attempts = 3
    last = run(cmd, timeout=timeout)
    if last.returncode == 0:
        return last

    for idx in range(attempts - 1):
        p = last
        if is_auth_error(p):
            if refresh_auth_from_cdp(profile=profile):
                retry = run(cmd, timeout=timeout)
                if retry.returncode == 0:
                    return retry
                last = retry
                continue

        if is_transient_net_error(p):
            time.sleep(2 * (idx + 1))
            retry = run(cmd, timeout=timeout)
            if retry.returncode == 0:
                return retry
            last = retry
            continue

        return p

    return last


def jload(raw: str) -> Optional[Any]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line in reversed(lines):
            if line.startswith("{") or line.startswith("["):
                try:
                    return json.loads(line)
                except Exception:
                    continue
    return None


def items_from_any(js: Any, keys: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
    lookup = tuple(keys or ("items", "results", "data", "artifacts", "sources"))
    if isinstance(js, list):
        return [x for x in js if isinstance(x, dict)]
    if isinstance(js, dict):
        for key in lookup:
            val = js.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        vals = [v for v in js.values() if isinstance(v, dict)]
        if vals:
            return vals
    return []


def is_infographic_item(item: Dict[str, Any]) -> bool:
    raw = json.dumps(item, ensure_ascii=False).lower()
    return (
        "infographic" in raw
        or "infographic" in str(item.get("type", "")).lower()
        or "infographic" in str(item.get("kind", "")).lower()
    )


def source_list(notebook_id: str, profile: str) -> List[Dict[str, Any]]:
    p = run_nlm(["nlm", "source", "list", notebook_id, "--json", "--profile", profile], timeout=180, profile=profile)
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "source list failed")[-600:])
    return items_from_any(jload(p.stdout), ["sources", "items", "results", "data"])


def studio_inf_status(notebook_id: str, profile: str) -> List[Dict[str, Any]]:
    p = run_nlm(
        ["nlm", "studio", "status", notebook_id, "--full", "--json", "--profile", profile],
        timeout=180,
        profile=profile,
    )
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout or "studio status failed")[-600:])
    rows = items_from_any(jload(p.stdout), ["artifacts", "items", "results", "data"])
    return [row for row in rows if is_infographic_item(row)]


def extract_source_id(source_add_stdout: str) -> Optional[str]:
    js = jload(source_add_stdout)
    candidates: List[Any] = []
    if isinstance(js, dict):
        candidates.extend([js.get("id"), js.get("source_id")])
        for key in ("source", "result", "data"):
            block = js.get(key)
            if isinstance(block, dict):
                candidates.extend([block.get("id"), block.get("source_id")])
    elif isinstance(js, list):
        for item in js:
            if isinstance(item, dict):
                candidates.extend([item.get("id"), item.get("source_id")])
    for cand in candidates:
        if isinstance(cand, str) and cand.strip():
            return cand.strip()
    return None


def extract_artifact_id(create_stdout: str, notebook_id: str) -> Optional[str]:
    js = jload(create_stdout)
    candidates: List[Any] = []
    if isinstance(js, dict):
        candidates.extend([js.get("id"), js.get("artifact_id")])
        for key in ("artifact", "result", "data"):
            block = js.get(key)
            if isinstance(block, dict):
                candidates.extend([block.get("id"), block.get("artifact_id")])
    elif isinstance(js, list):
        for item in js:
            if isinstance(item, dict):
                candidates.extend([item.get("id"), item.get("artifact_id")])
    for cand in candidates:
        if isinstance(cand, str) and cand.strip() and cand.strip() != notebook_id:
            return cand.strip()
    return None


def download_supports_profile() -> bool:
    p = run(["nlm", "download", "infographic", "--help"], timeout=40)
    text = (p.stdout or "") + "\n" + (p.stderr or "")
    return "--profile" in text


def download_infographic(
    notebook_id: str,
    artifact_id: str,
    out_path: Path,
    profile: str,
    supports_profile: bool,
    timeout: int = 300,
) -> subprocess.CompletedProcess:
    cmd = [
        "nlm",
        "download",
        "infographic",
        notebook_id,
        "--id",
        artifact_id,
        "--output",
        str(out_path),
        "--no-progress",
    ]
    if supports_profile:
        cmd += ["--profile", profile]
    return run(cmd, timeout=timeout)


def _normalize_chapter(ch: Dict[str, Any], fallback_id: int) -> Dict[str, Any]:
    chapter_id = str(ch.get("chapter_id", fallback_id)).strip() or str(fallback_id)
    return {
        "chapter_id": chapter_id,
        "title": ch.get("title") or f"chapter-{chapter_id}",
        "text": ch.get("text") or "",
        "score": ch.get("score"),
        "char_count": ch.get("char_count"),
    }


def select_chapters(chapters: List[Dict[str, Any]], chapter_ids: Optional[List[str]], max_chapters: int) -> List[Dict[str, Any]]:
    normalized = [_normalize_chapter(ch, idx + 1) for idx, ch in enumerate(chapters)]
    if chapter_ids:
        by_id = {ch["chapter_id"]: ch for ch in normalized}
        picked: List[Dict[str, Any]] = []
        for cid in chapter_ids:
            chapter = by_id.get(cid)
            if chapter is None:
                picked.append({"chapter_id": cid, "title": f"chapter-{cid}", "text": "", "score": None, "char_count": None})
            else:
                picked.append(chapter)
        normalized = picked
    if max_chapters > 0:
        normalized = normalized[:max_chapters]
    return normalized


def _new_source_id_by_diff(before: List[Dict[str, Any]], after: List[Dict[str, Any]]) -> Optional[str]:
    before_ids = {str(x.get("id")) for x in before if x.get("id")}
    candidates = [x for x in after if x.get("id") and str(x.get("id")) not in before_ids]
    if not candidates:
        return None
    # Deterministic tie-break: prefer latest by timestamp-like keys, then id.
    def _sort_key(item: Dict[str, Any]) -> tuple:
        stamp = str(item.get("updated_at") or item.get("created_at") or item.get("timestamp") or "")
        return (stamp, str(item.get("id")))

    best = sorted(candidates, key=_sort_key)[-1]
    return str(best.get("id"))


def _empty_artifact(chapter: Dict[str, Any], source_id: Optional[str], status: str, error: str = "") -> Dict[str, Any]:
    return {
        "chapter_id": chapter["chapter_id"],
        "source_id": source_id,
        "artifact_id": None,
        "status": status,
        "path": None,
        "size": 0,
        "error": error[-500:] if error else "",
    }


def _build_manifest_skeleton(
    run_id: str,
    notebook_id: str,
    started_at: str,
    selected_chapters: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": None,
        "notebook_id": notebook_id,
        "selected_chapters": [
            {
                "chapter_id": ch["chapter_id"],
                "title": ch["title"],
                "score": ch.get("score"),
                "char_count": ch.get("char_count"),
            }
            for ch in selected_chapters
        ],
        "source_map": {},
        "artifacts": [],
        "status": "started",
    }


def _finalize_status(manifest: Dict[str, Any]) -> str:
    artifacts = manifest.get("artifacts", [])
    if not artifacts:
        return "failed"
    oks = [a for a in artifacts if a.get("status") == "ok"]
    if len(oks) == len(artifacts):
        return "ok"
    if oks:
        return "partial"
    return "failed"


def run_generation(
    *,
    notebook_id: str,
    profile: str,
    out_dir: Path,
    selected_chapters: List[Dict[str, Any]],
    chapter_ids: Optional[List[str]] = None,
    max_chapters: int = 0,
    chars_per_chapter: int = 6000,
    poll_seconds: int = 8,
    max_polls: int = 36,
    source_map: Optional[Dict[str, Any]] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    picked = select_chapters(selected_chapters, chapter_ids=chapter_ids, max_chapters=max_chapters)
    started_at = now_iso_utc()
    rid = (run_id or "").strip() or f"run-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"
    run_dir = out_dir.expanduser().resolve() / rid
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = _build_manifest_skeleton(rid, notebook_id, started_at, picked)
    manifest["run_dir"] = str(run_dir)
    manifest_path = run_dir / "run_manifest.json"

    raw_source_map = source_map or {}
    normalized_map: Dict[str, str] = {}
    for key, val in raw_source_map.items():
        if val:
            normalized_map[str(key)] = str(val)

    chk = run_nlm(["nlm", "login", "--check", "--profile", profile], timeout=90, profile=profile)
    if chk.returncode != 0:
        manifest["finished_at"] = now_iso_utc()
        manifest["status"] = "auth_required"
        manifest["error"] = (chk.stderr or chk.stdout or "auth check failed")[-500:]
        manifest["source_map"] = {ch["chapter_id"]: normalized_map.get(ch["chapter_id"]) for ch in picked}
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        manifest["manifest_path"] = str(manifest_path)
        return manifest

    supports_profile = download_supports_profile()

    for chapter in picked:
        cid = chapter["chapter_id"]
        title = chapter["title"]
        text = (chapter.get("text") or "")[:chars_per_chapter]

        source_id = normalized_map.get(cid)
        if not source_id:
            if not text:
                manifest["artifacts"].append(_empty_artifact(chapter, None, "source_missing", "No source_map entry and no chapter text"))
                continue

            try:
                before_sources = source_list(notebook_id, profile)
            except Exception as exc:
                manifest["artifacts"].append(_empty_artifact(chapter, None, "source_list_failed", str(exc)))
                continue

            add = run_nlm(
                [
                    "nlm",
                    "source",
                    "add",
                    notebook_id,
                    "--text",
                    text,
                    "--title",
                    f"ch{cid} {title}",
                    "--wait",
                    "--profile",
                    profile,
                ],
                timeout=900,
                profile=profile,
            )
            if add.returncode != 0:
                manifest["artifacts"].append(
                    _empty_artifact(chapter, None, "source_add_failed", (add.stderr or add.stdout or "source add failed"))
                )
                continue

            source_id = extract_source_id(add.stdout)
            if not source_id:
                try:
                    after_sources = source_list(notebook_id, profile)
                    source_id = _new_source_id_by_diff(before_sources, after_sources)
                except Exception as exc:
                    manifest["artifacts"].append(_empty_artifact(chapter, None, "source_id_not_found", str(exc)))
                    continue

            if not source_id:
                manifest["artifacts"].append(_empty_artifact(chapter, None, "source_id_not_found", "Unable to resolve source id"))
                continue

        normalized_map[cid] = source_id

        try:
            before_inf = {x.get("id") for x in studio_inf_status(notebook_id, profile) if x.get("id")}
        except Exception:
            before_inf = set()

        cre = run_nlm(
            [
                "nlm",
                "infographic",
                "create",
                notebook_id,
                "--source-ids",
                source_id,
                "--confirm",
                "--profile",
                profile,
            ],
            timeout=300,
            profile=profile,
        )
        if cre.returncode != 0:
            manifest["artifacts"].append(
                _empty_artifact(chapter, source_id, "create_failed", (cre.stderr or cre.stdout or "create failed"))
            )
            continue

        artifact_id = extract_artifact_id(cre.stdout, notebook_id=notebook_id)
        artifact_state = "unknown"
        poll_error = ""

        for _ in range(max_polls):
            try:
                infs = studio_inf_status(notebook_id, profile)
            except Exception as exc:
                poll_error = str(exc)
                time.sleep(poll_seconds)
                continue

            if not artifact_id:
                candidates = [row for row in infs if row.get("id") and row.get("id") not in before_inf]
                if candidates:
                    artifact_id = str(candidates[0].get("id"))

            row = next((item for item in infs if item.get("id") == artifact_id), None) if artifact_id else None
            if row:
                artifact_state = str(row.get("status", "unknown")).lower()
                if artifact_state in TERMINAL_ARTIFACT_STATES:
                    break
            time.sleep(poll_seconds)

        if not artifact_id:
            manifest["artifacts"].append(_empty_artifact(chapter, source_id, "artifact_not_found", poll_error or "Artifact id unresolved"))
            continue

        out_path = run_dir / f"ch{cid}_{sanitize_filename(title)}.png"
        dl = download_infographic(notebook_id, artifact_id, out_path, profile, supports_profile=supports_profile, timeout=300)

        if dl.returncode != 0 and artifact_state in SUCCESS_ARTIFACT_STATES and supports_profile:
            dl = download_infographic(notebook_id, artifact_id, out_path, profile, supports_profile=False, timeout=300)

        ok = dl.returncode == 0 and out_path.exists()
        status = "ok" if ok else ("artifact_failed" if artifact_state in FAILED_ARTIFACT_STATES else "download_failed")
        manifest["artifacts"].append(
            {
                "chapter_id": cid,
                "source_id": source_id,
                "artifact_id": artifact_id,
                "status": status,
                "path": str(out_path) if out_path.exists() else None,
                "size": out_path.stat().st_size if out_path.exists() else 0,
                "error": (dl.stderr or dl.stdout or poll_error)[-500:] if not ok else "",
                "artifact_state": artifact_state,
            }
        )

    manifest["source_map"] = {ch["chapter_id"]: normalized_map.get(ch["chapter_id"]) for ch in picked}
    manifest["finished_at"] = now_iso_utc()
    manifest["status"] = _finalize_status(manifest)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def load_source_map_json(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    data = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if isinstance(data, dict):
        if isinstance(data.get("source_map"), dict):
            return {str(k): v for k, v in data["source_map"].items()}
        return {str(k): v for k, v in data.items()}
    raise ValueError(f"Invalid source map format: {path}")


def load_ranked_chapters(ranked_json: str) -> List[Dict[str, Any]]:
    data = json.loads(Path(ranked_json).expanduser().read_text(encoding="utf-8"))
    if isinstance(data, dict):
        if isinstance(data.get("selected_chapters"), list):
            return [x for x in data.get("selected_chapters", []) if isinstance(x, dict)]
        if isinstance(data.get("chapters"), list):
            return [x for x in data.get("chapters", []) if isinstance(x, dict)]
    raise ValueError(f"Unable to read chapters from {ranked_json}")
