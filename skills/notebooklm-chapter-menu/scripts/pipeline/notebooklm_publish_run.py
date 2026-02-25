#!/usr/bin/env python3
"""Publish NotebookLM run manifest results to Obsidian and Notion."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

NOTION_VERSION_DEFAULT = "2025-09-03"
NOTION_FILE_LIMIT_BYTES = 5 * 1024 * 1024
SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent.parent
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


def _read_json(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected object JSON in {path}")
    return data


def _load_notion_key(api_key_file: str) -> str:
    env_key = os.environ.get("NOTION_API_KEY", "").strip()
    if env_key:
        return env_key
    key_path = Path(api_key_file).expanduser()
    if key_path.exists():
        value = key_path.read_text(encoding="utf-8").strip()
        if value:
            return value
    return ""


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _build_gdrive_config(args: argparse.Namespace, defaults: Dict[str, Any]) -> Dict[str, Any]:
    disabled_flag = bool(getattr(args, "gdrive_disabled", False)) or _env_flag("GDRIVE_DISABLED", False)
    enabled_flag = _env_flag("GDRIVE_ENABLED", False)
    if isinstance(getattr(args, "gdrive_enabled", None), bool):
        enabled_flag = bool(getattr(args, "gdrive_enabled"))
    elif isinstance(defaults.get("gdrive_enabled"), bool):
        enabled_flag = bool(defaults.get("gdrive_enabled"))

    client_secrets = _str_choice(
        getattr(args, "gdrive_client_secrets", ""),
        os.environ.get("GDRIVE_CLIENT_SECRETS", ""),
        defaults.get("gdrive_client_secrets"),
    )
    token_file = _str_choice(
        getattr(args, "gdrive_token_file", ""),
        os.environ.get("GDRIVE_TOKEN_FILE", ""),
        defaults.get("gdrive_token_file"),
    )
    folder_id = _str_choice(
        getattr(args, "gdrive_folder_id", ""),
        os.environ.get("GDRIVE_FOLDER_ID", ""),
        defaults.get("gdrive_folder_id"),
    )

    anyone_reader = bool(getattr(args, "gdrive_anyone_reader", False)) or _env_flag("GDRIVE_ANYONE_READER", True)

    client_path = str(Path(client_secrets).expanduser()) if client_secrets else ""
    token_path = str(Path(token_file).expanduser()) if token_file else ""
    auto_enabled = bool(client_path and Path(client_path).exists())

    cfg = {
        "enabled": bool(False if disabled_flag else (enabled_flag or auto_enabled)),
        "client_secrets": client_path,
        "token_file": token_path,
        "folder_id": folder_id,
        "anyone_reader": anyone_reader,
    }

    if cfg["enabled"] and (not cfg["client_secrets"]):
        cfg["enabled"] = False
        cfg["disabled_reason"] = "missing_client_secrets"
    elif cfg["enabled"] and (not Path(cfg["client_secrets"]).exists()):
        cfg["enabled"] = False
        cfg["disabled_reason"] = f"missing_client_secrets:{cfg['client_secrets']}"

    return cfg


def _notion_request(method: str, url: str, key: str, notion_version: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    body = None
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url=url,
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {key}",
            "Notion-Version": notion_version,
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Notion HTTP {exc.code}: {text[:800]}") from exc


def _property_text_value(prop: Dict[str, Any]) -> str:
    ptype = prop.get("type")
    if ptype == "title":
        arr = prop.get("title", [])
    elif ptype == "rich_text":
        arr = prop.get("rich_text", [])
    else:
        return ""
    if not isinstance(arr, list):
        return ""
    texts = [str(x.get("plain_text", "")) for x in arr if isinstance(x, dict)]
    return "".join(texts).strip()


def _find_title_property(schema_props: Dict[str, Any]) -> Optional[str]:
    for name, meta in schema_props.items():
        if isinstance(meta, dict) and meta.get("type") == "title":
            return name
    return None


def _find_run_id_property(schema_props: Dict[str, Any]) -> Optional[str]:
    keys = {name.lower(): name for name in schema_props.keys()}
    for cand in ("run_id", "run id", "runid"):
        if cand in keys:
            name = keys[cand]
            meta = schema_props.get(name, {})
            if isinstance(meta, dict) and meta.get("type") in ("rich_text", "title"):
                return name
    return None


def _set_property(properties: Dict[str, Any], schema_props: Dict[str, Any], name: str, value: str) -> None:
    meta = schema_props.get(name)
    if not isinstance(meta, dict):
        return
    ptype = meta.get("type")
    if ptype == "title":
        properties[name] = {"title": [{"type": "text", "text": {"content": value}}]}
    elif ptype == "rich_text":
        properties[name] = {"rich_text": [{"type": "text", "text": {"content": value}}]}
    elif ptype == "date":
        properties[name] = {"date": {"start": value}}
    elif ptype == "status":
        properties[name] = {"status": {"name": value}}
    elif ptype == "select":
        properties[name] = {"select": {"name": value}}
    elif ptype == "url":
        properties[name] = {"url": value}
    elif ptype == "multi_select":
        properties[name] = {"multi_select": [{"name": value}]}


def _notion_rich_text(text: str, link: Optional[str] = None) -> List[Dict[str, Any]]:
    node: Dict[str, Any] = {"type": "text", "text": {"content": text}}
    if link:
        node["text"]["link"] = {"url": link}
    return [node]


def _build_obsidian_uri(vault_path: str, index_path: str) -> str:
    try:
        vault = Path(vault_path).expanduser().resolve()
        idx = Path(index_path).expanduser().resolve()
        rel = idx.relative_to(vault).as_posix()
        return f"obsidian://open?vault={urllib.parse.quote(vault.name)}&file={urllib.parse.quote(rel, safe='')}"
    except Exception:
        return ""


# helper removed: use _page_has_marker instead

def _append_page_blocks(page_id: str, blocks: List[Dict[str, Any]], api_key: str, notion_version: str) -> None:
    if not blocks:
        return
    base = "https://api.notion.com/v1"
    step = 50
    for i in range(0, len(blocks), step):
        chunk = blocks[i : i + step]
        _notion_request(
            "PATCH",
            f"{base}/blocks/{page_id}/children",
            api_key,
            notion_version,
            payload={"children": chunk},
        )


def _build_run_blocks(manifest: Dict[str, Any], obsidian_uri: str) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    run_id = str(manifest.get("run_id") or "")
    blocks.append(
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": _notion_rich_text("Run Summary")},
        }
    )
    for line in [
        f"run_id: {run_id}",
        f"status: {manifest.get('status', '')}",
        f"notebook_id: {manifest.get('notebook_id', '')}",
        f"started_at: {manifest.get('started_at', '')}",
        f"finished_at: {manifest.get('finished_at', '')}",
    ]:
        blocks.append(
            {
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": _notion_rich_text(line)},
            }
        )

    if obsidian_uri:
        blocks.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": _notion_rich_text("Open in Obsidian", link=obsidian_uri)},
            }
        )

    blocks.append(
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": _notion_rich_text("Artifacts")},
        }
    )
    artifacts = manifest.get("artifacts", []) if isinstance(manifest.get("artifacts"), list) else []
    for item in artifacts:
        cid = str(item.get("chapter_id", ""))
        status = str(item.get("status", ""))
        size = int(item.get("size") or 0)
        path = str(item.get("path") or "").strip()
        artifact_type = str(item.get("artifact_type") or "").strip()
        artifact_id = str(item.get("artifact_id") or "").strip()
        err = str(item.get("error") or "").strip()
        line = f"{artifact_type or '-'} | ch{cid or '-'} | {status or '-'} | {size} bytes | {artifact_id or '-'}"
        blocks.append(
            {
                "object": "block",
                "type": "numbered_list_item",
                "numbered_list_item": {"rich_text": _notion_rich_text(line)},
            }
        )
        if path:
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": _notion_rich_text(path)},
                }
            )
        elif err:
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": _notion_rich_text(err)},
                }
            )
    return blocks


def _page_has_marker(page_id: str, marker: str, api_key: str, notion_version: str) -> bool:
    base = "https://api.notion.com/v1"
    data = _notion_request("GET", f"{base}/blocks/{page_id}/children?page_size=100", api_key, notion_version)
    rows = data.get("results", []) if isinstance(data.get("results"), list) else []
    for row in rows:
        if marker in json.dumps(row, ensure_ascii=False):
            return True
    return False


def _append_marker(page_id: str, marker: str, api_key: str, notion_version: str) -> None:
    _append_page_blocks(
        page_id,
        [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": _notion_rich_text(marker)},
            }
        ],
        api_key,
        notion_version,
    )


def _load_studio_rows(notebook_id: str, profile: str) -> List[Dict[str, Any]]:
    if not notebook_id.strip():
        return []
    cmd = ["nlm", "studio", "status", notebook_id, "--full", "--json", "--profile", profile]
    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if p.returncode != 0:
        return []
    raw = (p.stdout or "").strip()
    if not raw:
        return []
    try:
        js = json.loads(raw)
    except Exception:
        rows: List[Dict[str, Any]] = []
        for ln in reversed([x.strip() for x in raw.splitlines() if x.strip()]):
            if ln.startswith("[") or ln.startswith("{"):
                try:
                    js = json.loads(ln)
                    break
                except Exception:
                    continue
        else:
            return []

    if isinstance(js, list):
        return [x for x in js if isinstance(x, dict)]
    if isinstance(js, dict):
        for k in ("artifacts", "items", "results", "data"):
            v = js.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _append_studio_artifacts_section(
    page_id: str,
    notebook_id: str,
    profile: str,
    api_key: str,
    notion_version: str,
) -> Dict[str, Any]:
    rows = _load_studio_rows(notebook_id, profile)
    if not rows:
        return {"added": 0, "reason": "no_rows"}

    blocks: List[Dict[str, Any]] = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": _notion_rich_text("Studio Artifacts (live)")},
        }
    ]

    added = 0
    for row in rows:
        aid = str(row.get("id") or "").strip()
        atype = str(row.get("type") or "").strip()
        status = str(row.get("status") or "").strip()
        title = str(row.get("title") or "").strip()
        if not (aid or atype or status or title):
            continue
        line = f"{atype or '-'} | {status or '-'} | {title or '-'} | {aid or '-'}"
        blocks.append(
            {
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": _notion_rich_text(line)},
            }
        )
        added += 1

    if added > 0:
        _append_page_blocks(page_id, blocks, api_key, notion_version)
    return {"added": added}


def _download_spec_for_type(artifact_type: str) -> Optional[Dict[str, str]]:
    m = {
        "audio": {"sub": "audio", "ext": ".m4a"},
        "report": {"sub": "report", "ext": ".txt"},
        "slide_deck": {"sub": "slide-deck", "ext": ".pdf"},
        "video": {"sub": "video", "ext": ".mp4"},
        "data_table": {"sub": "data-table", "ext": ".csv"},
        "mind_map": {"sub": "mind-map", "ext": ".json"},
        "quiz": {"sub": "quiz", "ext": ".json"},
        "flashcards": {"sub": "flashcards", "ext": ".json"},
    }
    return m.get((artifact_type or "").strip())


def _download_artifact_to_temp(notebook_id: str, artifact_id: str, artifact_type: str, profile: str) -> Optional[Path]:
    spec = _download_spec_for_type(artifact_type)
    if not spec:
        return None
    tmp_dir = Path(tempfile.mkdtemp(prefix="notion_artifact_"))
    out = tmp_dir / f"{artifact_type}_{artifact_id}{spec['ext']}"
    cmd = [
        "nlm",
        "download",
        spec["sub"],
        notebook_id,
        "--id",
        artifact_id,
        "--output",
        str(out),
    ]
    if spec["sub"] in {"audio", "video", "slide-deck"}:
        cmd.append("--no-progress")

    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if p.returncode != 0 or not out.exists() or out.stat().st_size == 0:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None
    return out


def _upload_file_to_gdrive(path: Path, cfg: Dict[str, Any]) -> Dict[str, Any]:
    helper = SCRIPT_DIR / "gdrive_upload_one.py"
    if not helper.exists():
        raise RuntimeError(f"gdrive helper missing: {helper}")

    client_secrets = str(cfg.get("client_secrets") or "").strip()
    token_file = str(cfg.get("token_file") or "").strip()
    if not client_secrets:
        raise RuntimeError("gdrive client_secrets is not configured")
    if not token_file:
        raise RuntimeError("gdrive token_file is not configured")

    cmd = [
        "uv",
        "run",
        "--with",
        "google-api-python-client",
        "--with",
        "google-auth-httplib2",
        "--with",
        "google-auth-oauthlib",
        "python3",
        str(helper),
        "--file",
        str(path),
        "--client-secrets",
        client_secrets,
        "--token-file",
        token_file,
        "--name",
        path.name,
    ]

    folder_id = str(cfg.get("folder_id") or "").strip()
    if folder_id:
        cmd += ["--folder-id", folder_id]

    if bool(cfg.get("anyone_reader")):
        cmd.append("--anyone-reader")

    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if p.returncode != 0:
        raise RuntimeError(f"gdrive upload failed: {(p.stderr or p.stdout)[-600:]}")

    text = (p.stdout or "").strip()
    payload: Dict[str, Any] = {}
    try:
        payload = json.loads(text)
    except Exception:
        for ln in reversed([x.strip() for x in text.splitlines() if x.strip()]):
            if ln.startswith("{"):
                try:
                    payload = json.loads(ln)
                    break
                except Exception:
                    continue

    if not isinstance(payload, dict) or payload.get("status") != "ok":
        raise RuntimeError(f"gdrive upload returned invalid payload: {text[-600:]}")

    return payload


def _append_downloaded_artifact_files(
    page_id: str,
    notebook_id: str,
    profile: str,
    api_key: str,
    notion_version: str,
    gdrive_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows = _load_studio_rows(notebook_id, profile)
    if not rows:
        return {
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "gdrive_uploaded": 0,
            "gdrive_failed": 0,
            "large_skipped": 0,
            "reason": "no_rows",
        }

    blocks: List[Dict[str, Any]] = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": _notion_rich_text("Downloaded Artifacts")},
        }
    ]

    uploaded = 0
    skipped = 0
    failed = 0
    gdrive_uploaded = 0
    gdrive_failed = 0
    large_skipped = 0

    for row in rows:
        aid = str(row.get("id") or "").strip()
        atype = str(row.get("type") or "").strip()
        status = str(row.get("status") or "").strip().lower()
        title = str(row.get("title") or "").strip()

        if not aid or status != "completed":
            skipped += 1
            continue
        if atype == "infographic":
            skipped += 1
            continue
        if not _download_spec_for_type(atype):
            skipped += 1
            continue

        path = _download_artifact_to_temp(notebook_id, aid, atype, profile)
        if not path:
            failed += 1
            continue

        try:
            size_bytes = int(path.stat().st_size)
            if size_bytes <= NOTION_FILE_LIMIT_BYTES:
                file_upload_id = _upload_file_to_notion(path, api_key, notion_version)
                blocks.append(
                    {
                        "object": "block",
                        "type": "file",
                        "file": {"type": "file_upload", "file_upload": {"id": file_upload_id}},
                    }
                )
                meta = f"{atype} | {title or '-'} | {aid}"
                blocks.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {"rich_text": _notion_rich_text(meta)},
                    }
                )
                uploaded += 1
                continue

            gdrive_enabled = bool(gdrive_config and gdrive_config.get("enabled"))
            if not gdrive_enabled:
                large_skipped += 1
                blocks.append(
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": _notion_rich_text(
                                f"{atype} | {title or '-'} | {aid} | skipped (>5MB, gdrive disabled)"
                            )
                        },
                    }
                )
                continue

            payload = _upload_file_to_gdrive(path, gdrive_config or {})
            web_link = str(payload.get("web_view_link") or payload.get("web_content_link") or "").strip()
            if not web_link:
                raise RuntimeError("gdrive upload returned empty web link")

            size_mb = round(size_bytes / (1024 * 1024), 2)
            label = f"{atype} | {title or '-'} | {aid} | {size_mb}MB"
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": _notion_rich_text(label)},
                }
            )
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": _notion_rich_text("Open on Google Drive", link=web_link)},
                }
            )
            gdrive_uploaded += 1
        except Exception:
            failed += 1
            if size_bytes > NOTION_FILE_LIMIT_BYTES:
                gdrive_failed += 1
        finally:
            shutil.rmtree(path.parent, ignore_errors=True)

    if uploaded > 0 or gdrive_uploaded > 0 or large_skipped > 0:
        _append_page_blocks(page_id, blocks, api_key, notion_version)

    return {
        "uploaded": uploaded,
        "skipped": skipped,
        "failed": failed,
        "gdrive_uploaded": gdrive_uploaded,
        "gdrive_failed": gdrive_failed,
        "large_skipped": large_skipped,
    }


def _compress_image_for_notion(src_path: Path) -> Path:
    tmp_dir = Path(tempfile.mkdtemp(prefix="notion_img_"))
    out_path = tmp_dir / f"{src_path.stem}.jpg"
    cmd = [
        "sips",
        "-s",
        "format",
        "jpeg",
        "-s",
        "formatOptions",
        "72",
        "-Z",
        "1920",
        str(src_path),
        "--out",
        str(out_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    return out_path


def _upload_file_to_notion(path: Path, api_key: str, notion_version: str) -> str:
    upload_target = path
    cleanup_dir: Optional[Path] = None
    if path.stat().st_size > NOTION_FILE_LIMIT_BYTES:
        if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp", ".gif"}:
            compressed = _compress_image_for_notion(path)
            cleanup_dir = compressed.parent
            upload_target = compressed
            if upload_target.stat().st_size > NOTION_FILE_LIMIT_BYTES:
                raise RuntimeError(f"File too large even after compression: {path.name}")
        else:
            raise RuntimeError(f"File too large for Notion upload limit: {path.name}")

    create = _notion_request(
        "POST",
        "https://api.notion.com/v1/file_uploads",
        api_key,
        notion_version,
        payload={},
    )
    file_upload_id = str(create.get("id") or "").strip()
    upload_url = str(create.get("upload_url") or "").strip()
    if not file_upload_id or not upload_url:
        raise RuntimeError("Failed to create Notion file upload")

    mime_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".pdf": "application/pdf",
        ".md": "text/plain",
        ".txt": "text/plain",
        ".m4a": "audio/mp4",
        ".mp4": "video/mp4",
        ".csv": "text/csv",
        ".json": "application/json",
    }
    ext = upload_target.suffix.lower()
    ctype = mime_map.get(ext, "application/octet-stream")

    cmd = [
        "curl",
        "-sS",
        "-X",
        "POST",
        upload_url,
        "-H",
        f"Authorization: Bearer {api_key}",
        "-H",
        f"Notion-Version: {notion_version}",
        "-F",
        f"file=@{upload_target};type={ctype}",
    ]
    p = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if p.returncode != 0:
        raise RuntimeError(f"curl upload failed: {p.stderr[-300:]}")
    js = json.loads((p.stdout or "{}").strip() or "{}")
    if js.get("status") != "uploaded":
        msg = js.get("message") or js.get("code") or json.dumps(js, ensure_ascii=False)[:300]
        raise RuntimeError(f"Notion upload failed: {msg}")

    if cleanup_dir and cleanup_dir.exists():
        shutil.rmtree(cleanup_dir, ignore_errors=True)

    return file_upload_id


def _append_artifact_images(page_id: str, manifest: Dict[str, Any], api_key: str, notion_version: str) -> Dict[str, Any]:
    artifacts = manifest.get("artifacts", []) if isinstance(manifest.get("artifacts"), list) else []
    uploaded = 0
    skipped = 0
    failed = 0
    blocks: List[Dict[str, Any]] = [
        {
            "object": "block",
            "type": "heading_2",
            "heading_2": {"rich_text": _notion_rich_text("Images")},
        }
    ]
    for item in artifacts:
        path = str(item.get("path") or "").strip()
        cid = str(item.get("chapter_id") or "")
        if not path or not Path(path).exists():
            skipped += 1
            continue
        try:
            file_upload_id = _upload_file_to_notion(Path(path), api_key, notion_version)
            blocks.append(
                {
                    "object": "block",
                    "type": "image",
                    "image": {"type": "file_upload", "file_upload": {"id": file_upload_id}},
                }
            )
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": _notion_rich_text(f"ch{cid}")},
                }
            )
            uploaded += 1
        except Exception:
            failed += 1

    if len(blocks) > 1:
        _append_page_blocks(page_id, blocks, api_key, notion_version)

    return {"uploaded": uploaded, "skipped": skipped, "failed": failed}


def publish_obsidian(manifest: Dict[str, Any], vault_path: str, subdir: str) -> Dict[str, Any]:
    if not vault_path.strip():
        return {"enabled": False, "status": "skipped", "reason": "obsidian vault path not set"}

    vault = Path(vault_path).expanduser().resolve()
    if not vault.exists():
        raise RuntimeError(f"Obsidian vault not found: {vault}")

    run_id = str(manifest.get("run_id") or "unknown-run")
    root = vault / subdir / run_id
    attachments = root / "attachments"
    attachments.mkdir(parents=True, exist_ok=True)

    artifacts: List[Dict[str, Any]] = manifest.get("artifacts", []) if isinstance(manifest.get("artifacts"), list) else []
    copied = 0
    linked = 0
    lines: List[str] = []
    lines.append(f"# NotebookLM Run {run_id}")
    lines.append("")
    lines.append(f"- status: {manifest.get('status', '')}")
    lines.append(f"- notebook_id: {manifest.get('notebook_id', '')}")
    lines.append(f"- started_at: {manifest.get('started_at', '')}")
    lines.append(f"- finished_at: {manifest.get('finished_at', '')}")
    lines.append("")
    lines.append("## Artifacts")

    for item in artifacts:
        cid = str(item.get("chapter_id", ""))
        status = str(item.get("status", ""))
        src = str(item.get("path") or "").strip()
        if src and Path(src).exists():
            src_path = Path(src).resolve()
            dst_name = src_path.name
            dst = attachments / dst_name
            if not dst.exists() or dst.stat().st_size != src_path.stat().st_size:
                shutil.copy2(src_path, dst)
                copied += 1
            rel = dst.relative_to(root)
            lines.append(f"- ch{cid}: {status} [[{rel.as_posix()}]]")
            lines.append(f"  ![[{rel.as_posix()}]]")
            linked += 1
        else:
            err = str(item.get("error") or "")
            lines.append(f"- ch{cid}: {status} (no file) {err}")

    index_path = root / "index.md"
    index_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

    return {
        "enabled": True,
        "status": "ok",
        "vault_path": str(vault),
        "run_dir": str(root),
        "index_path": str(index_path),
        "obsidian_uri": _build_obsidian_uri(str(vault), str(index_path)),
        "copied": copied,
        "linked": linked,
    }


def publish_notion(
    manifest: Dict[str, Any],
    data_source_id: str,
    api_key: str,
    notion_version: str,
    profile: str,
    obsidian_info: Optional[Dict[str, Any]] = None,
    gdrive_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not data_source_id.strip():
        return {"enabled": False, "status": "skipped", "reason": "notion data source id not set"}
    if not api_key.strip():
        return {"enabled": True, "status": "failed", "error": "notion api key missing"}

    run_id = str(manifest.get("run_id") or "")
    title_value = f"NotebookLM Run {run_id}"
    base = "https://api.notion.com/v1"

    ds = _notion_request("GET", f"{base}/data_sources/{data_source_id}", api_key, notion_version)
    props = ds.get("properties", {}) if isinstance(ds.get("properties"), dict) else {}
    title_prop = _find_title_property(props)
    run_id_prop = _find_run_id_property(props)

    query = _notion_request(
        "POST",
        f"{base}/data_sources/{data_source_id}/query",
        api_key,
        notion_version,
        payload={"page_size": 100},
    )
    results = query.get("results", []) if isinstance(query.get("results"), list) else []

    page_id: Optional[str] = None
    for page in results:
        if not isinstance(page, dict):
            continue
        page_props = page.get("properties", {}) if isinstance(page.get("properties"), dict) else {}
        run_value = ""
        if run_id_prop and isinstance(page_props.get(run_id_prop), dict):
            run_value = _property_text_value(page_props[run_id_prop])
        if run_value == run_id:
            page_id = str(page.get("id"))
            break

        if title_prop and isinstance(page_props.get(title_prop), dict):
            tval = _property_text_value(page_props[title_prop])
            if tval == title_value:
                page_id = str(page.get("id"))
                break

    obsidian_uri = ""
    if isinstance(obsidian_info, dict):
        obsidian_uri = str(obsidian_info.get("obsidian_uri") or "").strip()

    patch_props: Dict[str, Any] = {}
    if title_prop:
        _set_property(patch_props, props, title_prop, title_value)
    if run_id_prop:
        _set_property(patch_props, props, run_id_prop, run_id)

    for name in props.keys():
        lowered = name.lower()
        if lowered in ("notebook_id", "notebook id"):
            _set_property(patch_props, props, name, str(manifest.get("notebook_id") or ""))
        elif lowered in ("started_at", "started at"):
            _set_property(patch_props, props, name, str(manifest.get("started_at") or ""))
        elif lowered in ("finished_at", "finished at"):
            _set_property(patch_props, props, name, str(manifest.get("finished_at") or ""))
        elif lowered in ("status", "run_status", "run status"):
            _set_property(patch_props, props, name, str(manifest.get("status") or ""))
        elif lowered in ("类型", "type"):
            _set_property(patch_props, props, name, "NotebookLM Run")
        elif lowered in ("url 1", "url1", "url_1") and obsidian_uri:
            _set_property(patch_props, props, name, obsidian_uri)

    action = "updated" if page_id else "created"
    if page_id:
        _notion_request(
            "PATCH",
            f"{base}/pages/{page_id}",
            api_key,
            notion_version,
            payload={"properties": patch_props},
        )
    else:
        created = _notion_request(
            "POST",
            f"{base}/pages",
            api_key,
            notion_version,
            payload={
                "parent": {"data_source_id": data_source_id},
                "properties": patch_props,
            },
        )
        page_id = str(created.get("id") or "")

    summary_appended = False
    images_info = {"uploaded": 0, "skipped": 0, "failed": 0}
    studio_info = {"added": 0, "reason": "skipped"}
    files_info = {"uploaded": 0, "skipped": 0, "failed": 0, "reason": "skipped"}

    if page_id:
        summary_marker = f"notion_summary_run::{run_id}"
        if not _page_has_marker(page_id, summary_marker, api_key, notion_version):
            if _page_has_marker(page_id, run_id, api_key, notion_version):
                _append_marker(page_id, summary_marker, api_key, notion_version)
            else:
                blocks = _build_run_blocks(manifest, obsidian_uri)
                _append_page_blocks(page_id, blocks, api_key, notion_version)
                _append_marker(page_id, summary_marker, api_key, notion_version)
                summary_appended = True

        image_marker = f"notion_images_run::{run_id}"
        if not _page_has_marker(page_id, image_marker, api_key, notion_version):
            images_info = _append_artifact_images(page_id, manifest, api_key, notion_version)
            _append_marker(page_id, image_marker, api_key, notion_version)

        studio_marker = f"notion_studio_artifacts_run::{run_id}"
        if not _page_has_marker(page_id, studio_marker, api_key, notion_version):
            studio_info = _append_studio_artifacts_section(
                page_id=page_id,
                notebook_id=str(manifest.get("notebook_id") or ""),
                profile=profile,
                api_key=api_key,
                notion_version=notion_version,
            )
            _append_marker(page_id, studio_marker, api_key, notion_version)

        files_marker = f"notion_downloaded_artifacts_run::{run_id}"
        if gdrive_config and gdrive_config.get("enabled"):
            files_marker = f"{files_marker}::gdrive"

        if not _page_has_marker(page_id, files_marker, api_key, notion_version):
            files_info = _append_downloaded_artifact_files(
                page_id=page_id,
                notebook_id=str(manifest.get("notebook_id") or ""),
                profile=profile,
                api_key=api_key,
                notion_version=notion_version,
                gdrive_config=gdrive_config,
            )
            _append_marker(page_id, files_marker, api_key, notion_version)

    return {
        "enabled": True,
        "status": "ok",
        "action": action,
        "page_id": page_id,
        "summary_appended": summary_appended,
        "images": images_info,
        "studio_artifacts": studio_info,
        "downloaded_artifacts": files_info,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish NotebookLM run output to Obsidian and Notion")
    ap.add_argument("--manifest-json", required=True)
    ap.add_argument("--config", default="", help="JSON config path (default: skills/notebooklm-chapter-menu/config/defaults.json)")
    ap.add_argument("--obsidian-vault-path", default="")
    ap.add_argument("--obsidian-subdir", default="")
    ap.add_argument("--notion-data-source-id", default="")
    ap.add_argument("--notion-api-key-file", default="")
    ap.add_argument("--notion-version", default=NOTION_VERSION_DEFAULT)
    ap.add_argument("--profile", default=os.environ.get("NOTEBOOKLM_PROFILE", "default"))

    ap.add_argument("--gdrive-enabled", dest="gdrive_enabled", action="store_true", help="Enable Google Drive upload for artifacts >5MB")
    ap.add_argument("--no-gdrive", dest="gdrive_disabled", action="store_true", help="Disable Google Drive upload even if local credentials are present")
    ap.add_argument("--gdrive-client-secrets", default="")
    ap.add_argument("--gdrive-token-file", default="")
    ap.add_argument("--gdrive-folder-id", default="")
    ap.add_argument("--gdrive-anyone-reader", action="store_true", help="Grant anyone-reader permission on uploaded files")

    args = ap.parse_args()

    config_path = Path(
        _str_choice(args.config, os.environ.get(ENV_CONFIG_PATH, ""))
        or str(DEFAULT_CONFIG_PATH)
    ).expanduser().resolve()
    defaults = _load_defaults(config_path)

    args.obsidian_vault_path = _str_choice(
        args.obsidian_vault_path,
        os.environ.get("OBSIDIAN_VAULT_PATH", ""),
        defaults.get("obsidian_vault_path"),
    )
    args.obsidian_subdir = _str_choice(
        args.obsidian_subdir,
        os.environ.get("OBSIDIAN_SUBDIR", ""),
        defaults.get("obsidian_subdir"),
        "NotebookLM/Infographics",
    )
    args.notion_data_source_id = _str_choice(
        args.notion_data_source_id,
        os.environ.get("NOTION_DATA_SOURCE_ID", ""),
        defaults.get("notion_data_source_id"),
    )
    args.notion_api_key_file = _str_choice(
        args.notion_api_key_file,
        os.environ.get("NOTION_API_KEY_FILE", ""),
        defaults.get("notion_api_key_file"),
        "~/.config/notion/api_key",
    )

    gdrive_config = _build_gdrive_config(args, defaults)

    out: Dict[str, Any] = {
        "status": "started",
        "manifest_json": str(Path(args.manifest_json).expanduser().resolve()),
        "config_path": str(config_path),
        "obsidian": {},
        "notion": {},
        "gdrive": gdrive_config,
    }

    try:
        manifest = _read_json(Path(args.manifest_json).expanduser())
    except Exception as exc:
        out["status"] = "failed"
        out["error"] = f"manifest_load_failed: {exc}"
        print(json.dumps(out, ensure_ascii=False))
        return

    obsidian_error = ""
    notion_error = ""

    try:
        out["obsidian"] = publish_obsidian(manifest, args.obsidian_vault_path, args.obsidian_subdir)
    except Exception as exc:
        obsidian_error = str(exc)
        out["obsidian"] = {"enabled": True, "status": "failed", "error": obsidian_error}

    try:
        notion_key = _load_notion_key(args.notion_api_key_file)
        out["notion"] = publish_notion(
            manifest,
            data_source_id=args.notion_data_source_id,
            api_key=notion_key,
            notion_version=args.notion_version,
            profile=args.profile,
            obsidian_info=out.get("obsidian"),
            gdrive_config=gdrive_config,
        )
    except Exception as exc:
        notion_error = str(exc)
        out["notion"] = {"enabled": True, "status": "failed", "error": notion_error}

    sink_statuses = [
        str(out["obsidian"].get("status", "skipped")),
        str(out["notion"].get("status", "skipped")),
    ]
    failures = [s for s in sink_statuses if s == "failed"]
    oks = [s for s in sink_statuses if s == "ok"]

    if failures and oks:
        out["status"] = "partial"
    elif failures and not oks:
        out["status"] = "failed"
    else:
        out["status"] = "ok"

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
