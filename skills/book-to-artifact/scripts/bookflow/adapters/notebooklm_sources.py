"""NotebookLM source ID resolver by chapter IDs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Sequence

from .runner import AdapterError, get_list_from_any, run_json_any

_CHAPTER_ID_RE = re.compile(r"\bch\s*0*(\d+)\b", re.I)


@dataclass
class SourceResolution:
    source_ids: List[str]
    source_map: Dict[str, str]
    missing_chapter_ids: List[str]


def _normalize_chapter_id(raw: str) -> str:
    token = str(raw).strip()
    if token.isdigit():
        return str(int(token))
    return token


def resolve_source_ids(notebook_id: str, profile: str, chapter_ids: Sequence[str], timeout: int = 300) -> SourceResolution:
    if not notebook_id.strip():
        raise AdapterError("notebook_id is required")

    cmd = ["nlm", "source", "list", notebook_id, "--json", "--profile", profile]
    payload = run_json_any(cmd, timeout=timeout)
    rows = get_list_from_any(payload, ["sources", "items", "results", "data"])

    source_map: Dict[str, str] = {}
    for row in rows:
        source_id = str(row.get("id") or "").strip()
        title = str(row.get("title") or "")
        if not source_id:
            continue

        match = _CHAPTER_ID_RE.search(title)
        if not match:
            continue

        chapter_id = _normalize_chapter_id(match.group(1))
        source_map.setdefault(chapter_id, source_id)

    normalized_chapters = [_normalize_chapter_id(cid) for cid in chapter_ids]
    picked: List[str] = []
    missing: List[str] = []
    for chapter_id in normalized_chapters:
        source_id = source_map.get(chapter_id)
        if source_id:
            picked.append(source_id)
        else:
            missing.append(chapter_id)

    return SourceResolution(source_ids=picked, source_map=source_map, missing_chapter_ids=missing)
