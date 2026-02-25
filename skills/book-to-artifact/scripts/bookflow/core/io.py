"""I/O utilities for run manifests and structured command output parsing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from .models import RunManifest, now_iso


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_json_payload(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        raise ValueError("No JSON found in output")

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    best_obj: Any = None
    best_end_index = -1
    for index, char in enumerate(text):
        if char not in "[{":
            continue
        try:
            obj, end = decoder.raw_decode(text[index:])
            end_index = index + end
            if end_index > best_end_index:
                best_obj = obj
                best_end_index = end_index
        except json.JSONDecodeError:
            continue
    if best_obj is not None:
        return best_obj

    for line in reversed([ln.strip() for ln in text.splitlines() if ln.strip()]):
        if line.startswith("{") or line.startswith("["):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue

    raise ValueError("No JSON object found in output")


def parse_json_object(raw: str) -> Dict[str, Any]:
    parsed = parse_json_payload(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected JSON object, got {type(parsed).__name__}")
    return parsed


def read_manifest(path: Path) -> RunManifest:
    data = json.loads(path.read_text(encoding="utf-8"))
    return RunManifest.from_dict(data)


def write_manifest(path: Path, manifest: RunManifest) -> None:
    ensure_parent(path)
    manifest.touch()
    path.write_text(json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def append_event(path: Path, event: str, payload: Dict[str, Any]) -> None:
    ensure_parent(path)
    row = {
        "ts": now_iso(),
        "event": event,
        "payload": payload,
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
