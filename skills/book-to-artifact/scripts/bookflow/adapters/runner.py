"""Shared subprocess runner and robust JSON parsing for adapters."""

from __future__ import annotations

import json
import subprocess
from typing import Any, Dict, Iterable, List, Sequence

from ..core.io import parse_json_object, parse_json_payload


class AdapterError(RuntimeError):
    pass


def _tail(text: str, limit: int = 1200) -> str:
    raw = text or ""
    return raw[-limit:]


def run_command(cmd: Sequence[str], timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(list(cmd), capture_output=True, text=True, timeout=timeout)


def run_json_dict(cmd: Sequence[str], timeout: int = 1800) -> Dict[str, Any]:
    proc = run_command(cmd, timeout=timeout)
    if proc.returncode != 0:
        raise AdapterError(
            json.dumps(
                {
                    "cmd": list(cmd),
                    "returncode": proc.returncode,
                    "stdout": _tail(proc.stdout),
                    "stderr": _tail(proc.stderr),
                },
                ensure_ascii=False,
            )
        )

    try:
        return parse_json_object(proc.stdout or "")
    except ValueError as exc:
        raise AdapterError(f"Failed to parse JSON object from command output: {cmd}: {exc}") from exc


def run_json_any(cmd: Sequence[str], timeout: int = 1800) -> Any:
    proc = run_command(cmd, timeout=timeout)
    if proc.returncode != 0:
        raise AdapterError(
            json.dumps(
                {
                    "cmd": list(cmd),
                    "returncode": proc.returncode,
                    "stdout": _tail(proc.stdout),
                    "stderr": _tail(proc.stderr),
                },
                ensure_ascii=False,
            )
        )

    try:
        return parse_json_payload(proc.stdout or "")
    except ValueError as exc:
        raise AdapterError(f"Failed to parse JSON payload from command output: {cmd}: {exc}") from exc


def get_list_from_any(payload: Any, keys: Iterable[str]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]

        gathered: List[Dict[str, Any]] = []
        for value in payload.values():
            if isinstance(value, list):
                gathered.extend([row for row in value if isinstance(row, dict)])
        if gathered:
            return gathered

    return []
