"""Adapter for telegram-book-fetch skill."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .runner import AdapterError, run_json_dict
from ..core.config import SKILL_TELEGRAM_FETCH


class TelegramFetchAdapter:
    def __init__(self, workspace_root: Path) -> None:
        self.workspace_root = workspace_root
        self.script_path = (workspace_root / SKILL_TELEGRAM_FETCH).resolve()
        if not self.script_path.exists():
            raise AdapterError(f"telegram fetch script missing: {self.script_path}")

    def fetch(
        self,
        *,
        book_title: str,
        tg_bot: str,
        tg_session_file: str,
        tg_output_root: str = "",
        timeout: int = 2400,
    ) -> Dict[str, Any]:
        if not book_title.strip():
            raise AdapterError("book_title is required for telegram fetch")

        uv_cmd = [
            "uv",
            "run",
            "--with",
            "telethon",
            "python3",
            str(self.script_path),
            "--query",
            book_title,
            "--bot",
            tg_bot,
            "--session-file",
            tg_session_file,
        ]
        if tg_output_root:
            uv_cmd += ["--output-root", tg_output_root]

        try:
            payload = run_json_dict(uv_cmd, timeout=timeout)
        except Exception:
            py_cmd = [
                "python3",
                str(self.script_path),
                "--query",
                book_title,
                "--bot",
                tg_bot,
                "--session-file",
                tg_session_file,
            ]
            if tg_output_root:
                py_cmd += ["--output-root", tg_output_root]
            payload = run_json_dict(py_cmd, timeout=timeout)

        status = str(payload.get("status") or "").strip()
        if not status:
            raise AdapterError("telegram fetch response missing status")
        return payload
