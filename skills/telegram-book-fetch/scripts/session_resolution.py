"""Shared Telegram session path resolution for telegram-book-fetch scripts."""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_SESSION_FILE = "~/.openclaw/credentials/telegram/main"


def resolve_session_file(explicit: str = "") -> str:
    raw = (explicit or "").strip()
    if raw:
        return str(Path(raw).expanduser())

    env = os.environ.get("TG_SESSION_FILE", "").strip()
    if env:
        return str(Path(env).expanduser())

    return str(Path(DEFAULT_SESSION_FILE).expanduser())
