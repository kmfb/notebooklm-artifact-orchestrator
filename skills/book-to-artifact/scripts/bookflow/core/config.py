"""Shared constants for Bookflow orchestration."""

from __future__ import annotations

from pathlib import Path

DEFAULT_WORKSPACE_ROOT = ""
DEFAULT_TELEGRAM_SESSION_PATH = "~/.openclaw/credentials/telegram/main"
DEFAULT_BOOKFLOW_CONFIG = Path("skills/book-to-artifact/config/defaults.json")

ENV_WORKSPACE_ROOT = "OPENCLAW_WORKSPACE"
ENV_TELEGRAM_SESSION = "TG_SESSION_FILE"
ENV_BOOKFLOW_CONFIG = "BOOKFLOW_CONFIG"

BOOKFLOW_TMP_DIRNAME = "book-to-artifact"
MANIFEST_FILENAME = "run_manifest_v2.json"
EVENTS_FILENAME = "events.jsonl"

SKILL_TELEGRAM_FETCH = Path("skills/telegram-book-fetch/scripts/fetch_book_from_telegram_bot.py")
SKILL_CHAPTER_MENU = Path("skills/notebooklm-chapter-menu/scripts/run_chapter_menu.py")
SKILL_GUARDED_GENERATOR = Path("skills/notebooklm-guarded-generator/scripts/guarded_generate.py")
