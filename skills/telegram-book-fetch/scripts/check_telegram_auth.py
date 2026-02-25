#!/usr/bin/env python3
"""Check whether a Telegram session is authorized and emit JSON status."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Dict

from session_resolution import resolve_session_file

DEFAULT_CONFIG_PATH = "~/.config/telegram-downloader/config.json"


def load_telegram_config(config_path: str) -> Dict[str, object]:
    api_id = os.environ.get("TG_API_ID")
    api_hash = os.environ.get("TG_API_HASH")
    if api_id and api_hash:
        return {"api_id": int(api_id), "api_hash": api_hash}

    cfg_file = Path(config_path).expanduser()
    if cfg_file.exists():
        raw = json.loads(cfg_file.read_text(encoding="utf-8"))
        if raw.get("api_id") and raw.get("api_hash"):
            return {"api_id": int(raw["api_id"]), "api_hash": raw["api_hash"]}

    raise RuntimeError("Missing Telegram API credentials (TG_API_ID/TG_API_HASH or config file).")


async def check_auth(session_file: str, config_path: str) -> Dict[str, object]:
    try:
        cfg = load_telegram_config(config_path)
    except Exception as e:  # noqa: BLE001
        return {
            "status": "error",
            "session_file": session_file,
            "authorized": False,
            "reason": str(e),
        }

    try:
        from telethon import TelegramClient
    except ModuleNotFoundError:
        return {
            "status": "error",
            "session_file": session_file,
            "authorized": False,
            "reason": "telethon is required. Install it with: uv run --with telethon python3 ...",
        }

    client = TelegramClient(session_file, cfg["api_id"], cfg["api_hash"])
    try:
        await client.connect()
        authorized = bool(await client.is_user_authorized())
        if authorized:
            return {
                "status": "ok",
                "session_file": session_file,
                "authorized": True,
                "reason": "",
            }
        return {
            "status": "auth_required",
            "session_file": session_file,
            "authorized": False,
            "reason": "Telegram session not authorized.",
        }
    except Exception as e:  # noqa: BLE001
        return {
            "status": "error",
            "session_file": session_file,
            "authorized": False,
            "reason": str(e),
        }
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Check Telegram session authorization")
    p.add_argument("--session-file", default="")
    p.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Telegram config JSON path")
    return p


def main() -> None:
    args = build_parser().parse_args()
    session_file = resolve_session_file(args.session_file)
    result = asyncio.run(check_auth(session_file, args.config))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
