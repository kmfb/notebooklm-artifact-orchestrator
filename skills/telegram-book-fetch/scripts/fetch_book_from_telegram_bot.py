#!/usr/bin/env python3
"""
Fetch a book file by querying a Telegram bot (default: @BookLib7890Bot).

Robust polling mode (no Telethon conversation object):
1) Send title query
2) Poll bot chat for new messages
3) Auto-send /book... command or click best inline button
4) Pick best document by preferred extension and download
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

try:
    from telethon import TelegramClient
except ModuleNotFoundError as e:
    raise SystemExit(
        "telethon is required. Run with: uv run --with telethon python3 skills/telegram-book-fetch/scripts/fetch_book_from_telegram_bot.py ..."
    ) from e

from session_resolution import resolve_session_file


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_ROOT = str(ROOT / "data" / "telegram_book_fetch" / "downloads")
DEFAULT_TELEGRAM_CONFIG_PATH = "~/.config/telegram-downloader/config.json"
DEFAULT_CONFIG_PATH = ROOT / "config" / "defaults.json"
ENV_CONFIG_PATH = "TELEGRAM_BOOK_FETCH_CONFIG"


def _load_defaults(config_path: Path) -> Dict[str, object]:
    if not config_path.exists():
        return {}
    data = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a JSON object: {config_path}")
    return data


def _str_choice(*values: object) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def slugify(text: str, max_len: int = 80) -> str:
    s = text.strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff_-]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:max_len] or "book"


def normalize_ext_list(raw: str) -> List[str]:
    out = []
    for x in raw.split(","):
        x = x.strip().lower()
        if not x:
            continue
        if not x.startswith("."):
            x = f".{x}"
        out.append(x)
    return out or [".epub", ".txt", ".md", ".pdf", ".docx"]


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


def get_filename_from_message(msg) -> str:
    name = getattr(getattr(msg, "file", None), "name", None)
    if name:
        return str(name)

    mime = getattr(getattr(msg, "file", None), "mime_type", "") or ""
    ext = ""
    if "pdf" in mime:
        ext = ".pdf"
    elif "epub" in mime:
        ext = ".epub"
    elif "text" in mime:
        ext = ".txt"
    return f"message_{getattr(msg, 'id', 'unknown')}{ext}"


def get_ext(name: str) -> str:
    return Path(name).suffix.lower()


def is_nav_button(text: str) -> bool:
    t = text.strip().lower()
    nav_words = ["next", "prev", "back", "上一页", "下一页", "返回", "更多", "- 1 -"]
    return any(x in t for x in nav_words)


def tokenize(s: str) -> List[str]:
    return re.findall(r"[a-z0-9\u4e00-\u9fff]+", s.lower())


def ext_in_text(text: str) -> Optional[str]:
    t = text.lower()
    m = re.search(r"(?:\.|\b)(epub|pdf|txt|md|docx|mobi|azw3)\b", t)
    if m:
        return f".{m.group(1)}"
    return None


def score_button(text: str, query_tokens: Sequence[str], preferred_exts: Sequence[str]) -> Tuple[float, str]:
    t = text.strip()
    tl = t.lower()

    if is_nav_button(t):
        return -100.0, "nav_button"

    score = 0.0
    reason = "fallback"

    ext = ext_in_text(t)
    if ext and ext in preferred_exts:
        score += 200 - preferred_exts.index(ext) * 10
        reason = f"preferred_ext:{ext}"
    elif ext:
        score += 120
        reason = f"other_ext:{ext}"

    btokens = set(tokenize(tl))
    overlap = len(set(query_tokens) & btokens)
    if overlap > 0:
        score += 20 + overlap * 3
        if reason == "fallback":
            reason = f"title_overlap:{overlap}"

    if re.search(r"\b(upload|size|mb|kb|year|作者|出版社|isbn)\b", tl):
        score -= 5

    return score, reason


def flatten_buttons(msg) -> List[Tuple[int, int, str]]:
    out: List[Tuple[int, int, str]] = []
    rows = getattr(msg, "buttons", None) or []
    for i, row in enumerate(rows):
        for j, btn in enumerate(row):
            text = (getattr(btn, "text", "") or "").strip()
            if text:
                out.append((i, j, text))
    return out


def summarize_message(msg) -> Dict[str, object]:
    text = (getattr(msg, "raw_text", "") or "").strip()
    filename = get_filename_from_message(msg) if getattr(msg, "file", None) else None
    size = getattr(getattr(msg, "file", None), "size", None)
    return {
        "message_id": getattr(msg, "id", None),
        "date": str(getattr(msg, "date", "")),
        "text": text[:320],
        "has_buttons": bool(getattr(msg, "buttons", None)),
        "file_name": filename,
        "file_size": size,
    }


def extract_book_commands(text: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for line in (text or "").splitlines():
        m = re.search(r"(\/book[\w\d_]+)\s*\(([^\)]+)\)", line)
        if not m:
            continue
        cmd = m.group(1)
        meta = m.group(2).strip().lower()
        ext = ""
        m2 = re.match(r"(epub|pdf|txt|md|docx|mobi|azw3)\b", meta)
        if m2:
            ext = f".{m2.group(1)}"
        out.append({"command": cmd, "ext": ext, "line": line.strip()})
    return out


def choose_best_command(items: Sequence[Dict[str, str]], preferred_exts: Sequence[str]) -> Optional[Dict[str, str]]:
    if not items:
        return None
    scored = []
    for it in items:
        ext = it.get("ext", "")
        rank = preferred_exts.index(ext) if ext in preferred_exts else len(preferred_exts) + 5
        scored.append((rank, it))
    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def choose_best_document(candidates: Sequence, preferred_exts: Sequence[str]):
    scored = []
    for msg in candidates:
        filename = get_filename_from_message(msg)
        ext = get_ext(filename)
        ext_rank = preferred_exts.index(ext) if ext in preferred_exts else len(preferred_exts) + 5
        size = int(getattr(getattr(msg, "file", None), "size", 0) or 0)
        scored.append((ext_rank, -size, msg))
    scored.sort(key=lambda x: (x[0], x[1]))
    return scored[0][2] if scored else None


async def fetch(args) -> Dict[str, object]:
    cfg = load_telegram_config(args.telegram_config)
    preferred_exts = normalize_ext_list(args.prefer)
    query_tokens = tokenize(args.query)

    client = TelegramClient(args.session_file, cfg["api_id"], cfg["api_hash"])
    await client.connect()

    transcript: List[Dict[str, object]] = []
    clicked: List[Dict[str, object]] = []
    candidates = []

    try:
        if not await client.is_user_authorized():
            return {
                "status": "auth_required",
                "reason": "Telegram session not authorized",
                "checked_at": now_iso(),
            }

        bot = await client.get_entity(args.bot)
        latest = await client.get_messages(bot, limit=1)
        baseline_id = latest[0].id if latest else 0

        await client.send_message(bot, args.query)
        sent_cmd_sigs = set()
        clicked_sigs = set()
        seen_ids = set()

        deadline = time.time() + args.total_timeout
        last_candidate_at = None

        while time.time() < deadline:
            msgs = await client.get_messages(bot, limit=args.poll_limit)
            new_msgs = [m for m in reversed(msgs) if m.id > baseline_id and m.id not in seen_ids]

            if not new_msgs:
                if candidates and last_candidate_at and time.time() - last_candidate_at > args.candidate_grace:
                    break
                await asyncio.sleep(args.poll_interval)
                continue

            for msg in new_msgs:
                seen_ids.add(msg.id)
                transcript.append(summarize_message(msg))
                if getattr(msg, "file", None):
                    candidates.append(msg)
                    last_candidate_at = time.time()

            # first: /book command pattern
            command_sent = False
            for msg in new_msgs:
                text = (getattr(msg, "raw_text", "") or "").strip()
                commands = extract_book_commands(text)
                if not commands:
                    continue
                best_cmd = choose_best_command(commands, preferred_exts)
                if not best_cmd:
                    continue

                cmd_sig = f"{msg.id}:{best_cmd['command']}"
                if cmd_sig in sent_cmd_sigs:
                    continue

                sent_cmd_sigs.add(cmd_sig)
                await client.send_message(bot, best_cmd["command"])
                clicked.append(
                    {
                        "message_id": msg.id,
                        "action": "send_command",
                        "command": best_cmd["command"],
                        "reason": f"command_match_ext:{best_cmd.get('ext') or 'unknown'}",
                    }
                )
                command_sent = True
                break

            if command_sent:
                await asyncio.sleep(args.poll_interval)
                continue

            # second: inline button click
            clicked_this_round = False
            for msg in reversed(new_msgs):
                btns = flatten_buttons(msg)
                if not btns:
                    continue

                scored = []
                for i, j, text in btns:
                    s, reason = score_button(text, query_tokens, preferred_exts)
                    scored.append((s, i, j, text, reason))
                scored.sort(key=lambda x: x[0], reverse=True)
                if not scored:
                    continue

                top = scored[0]
                if top[0] < 0:
                    continue

                sig = f"{msg.id}:{top[1]}:{top[2]}"
                if sig in clicked_sigs:
                    continue

                clicked_sigs.add(sig)
                await msg.click(i=top[1], j=top[2])
                clicked.append(
                    {
                        "message_id": msg.id,
                        "action": "click_button",
                        "button": top[3],
                        "score": top[0],
                        "reason": top[4],
                    }
                )
                clicked_this_round = True
                break

            if clicked_this_round:
                await asyncio.sleep(args.poll_interval)
                continue

            if candidates and last_candidate_at and time.time() - last_candidate_at > args.candidate_grace:
                break

            await asyncio.sleep(args.poll_interval)

        if not candidates:
            return {
                "status": "not_found",
                "reason": "No downloadable document received from bot",
                "query": args.query,
                "bot": args.bot,
                "clicked": clicked,
                "transcript": transcript[-40:],
                "checked_at": now_iso(),
            }

        chosen = choose_best_document(candidates, preferred_exts)
        if not chosen:
            return {
                "status": "not_found",
                "reason": "No suitable file candidate",
                "query": args.query,
                "bot": args.bot,
                "clicked": clicked,
                "transcript": transcript[-40:],
                "checked_at": now_iso(),
            }

        original_name = get_filename_from_message(chosen)
        ext = get_ext(original_name)
        safe_name = re.sub(r"[\\/:*?\"<>|]", "_", original_name)

        save_root = Path(args.output_root).expanduser().resolve() / slugify(args.query)
        save_root.mkdir(parents=True, exist_ok=True)
        out_path = save_root / safe_name
        if out_path.exists():
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            out_path = save_root / f"{out_path.stem}-{ts}{out_path.suffix}"

        downloaded = await chosen.download_media(file=str(out_path))
        if not downloaded or not Path(downloaded).exists():
            return {
                "status": "download_failed",
                "reason": "download_media returned empty path",
                "query": args.query,
                "bot": args.bot,
                "clicked": clicked,
                "checked_at": now_iso(),
            }

        cand_summary = []
        for msg in candidates:
            fname = get_filename_from_message(msg)
            cand_summary.append(
                {
                    "message_id": getattr(msg, "id", None),
                    "file_name": fname,
                    "ext": get_ext(fname),
                    "size": int(getattr(getattr(msg, "file", None), "size", 0) or 0),
                }
            )

        return {
            "status": "ok",
            "query": args.query,
            "bot": args.bot,
            "preferred_exts": preferred_exts,
            "downloaded_path": str(Path(downloaded).resolve()),
            "downloaded_name": Path(downloaded).name,
            "downloaded_ext": ext,
            "downloaded_size": Path(downloaded).stat().st_size,
            "clicked": clicked,
            "candidates": cand_summary,
            "checked_at": now_iso(),
        }

    finally:
        await client.disconnect()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch a book file by querying a Telegram bot")
    p.add_argument("--query", required=True, help="Book title query")
    p.add_argument("--config", default="", help="JSON config path (default: skills/telegram-book-fetch/config/defaults.json)")
    p.add_argument("--bot", default="@BookLib7890Bot", help="Target Telegram bot username")
    p.add_argument("--session-file", default="")
    p.add_argument("--telegram-config", default="", help="Telegram API config JSON path")
    p.add_argument("--output-root", default="")
    p.add_argument(
        "--prefer",
        default="epub,txt,md,pdf,docx,mobi,azw3",
        help="Preferred extension order (comma-separated)",
    )
    p.add_argument("--total-timeout", type=float, default=120.0)
    p.add_argument("--poll-interval", type=float, default=1.5)
    p.add_argument("--poll-limit", type=int, default=30)
    p.add_argument("--candidate-grace", type=float, default=4.0)
    return p


def main() -> None:
    args = build_parser().parse_args()

    config_path = Path(
        _str_choice(args.config, os.environ.get(ENV_CONFIG_PATH, ""))
        or str(DEFAULT_CONFIG_PATH)
    ).expanduser().resolve()
    defaults = _load_defaults(config_path)

    args.telegram_config = _str_choice(
        args.telegram_config,
        os.environ.get("TG_CONFIG_PATH", ""),
        str(defaults.get("telegram_config_path") or ""),
        DEFAULT_TELEGRAM_CONFIG_PATH,
    )
    args.output_root = _str_choice(
        args.output_root,
        str(defaults.get("output_root") or ""),
        DEFAULT_OUTPUT_ROOT,
    )
    args.session_file = resolve_session_file(args.session_file)

    result = asyncio.run(fetch(args))
    result["config_path"] = str(config_path)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
