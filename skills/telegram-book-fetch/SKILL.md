---
name: telegram-book-fetch
description: Download book files from Telegram bots (default @BookLib7890Bot) by title query, then return structured JSON with downloaded_path and metadata. Use when users ask to “下书/从 Telegram 机器人下载电子书/搜书并下载”.
---

# Telegram Book Fetch

Use `scripts/fetch_book_from_telegram_bot.py` to query a Telegram book bot, pick the best file by extension preference, and download it to a deterministic folder.

## Command

```bash
uv run --with telethon python3 skills/telegram-book-fetch/scripts/fetch_book_from_telegram_bot.py \
  --config skills/telegram-book-fetch/config/defaults.json \
  --query "Atomic Habits" \
  --bot @BookLib7890Bot
```

## Key flags

- `--query` (required): book title keyword.
- `--config`: defaults JSON path (default `skills/telegram-book-fetch/config/defaults.json`; env: `TELEGRAM_BOOK_FETCH_CONFIG`).
- `--bot`: bot username (default `@BookLib7890Bot`).
- `--session-file`: Telegram session path.
  Resolution order is: explicit `--session-file` > `TG_SESSION_FILE` env > `~/.openclaw/credentials/telegram/main`.
- `--telegram-config`: Telegram API config JSON path (fallback: `TG_CONFIG_PATH` env -> config file -> `~/.config/telegram-downloader/config.json`).
- `--output-root`: download root (fallback: config file -> `<skill-root>/data/telegram_book_fetch/downloads`).
- `--prefer`: extension preference order, default `epub,txt,md,pdf,docx,mobi,azw3`.

## Output contract

Script prints one JSON object:

- success: `{"status":"ok", "downloaded_path": "...", ...}`
- auth issue: `{"status":"auth_required", ...}`
- not found / failed: `{"status":"not_found"|"download_failed", ...}`

Use `downloaded_path` as downstream EPUB input when `status=ok`.

See `references/runbook.md` for troubleshooting and environment assumptions.
