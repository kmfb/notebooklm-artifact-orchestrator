# Telegram Book Fetch Runbook

## Prerequisites

- Telegram API credentials in env or config:
  - `TG_API_ID` + `TG_API_HASH`, or
  - `--telegram-config` path (`TG_CONFIG_PATH` env / defaults JSON / `~/.config/telegram-downloader/config.json`)
- Defaults config path:
  - `skills/telegram-book-fetch/config/defaults.json` (or `--config` / `TELEGRAM_BOOK_FETCH_CONFIG`)
- Telegram session resolution order:
  - explicit `--session-file`
  - `TG_SESSION_FILE` env
  - default `~/.openclaw/credentials/telegram/main`
- Telethon runtime (recommended invocation):
  - `uv run --with telethon python3 ...`

## Basic usage

```bash
uv run --with telethon python3 skills/telegram-book-fetch/scripts/fetch_book_from_telegram_bot.py \
  --config skills/telegram-book-fetch/config/defaults.json \
  --query "Atomic Habits"
```

## Advanced usage

```bash
uv run --with telethon python3 skills/telegram-book-fetch/scripts/fetch_book_from_telegram_bot.py \
  --query "Atomic Habits" \
  --bot @BookLib7890Bot \
  --session-file /path/to/session \
  --output-root /path/to/downloads \
  --prefer epub,pdf,txt
```

## Result handling

- If `status=ok`: use `downloaded_path`.
- If `status=auth_required`: login Telegram account first with same session.
- If `status=not_found`: refine query keywords or switch bot.
- If `status=download_failed`: retry; bot may send stale links or transient failures.
