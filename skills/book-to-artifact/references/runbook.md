# Book to Artifact Runbook

Architecture references:
- `docs/bookflow/ARCHITECTURE.md`
- `docs/bookflow/RUN_MANIFEST_V2.md`
- `skills/book-to-artifact/references/architecture.md`

## Inputs

- `--book-title`: optional title query for Telegram fetch.
- `--epub`: local EPUB path.
- `--ranked-json`: skip prep and reuse existing ranked chapters JSON.
- `--notebook-strategy`: `run|object|hybrid` (default `run`).
- `--run-notebook-id`: optional explicit run notebook ID.
- `--object-notebook-id`: optional explicit object notebook ID.
- `--chapter-ids`: optional comma-separated IDs.
  - If omitted, runner stops after prepare with `status=awaiting_chapter_selection`.
- `--artifact-plan`: comma-separated artifact types, default `infographic,slides,report,audio`.
- `--max-per-bucket`: default `0` in this orchestrator (disabled), to avoid over-constraining books with flat/identical titles.
- `--profile`: NotebookLM profile, default `default`.
- `--workspace-root`: workspace root (default: `OPENCLAW_WORKSPACE` or current working directory).
- `--config`: optional JSON config path (default: `skills/book-to-artifact/config/defaults.json`, overridable via `BOOKFLOW_CONFIG`).
- `BOOKFLOW_DB_PATH`: optional DB override (default `~/.openclaw/state/bookflow/bookflow.db`).

Pass-through generation/publish flags:
- `--publish-after-generate` (default ON)
- `--no-publish-after-generate`
- `--obsidian-vault-path`
- `--notion-data-source-id` (override; otherwise resolved from env/config)
- `--gdrive-enabled` / `--no-gdrive` (override; otherwise resolved from env/config)
- `--gdrive-folder-id` (override; otherwise resolved from env/config)

## Stage behavior

1. Fetch stage (optional)
   - Triggered only when `--book-title` is provided and both `--epub/--ranked-json` are absent.
   - Calls `skills/telegram-book-fetch/scripts/fetch_book_from_telegram_bot.py`.

2. Prepare stage
   - Calls `skills/notebooklm-chapter-menu/scripts/run_chapter_menu.py` in prepare mode.
   - Produces `ranked_json` and menu.

3. Infographic stage (optional)
   - If `artifact-plan` contains `infographic`, calls chapter-menu generation path using resolved active notebook.

4. Non-infographic stage (optional)
   - Resolves chapter->source mapping with **DB-first** strategy:
     1) look up prior successful mappings in SQLite for `(asset_id + active_notebook_id + chapter_id)`;
     2) only for cache misses, call `nlm source list --json` fallback parsing.
   - Calls `skills/notebooklm-guarded-generator/scripts/guarded_generate.py` with filtered plan.

5. Notebook strategy resolution
   - `run`: uses run notebook for generation; creates one with `nlm notebook create` if omitted.
   - `object`: uses object notebook keyed by **EPUB content hash** (when EPUB is known); creates one if none exists.
   - `hybrid`: ensures both notebooks exist; run notebook is active for generation.
   - Object notebook mappings and run notebook mappings are persisted in SQLite control-plane.

6. Manifest/event persistence
   - Writes `tmp/book-to-artifact/<run_id>/run_manifest_v2.json`.
   - Writes `tmp/book-to-artifact/<run_id>/events.jsonl`.

7. SQLite control-plane persistence
   - Writes `assets`, `object_notebooks`, `runs`, `run_notebooks`, `run_sources`, `artifacts`.
   - DB path defaults to `~/.openclaw/state/bookflow/bookflow.db`.

## Task board (visibility)

Bookflow now plugs into the **generic task board** layer.

Preferred command:

```bash
python3 scripts/task_board.py --provider bookflow --limit 40
```

Heartbeat mode (only alerts when actionable, else `HEARTBEAT_OK`):

```bash
python3 scripts/task_board.py --provider bookflow --heartbeat
```

## Assumptions

- `nlm` CLI is installed and authenticated.
- For Telegram fetch: `uv` + `telethon`, and Telegram credentials/session are available.
- For Notion/Obsidian publish: downstream chapter-menu publish requirements still apply.

## Dependency bundle packaging

Package this skill together with required dependencies:

```bash
python3 scripts/package_book_to_artifact_bundle.py \
  --workspace-root . \
  --output-dir dist/book-to-artifact-bundle
```

Output includes:
- `book-to-artifact-bundle.zip` (all `.skill` files + manifest)
- `bundle_audit_report.json` (sensitive-pattern checks + cache cleanup report)
