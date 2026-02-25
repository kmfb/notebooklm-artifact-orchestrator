---
name: book-to-artifact
description: "Orchestrate end-to-end book processing with optional Telegram fetch, chapter prep/menu, then NotebookLM artifact generation (infographic/slides/report/audio). Use when users ask for one command to run the full pipeline across multiple artifact types."
---

# Book to Artifact

Use this skill as a thin orchestrator over existing skills:
- `telegram-book-fetch` (optional fetch)
- `notebooklm-chapter-menu` (prep + infographic)
- `notebooklm-guarded-generator` (non-infographic artifacts)

Internal architecture:
- Core domain/state/io lives in `skills/book-to-artifact/scripts/bookflow/core`.
- External process calls/parsing live in `skills/book-to-artifact/scripts/bookflow/adapters`.
- The orchestration entrypoint is `skills/book-to-artifact/scripts/run_book_to_artifact.py`.

## Command

```bash
python3 skills/book-to-artifact/scripts/run_book_to_artifact.py \
  --config skills/book-to-artifact/config/defaults.json \
  --epub /path/to/book.epub \
  --notebook-strategy run \
  --artifact-plan infographic,slides,report,audio
```

Fetch + prepare menu (default waits for manual chapter selection):

```bash
python3 skills/book-to-artifact/scripts/run_book_to_artifact.py \
  --book-title "Atomic Habits" \
  --notebook-strategy run
```

Then run generation with explicit chapters:

```bash
python3 skills/book-to-artifact/scripts/run_book_to_artifact.py \
  --book-title "Atomic Habits" \
  --notebook-strategy run \
  --chapter-ids 6,50,77 \
  --artifact-plan infographic,slides,audio
```

## Notes

- `--book-title` mode uses `uv run --with telethon` first, then falls back to `python3`.
- `infographic` goes through chapter-menu generation path.
- Non-infographic types (`slides/report/audio/...`) go through guarded generator with selected chapter source IDs.
- Default behavior is **manual chapter selection**: if `--chapter-ids` is omitted, the run stops after menu output with `status=awaiting_chapter_selection`.
- Output is a v2 run manifest JSON with stage payloads, artifact records, and next-action guidance.
- Runtime defaults come from `skills/book-to-artifact/config/defaults.json` (or `--config` / `BOOKFLOW_CONFIG`).
- Google Drive upload behavior is controlled by `gdrive_enabled` + `gdrive_folder_id` in config, and can be overridden with `--gdrive-enabled/--no-gdrive`.

## Task visibility (v1)

This skill now writes to a **generic task board layer**.

Preferred command:

```bash
python3 scripts/task_board.py --provider bookflow --limit 40
```

Heartbeat-friendly mode (prints `HEARTBEAT_OK` when no actionable items):

```bash
python3 scripts/task_board.py --provider bookflow --heartbeat
```

See `references/runbook.md` for flags, assumptions, and dependency-bundle packaging, and `docs/bookflow/ARCHITECTURE.md` for package boundaries.
