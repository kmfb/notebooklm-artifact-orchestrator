# Book-to-Artifact Clean Architecture

## Boundaries

- Entrypoint: `skills/book-to-artifact/scripts/run_book_to_artifact.py`
- Internal core: `skills/book-to-artifact/scripts/bookflow/core/`
  - `models.py`: manifest and artifact domain schema.
  - `state_machine.py`: allowed lifecycle transitions.
  - `io.py`: manifest/event persistence and robust JSON parsing.
  - `config.py`: workspace/session defaults and downstream skill paths.
  - `quality.py`: chapter extract quality scoring helpers.
- Internal adapters: `skills/book-to-artifact/scripts/bookflow/adapters/`
  - `telegram_fetch.py`: Telegram download stage.
  - `chapter_menu.py`: chapter prep and infographic stage.
  - `notebooklm_sources.py`: chapter-id to source-id resolution.
  - `guarded_gen.py`: non-infographic generation stage.
  - `runner.py`: subprocess execution and JSON contract handling.
- Internal store: `skills/book-to-artifact/scripts/bookflow/store/`
  - `db.py`: SQLite control-plane schema + repositories.

## Why no compatibility layer

This skill package is now self-contained so `.skill` packaging includes all runtime modules required by the orchestrator.

- No dependency on top-level `packages/bookflow_core` or `packages/bookflow_adapters`.
- No legacy `--auto-select-chapters` path.
- Notebook routing uses explicit strategy: `--notebook-strategy run|object|hybrid`.
- If `--chapter-ids` is absent, orchestration always stops at `awaiting_chapter_selection`.

The result is deterministic behavior and a single deployable boundary for book-to-artifact orchestration.
