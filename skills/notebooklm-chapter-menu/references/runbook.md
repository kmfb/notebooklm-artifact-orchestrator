# NotebookLM Chapter Menu Runbook

## Inputs

- `--epub`: source EPUB for chapter extraction and ranking.
- `--ranked-json`: optional existing `chapters_ranked.json` to skip prep.
- `--config`: optional JSON config path (default: `skills/notebooklm-chapter-menu/config/defaults.json`; env: `NOTEBOOKLM_CHAPTER_MENU_CONFIG`).
- `--workspace-root`: optional base output root. Fallback order: `--workspace-root` -> config `workspace_root` -> `NOTEBOOKLM_CHAPTER_MENU_ROOT` -> skill directory.
- `--select-mode`: defaults to `score`.
- `--allow-random`: required when `--select-mode random`.
- `--notebook-id`: enable generation step when provided.
- `--chapter-ids`: comma-separated IDs; if omitted, selected chapters from ranked JSON are used.
- `--source-map-json`: optional JSON map or prior `run_manifest.json` with explicit chapter-to-source mapping.
- `--publish-after-generate`: optional publish step after generation.
- `--obsidian-vault-path`: optional local vault path for markdown index + attachments.
- `--notion-data-source-id`: optional Notion data source ID for run index upsert.

## Pipeline Reuse

Runner delegates to:

- `scripts/pipeline/run_image_first_pipeline.py`
- `scripts/pipeline/notebooklm_chapter_infographic_run.py`
- `scripts/pipeline/notebooklm_publish_run.py` (optional)

All pipeline scripts are resolved relative to `scripts/run_chapter_menu.py`; no external workspace script paths are used.

## Default Output Locations

- Prep extraction default: `<workspace-root>/data/notebooklm_pipeline/<issue-label>/`
- Generate default: `<workspace-root>/tmp/notebooklm_poc/chapter-menu/`
- Direct generator default (`notebooklm_chapter_infographic_run.py`): `<workspace-root>/tmp/notebooklm_poc/chapter-infographic-artifacts/`

## Generation Output Schema

Generation returns a unified schema and writes the same payload to:

- `<infographic-out-dir>/<run_id>/run_manifest.json`

Required fields:

- `schema_version`
- `run_id`
- `started_at`
- `finished_at`
- `notebook_id`
- `selected_chapters`
- `source_map`
- `artifacts[]`

Each artifact includes:

- `chapter_id`
- `source_id`
- `artifact_id`
- `status`
- `path`
- `size`
- `error`

## Chapter-Menu Output Contract

Top-level JSON keys:

- `status`: `prepared`, `ok`, or `partial`
- `workspace_root`: resolved output root
- `config_path`: resolved defaults JSON path
- `ranked_json`: resolved ranked artifact path
- `menu`: selected chapter list (`chapter_id`, `title`, `score`, `char_count`)
- `steps.prepare`: prep step JSON (if prep ran)
- `steps.generate`: generation manifest payload (if generation ran)
- `steps.publish`: publish payload (if publish ran)
