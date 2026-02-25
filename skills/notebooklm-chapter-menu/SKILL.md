---
name: notebooklm-chapter-menu
description: Build and run a skill-first NotebookLM chapter infographic flow from EPUB prep to chapter menu, deterministic generation manifest, and optional publish to Obsidian/Notion.
---

# NotebookLM Chapter Menu

Run the chapter-menu flow with deterministic local scripts, then choose whether to stop at prep, generate infographics, or generate+publish.
This skill is self-contained: runner and pipeline scripts are resolved from `skills/notebooklm-chapter-menu/scripts/pipeline`.

## Workflow

1. Run `scripts/run_chapter_menu.py` with `--epub` to prepare ranked chapter artifacts and a chapter menu.
2. Pick chapter IDs from the JSON `menu` output (`chapter_id`, `title`, `score`).
3. Re-run with `--notebook-id` and optional `--chapter-ids` to generate infographics.
4. The generation step writes `<out-dir>/<run_id>/run_manifest.json` with explicit `source_map` and `artifacts` metadata.
5. Optional: add `--publish-after-generate` to publish the run to Obsidian and/or Notion.

## Defaults

- Deterministic mode is default: `--select-mode score`.
- Random selection is blocked unless `--allow-random` is set.
- Runtime defaults are loaded from `skills/notebooklm-chapter-menu/config/defaults.json` (or `--config` / `NOTEBOOKLM_CHAPTER_MENU_CONFIG`).
- `--workspace-root` controls default output locations only. If omitted, output root defaults to this skill directory.
- Optional env override for output root: `NOTEBOOKLM_CHAPTER_MENU_ROOT`.

## Commands

Prepare only (deterministic score mode default):
```bash
python3 skills/notebooklm-chapter-menu/scripts/run_chapter_menu.py \
  --config skills/notebooklm-chapter-menu/config/defaults.json \
  --epub /path/to/book.epub \
  --top-n 6 \
  --batch-size 3
```

Prepare and generate selected chapters:
```bash
python3 skills/notebooklm-chapter-menu/scripts/run_chapter_menu.py \
  --epub /path/to/book.epub \
  --notebook-id <NOTEBOOK_ID> \
  --chapter-ids 3,7,11 \
  --profile default
```

Generate and publish:
```bash
python3 skills/notebooklm-chapter-menu/scripts/run_chapter_menu.py \
  --epub /path/to/book.epub \
  --notebook-id <NOTEBOOK_ID> \
  --chapter-ids 3,7,11 \
  --publish-after-generate \
  --obsidian-vault-path ~/Documents/ObsidianVault \
  --notion-data-source-id <NOTION_DATA_SOURCE_ID>
```

Random selection is opt-in only:
```bash
python3 skills/notebooklm-chapter-menu/scripts/run_chapter_menu.py \
  --epub /path/to/book.epub \
  --select-mode random \
  --allow-random
```

Load `references/runbook.md` for parameter and output details.
