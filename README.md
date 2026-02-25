# notebooklm-artifact-orchestrator

An open-source orchestration workspace for NotebookLM artifact workflows.

It packages a practical multi-skill pipeline:
- `telegram-book-fetch` (optional source fetch)
- `notebooklm-chapter-menu` (chapter prep + infographic path)
- `notebooklm-guarded-generator` (fallback-safe generation for slides/report/audio)
- `book-to-artifact` (thin orchestrator across the skills above)

## What this repo provides

- Source code for all 4 skills
- Config templates (`config/defaults.json`) with no private credentials
- A strict packaging script to produce installable `.skill` bundles
- CI checks for hygiene (cache files + sensitive pattern scan + compile)

## Quick start

### 1) Package a clean bundle

```bash
python3 scripts/package_notebooklm_artifact_orchestrator_bundle.py \
  --workspace-root . \
  --output-dir dist/notebooklm-artifact-orchestrator-bundle
```

### 2) Install skills (recommended order)

1. `telegram-book-fetch`
2. `notebooklm-chapter-menu`
3. `notebooklm-guarded-generator`
4. `book-to-artifact`

Each `.skill` is a zip archive with the skill folder at root.

## Runtime requirements

- Python 3.10+
- `nlm` CLI authenticated for NotebookLM flows
- `uv` + `telethon` if using Telegram fetch
- Optional Notion / Google Drive credentials for publish sinks

## Config model

Each skill exposes a `config/defaults.json` template.
Use one of:
- explicit `--config <path>`
- environment variable override (skill-specific)
- defaults JSON file in the skill

No personal IDs/tokens are committed in this repository.

## Security notes

Before release, run strict packaging and check `bundle_audit_report.json`.

If you find a security issue, see `SECURITY.md`.
