---
name: notebooklm-guarded-generator
description: Guard NotebookLM artifact creation with preflight checks, daily quota/budget limits, per-artifact circuit breaker, fail-fast create validation, and fallback generation chains (e.g., infographic→slides→report→audio). Use when NotebookLM is unstable, when infographic is risk-controlled/quota-gated, or when you need reliable non-blocking generation with observable JSON logs/state.
---

# NotebookLM Guarded Generator

Run NotebookLM generation with safety rails so a single backend rejection does not break the whole flow.

## Use this workflow

1. Run preflight first to verify `nlm`, auth, and source availability.
2. Run guarded generation with a fallback chain.
3. Read JSON summary and event log.
4. If breaker is open for one artifact type, let cooldown expire or switch priority order.

## Commands

### Preflight only
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --profile default \
  --dry-run
```

### Default guarded run
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --profile default \
  --plan infographic,slides,report,audio \
  --max-success 1
```

### Risk-control day (tight budget)
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --daily-budget-total 12 \
  --daily-budget-per-type "infographic:3,slides:3,report:3,audio:3"
```

### Chapter-specific sources
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --source-ids <SOURCE_ID_1>,<SOURCE_ID_2>
```

## Behavioral contract

- Treat missing `artifact_id` after create as immediate failure (fail-fast).
- Count attempts against daily budget.
- Open breaker per artifact type after consecutive failures.
- Continue fallback chain while budget and breaker allow.
- Emit machine-readable summary JSON.

## Output and state

Default files:
- `~/.openclaw/state/notebooklm-guarded-generator/state.json`
- `~/.openclaw/state/notebooklm-guarded-generator/events.jsonl`

Load details only when needed:
- Runbook: `references/runbook.md`
- State fields: `references/state-schema.md`

## Scripts

- `scripts/guarded_generate.py`: main guarded runner (preflight + budget + breaker + fallback + observability)
