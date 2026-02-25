# NotebookLM Guarded Generator Runbook

## What this skill is for
Use this when NotebookLM generation is unstable (quota/risk-control windows, transient backend failures) and you need a **controlled fallback chain** instead of one-shot create calls.

## Operator workflow
1. Run preflight-only once.
2. Run guarded generation with your fallback plan.
3. Read JSON summary + events log.
4. If repeated failures open the circuit breaker, wait for cool-down or switch artifact type.

## Command templates

### 1) Preflight check
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --profile default \
  --dry-run
```

### 2) Normal guarded run (fallback: infographic → slides → report → audio)
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --profile default \
  --plan infographic,slides,report,audio \
  --max-success 1
```

### 3) Tight budget mode (risk-control days)
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --daily-budget-total 12 \
  --daily-budget-per-type "infographic:3,slides:3,report:3,audio:3"
```

### 4) Custom source set
```bash
python3 skills/notebooklm-guarded-generator/scripts/guarded_generate.py \
  --notebook-id <NOTEBOOK_ID> \
  --source-ids <SOURCE_ID_1>,<SOURCE_ID_2>
```

## Interpreting output
- `status=ok`: reached target success count.
- `status=degraded`: at least one success, but below target success count.
- `status=failed`: no success in this run.
- `status=failed_preflight`: blocked before attempting generation.

## Fail-fast behavior
Create step is considered failed immediately when CLI returns success but no `artifact_id` is present. This avoids fake "started" states and aligns with patched `nlm studio` behavior.

## Logs/state locations
Defaults:
- State: `~/.openclaw/state/notebooklm-guarded-generator/state.json`
- Events: `~/.openclaw/state/notebooklm-guarded-generator/events.jsonl`

See `references/state-schema.md` for fields.
