# State Schema

This skill keeps lightweight runtime state for quota and breaker decisions.

## `state.json`

```json
{
  "schema_version": 1,
  "daily": {
    "date": "2026-02-13",
    "total_used": 4,
    "per_type": {
      "infographic": 2,
      "slides": 1,
      "report": 1
    }
  },
  "breaker": {
    "infographic": {
      "consecutive_failures": 3,
      "open_until": "2026-02-13T15:40:00+08:00",
      "last_failure_at": "2026-02-13T14:10:22+08:00",
      "last_success_at": "2026-02-12T19:20:11+08:00"
    }
  },
  "last_run": {
    "at": "2026-02-13T14:11:01+08:00",
    "status": "degraded",
    "notebook_id": "<id>",
    "plan": ["infographic", "slides", "report", "audio"],
    "success_count": 1,
    "attempt_count": 3
  }
}
```

### Notes
- `daily` resets automatically when local date changes.
- `total_used` and `per_type` count **attempts**, not only successes.
- Breaker opens when `consecutive_failures >= --breaker-consecutive-failures`.
- Open duration is controlled by `--breaker-open-minutes`.

## `events.jsonl`
One JSON record per line for observability and debugging.

Typical event types:
- `preflight`
- `skip`
- `create_failed`
- `create_failed_no_artifact`
- `completed`
- `failed`
- `timeout`

Fields vary by event but always include:
- `ts`
- `event`
- `artifact_type` (except preflight)
