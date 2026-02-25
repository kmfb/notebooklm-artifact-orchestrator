#!/usr/bin/env python3
"""
Guarded NotebookLM artifact generation with:
- preflight checks
- daily budget/quota guard
- per-artifact circuit breaker
- fallback artifact chain
- JSON observability output + event logs
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_PLAN = "infographic,slides,report,audio"
STATUS_MAP = {1: "in_progress", 3: "completed", 4: "failed"}
SUCCESS_STATES = {"completed", "done", "ready", "succeeded"}
FAIL_STATES = {"failed", "error"}


def now_local() -> datetime:
    return datetime.now().astimezone()


def now_iso() -> str:
    return now_local().isoformat()


def run(cmd: List[str], timeout: int = 240) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=os.environ.copy(),
    )


def jload(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass

    # Some nlm outputs include extra lines around JSON. Parse last JSON-like line.
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if ln.startswith("{") or ln.startswith("["):
            try:
                return json.loads(ln)
            except Exception:
                continue
    return None


def _is_auth_error(p: subprocess.CompletedProcess) -> bool:
    msg = ((p.stderr or "") + "\n" + (p.stdout or "")).lower()
    keys = [
        "no authentication found",
        "please run: nlm login",
        "authentication expired",
        "profile not found",
        "login required",
    ]
    return any(k in msg for k in keys)


def _is_transient_net_error(p: subprocess.CompletedProcess) -> bool:
    msg = ((p.stderr or "") + "\n" + (p.stdout or "")).lower()
    keys = [
        "unexpected_eof_while_reading",
        "connecterror",
        "connection reset",
        "timed out",
        "temporary failure",
        "network is unreachable",
        "502",
        "503",
        "504",
    ]
    return any(k in msg for k in keys)


def refresh_auth_from_cdp(profile: str) -> bool:
    p = run(
        [
            "nlm",
            "login",
            "--profile",
            profile,
            "--provider",
            "openclaw",
            "--cdp-url",
            "http://127.0.0.1:18800",
        ],
        timeout=180,
    )
    if p.returncode != 0:
        return False
    chk = run(["nlm", "login", "--check", "--profile", profile], timeout=120)
    return chk.returncode == 0


def run_nlm(
    cmd: List[str],
    profile: str,
    timeout: int = 240,
    attempts: int = 3,
    auto_refresh_auth: bool = True,
) -> subprocess.CompletedProcess:
    last: Optional[subprocess.CompletedProcess] = None
    for i in range(attempts):
        p = run(cmd, timeout=timeout)
        last = p
        if p.returncode == 0:
            return p

        if auto_refresh_auth and _is_auth_error(p):
            if refresh_auth_from_cdp(profile=profile):
                p2 = run(cmd, timeout=timeout)
                last = p2
                if p2.returncode == 0:
                    return p2

        if _is_transient_net_error(p) and i < attempts - 1:
            time.sleep(2 * (i + 1))
            continue

        return p

    assert last is not None
    return last


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, obj: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_csv_ids(raw: str) -> List[str]:
    return [x.strip() for x in (raw or "").split(",") if x.strip()]


def parse_plan(raw: str) -> List[str]:
    aliases = {
        "data_table": "data-table",
        "datatable": "data-table",
        "slide_deck": "slides",
        "mind_map": "mindmap",
    }
    out: List[str] = []
    for item in [x.strip().lower() for x in (raw or "").split(",") if x.strip()]:
        out.append(aliases.get(item, item))
    return out


def parse_budget_per_type(raw: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if not raw:
        return out
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip().lower()
        try:
            out[k] = int(v.strip())
        except ValueError:
            continue
    return out


def _items_from_any(js: Any, keys: List[str]) -> List[Dict[str, Any]]:
    if isinstance(js, list):
        return [x for x in js if isinstance(x, dict)]
    if isinstance(js, dict):
        for k in keys:
            v = js.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        # fallback: list-like values nested one level
        rows: List[Dict[str, Any]] = []
        for v in js.values():
            if isinstance(v, list):
                rows.extend([x for x in v if isinstance(x, dict)])
        if rows:
            return rows
    return []


def extract_artifact_id(raw_output: str) -> Optional[str]:
    js = jload(raw_output)
    if isinstance(js, dict):
        for key in ("artifact_id", "id"):
            val = js.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        for k in ("artifact", "result", "data"):
            val = js.get(k)
            if isinstance(val, dict):
                cand = val.get("artifact_id") or val.get("id")
                if isinstance(cand, str) and cand.strip():
                    return cand.strip()
    if isinstance(js, list):
        for row in js:
            if isinstance(row, dict):
                cand = row.get("artifact_id") or row.get("id")
                if isinstance(cand, str) and cand.strip():
                    return cand.strip()

    text = raw_output or ""
    m = re.search(r"Artifact ID:\s*([0-9a-fA-F-]{36})", text, re.I)
    if m:
        return m.group(1)

    m = re.search(r"\b([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\b", text)
    if m:
        return m.group(1)

    return None


def normalize_status(raw: Any) -> str:
    if raw is None:
        return "unknown"
    if isinstance(raw, int):
        return STATUS_MAP.get(raw, str(raw))
    text = str(raw).strip().lower()
    alias = {
        "complete": "completed",
        "success": "completed",
        "succeeded": "completed",
        "in progress": "in_progress",
        "running": "in_progress",
    }
    return alias.get(text, text)


def find_artifact_row(rows: List[Dict[str, Any]], artifact_id: str) -> Optional[Dict[str, Any]]:
    for row in rows:
        rid = row.get("id") or row.get("artifact_id")
        if isinstance(rid, str) and rid == artifact_id:
            return row
    return None


def load_source_ids(notebook_id: str, profile: str, auto_refresh_auth: bool) -> Tuple[List[str], Optional[str]]:
    p = run_nlm(
        ["nlm", "source", "list", notebook_id, "--json", "--profile", profile],
        profile=profile,
        timeout=120,
        auto_refresh_auth=auto_refresh_auth,
    )
    if p.returncode != 0:
        return [], (p.stderr or p.stdout).strip()
    js = jload(p.stdout)
    rows = _items_from_any(js, ["sources", "items", "results", "data"])
    ids = [str(r.get("id")) for r in rows if r.get("id")]
    return ids, None


def load_studio_rows(notebook_id: str, profile: str, auto_refresh_auth: bool) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    p = run_nlm(
        ["nlm", "studio", "status", notebook_id, "--full", "--json", "--profile", profile],
        profile=profile,
        timeout=120,
        auto_refresh_auth=auto_refresh_auth,
    )
    if p.returncode != 0:
        return [], (p.stderr or p.stdout).strip()
    js = jload(p.stdout)
    rows = _items_from_any(js, ["artifacts", "items", "results", "data"])
    return rows, None


def build_create_cmd(artifact_type: str, notebook_id: str, source_ids: List[str], profile: str) -> List[str]:
    cmd = ["nlm", artifact_type, "create", notebook_id, "--confirm", "--profile", profile]
    if source_ids:
        cmd += ["--source-ids", ",".join(source_ids)]
    return cmd


def default_state() -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "daily": {
            "date": now_local().date().isoformat(),
            "total_used": 0,
            "per_type": {},
        },
        "breaker": {},
        "last_run": {},
    }


def normalize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(state, dict):
        state = {}
    if "schema_version" not in state:
        state["schema_version"] = 1
    if "daily" not in state or not isinstance(state["daily"], dict):
        state["daily"] = {"date": now_local().date().isoformat(), "total_used": 0, "per_type": {}}
    state["daily"].setdefault("date", now_local().date().isoformat())
    state["daily"].setdefault("total_used", 0)
    state["daily"].setdefault("per_type", {})
    if "breaker" not in state or not isinstance(state["breaker"], dict):
        state["breaker"] = {}
    if "last_run" not in state or not isinstance(state["last_run"], dict):
        state["last_run"] = {}
    return state


def maybe_reset_daily(state: Dict[str, Any]) -> None:
    today = now_local().date().isoformat()
    if state["daily"].get("date") != today:
        state["daily"] = {"date": today, "total_used": 0, "per_type": {}}


def consume_budget(state: Dict[str, Any], artifact_type: str) -> None:
    state["daily"]["total_used"] = int(state["daily"].get("total_used", 0)) + 1
    per = state["daily"].setdefault("per_type", {})
    per[artifact_type] = int(per.get(artifact_type, 0)) + 1


def budget_allowed(
    state: Dict[str, Any],
    artifact_type: str,
    daily_budget_total: int,
    daily_budget_per_type: Dict[str, int],
) -> Tuple[bool, Optional[str]]:
    used_total = int(state["daily"].get("total_used", 0))
    if daily_budget_total > 0 and used_total >= daily_budget_total:
        return False, "daily_total_budget_exhausted"

    type_limit = daily_budget_per_type.get(artifact_type)
    used_type = int(state["daily"].get("per_type", {}).get(artifact_type, 0))
    if type_limit is not None and type_limit >= 0 and used_type >= type_limit:
        return False, f"daily_{artifact_type}_budget_exhausted"

    return True, None


def breaker_status(state: Dict[str, Any], artifact_type: str) -> Tuple[bool, Optional[str]]:
    now = now_local()
    b = state.get("breaker", {}).get(artifact_type, {})
    open_until = b.get("open_until")
    if not open_until:
        return False, None
    try:
        dt = datetime.fromisoformat(open_until)
    except Exception:
        return False, None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone()
    if dt > now:
        seconds = int((dt - now).total_seconds())
        return True, f"breaker_open_{seconds}s"
    return False, None


def breaker_record_success(state: Dict[str, Any], artifact_type: str) -> None:
    b = state.setdefault("breaker", {}).setdefault(artifact_type, {})
    b["consecutive_failures"] = 0
    b["open_until"] = None
    b["last_failure_at"] = b.get("last_failure_at")
    b["last_success_at"] = now_iso()


def breaker_record_failure(
    state: Dict[str, Any],
    artifact_type: str,
    threshold: int,
    open_minutes: int,
) -> None:
    b = state.setdefault("breaker", {}).setdefault(artifact_type, {})
    cf = int(b.get("consecutive_failures", 0)) + 1
    b["consecutive_failures"] = cf
    b["last_failure_at"] = now_iso()
    if threshold > 0 and cf >= threshold:
        b["open_until"] = (now_local() + timedelta(minutes=open_minutes)).isoformat()


def poll_artifact_status(
    notebook_id: str,
    artifact_id: str,
    profile: str,
    poll_seconds: int,
    max_polls: int,
    auto_refresh_auth: bool,
) -> Tuple[str, Optional[str], Optional[Dict[str, Any]]]:
    last_status = "unknown"
    for _ in range(max_polls):
        rows, err = load_studio_rows(notebook_id, profile, auto_refresh_auth=auto_refresh_auth)
        if err:
            time.sleep(poll_seconds)
            continue

        row = find_artifact_row(rows, artifact_id)
        if not row:
            time.sleep(poll_seconds)
            continue

        raw_status = row.get("status")
        if raw_status is None:
            raw_status = row.get("state")
        status = normalize_status(raw_status)
        last_status = status

        if status in SUCCESS_STATES:
            return "completed", None, row
        if status in FAIL_STATES:
            return "failed", None, row

        time.sleep(poll_seconds)

    return "timeout", f"poll_timeout_last={last_status}", None


def preflight(
    notebook_id: str,
    profile: str,
    source_ids: List[str],
    auto_refresh_auth: bool,
) -> Tuple[bool, Dict[str, Any]]:
    report: Dict[str, Any] = {"checked_at": now_iso(), "ok": False}

    v = run(["nlm", "--version"], timeout=30)
    if v.returncode != 0:
        report.update({"reason": "nlm_not_available", "detail": (v.stderr or v.stdout).strip()})
        return False, report

    chk = run_nlm(
        ["nlm", "login", "--check", "--profile", profile],
        profile=profile,
        timeout=90,
        auto_refresh_auth=auto_refresh_auth,
    )
    if chk.returncode != 0:
        report.update({"reason": "auth_required", "detail": (chk.stderr or chk.stdout).strip()})
        return False, report

    ids = source_ids
    if not ids:
        ids, err = load_source_ids(notebook_id, profile, auto_refresh_auth=auto_refresh_auth)
        if err:
            report.update({"reason": "source_list_failed", "detail": err})
            return False, report

    if not ids:
        report.update({"reason": "no_sources", "detail": "Notebook has no sources."})
        return False, report

    report.update({"ok": True, "resolved_source_count": len(ids)})
    return True, report


def main() -> None:
    ap = argparse.ArgumentParser(description="Guarded NotebookLM artifact generator")
    ap.add_argument("--notebook-id", required=True)
    ap.add_argument("--source-ids", default="", help="Comma-separated source IDs. Empty = all sources")
    ap.add_argument("--profile", default="default")
    ap.add_argument("--plan", default=DEFAULT_PLAN, help="Fallback chain, comma-separated")
    ap.add_argument("--max-success", type=int, default=1, help="Stop after N successful artifacts")
    ap.add_argument("--poll-seconds", type=int, default=15)
    ap.add_argument("--max-polls", type=int, default=40)

    ap.add_argument("--daily-budget-total", type=int, default=40)
    ap.add_argument(
        "--daily-budget-per-type",
        default="infographic:10,slides:10,report:12,audio:12",
        help="e.g. 'infographic:8,slides:10,report:12,audio:12'",
    )
    ap.add_argument("--breaker-consecutive-failures", type=int, default=3)
    ap.add_argument("--breaker-open-minutes", type=int, default=90)

    ap.add_argument(
        "--state-file",
        default="~/.openclaw/state/notebooklm-guarded-generator/state.json",
    )
    ap.add_argument(
        "--events-file",
        default="~/.openclaw/state/notebooklm-guarded-generator/events.jsonl",
    )
    ap.add_argument("--dry-run", action="store_true", help="Run preflight only")
    ap.add_argument("--no-auto-refresh-auth", action="store_true")
    args = ap.parse_args()

    notebook_id = args.notebook_id
    source_ids = parse_csv_ids(args.source_ids)
    plan = parse_plan(args.plan)
    auto_refresh_auth = not args.no_auto_refresh_auth

    state_path = Path(args.state_file).expanduser().resolve()
    events_path = Path(args.events_file).expanduser().resolve()

    state = normalize_state(read_json(state_path, default_state()))
    maybe_reset_daily(state)

    # Preflight
    ok, preflight_report = preflight(
        notebook_id=notebook_id,
        profile=args.profile,
        source_ids=source_ids,
        auto_refresh_auth=auto_refresh_auth,
    )
    append_jsonl(events_path, {"ts": now_iso(), "event": "preflight", "report": preflight_report})

    if not ok:
        state["last_run"] = {
            "at": now_iso(),
            "status": "failed_preflight",
            "preflight": preflight_report,
        }
        write_json(state_path, state)
        print(
            json.dumps(
                {
                    "status": "failed_preflight",
                    "preflight": preflight_report,
                    "state_file": str(state_path),
                    "events_file": str(events_path),
                },
                ensure_ascii=False,
            )
        )
        return

    if not source_ids:
        source_ids, _ = load_source_ids(notebook_id, args.profile, auto_refresh_auth=auto_refresh_auth)

    if args.dry_run:
        state["last_run"] = {
            "at": now_iso(),
            "status": "dry_run_ok",
            "preflight": preflight_report,
        }
        write_json(state_path, state)
        print(
            json.dumps(
                {
                    "status": "dry_run_ok",
                    "preflight": preflight_report,
                    "resolved_source_ids": source_ids,
                    "state_file": str(state_path),
                    "events_file": str(events_path),
                },
                ensure_ascii=False,
            )
        )
        return

    per_type_budget = parse_budget_per_type(args.daily_budget_per_type)

    attempts: List[Dict[str, Any]] = []
    successes: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for artifact_type in plan:
        if args.max_success > 0 and len(successes) >= args.max_success:
            break

        allow, reason = budget_allowed(state, artifact_type, args.daily_budget_total, per_type_budget)
        if not allow:
            row = {"artifact_type": artifact_type, "outcome": "skipped", "reason": reason}
            skipped.append(row)
            append_jsonl(events_path, {"ts": now_iso(), "event": "skip", **row})
            continue

        open_now, breaker_reason = breaker_status(state, artifact_type)
        if open_now:
            row = {"artifact_type": artifact_type, "outcome": "skipped", "reason": breaker_reason}
            skipped.append(row)
            append_jsonl(events_path, {"ts": now_iso(), "event": "skip", **row})
            continue

        cmd = build_create_cmd(artifact_type, notebook_id, source_ids, args.profile)
        consume_budget(state, artifact_type)
        created_at = now_iso()
        p = run_nlm(
            cmd,
            profile=args.profile,
            timeout=300,
            auto_refresh_auth=auto_refresh_auth,
        )

        if p.returncode != 0:
            err = (p.stderr or p.stdout).strip()[-800:]
            breaker_record_failure(
                state,
                artifact_type,
                threshold=args.breaker_consecutive_failures,
                open_minutes=args.breaker_open_minutes,
            )
            row = {
                "artifact_type": artifact_type,
                "outcome": "create_failed",
                "reason": err,
                "created_at": created_at,
            }
            attempts.append(row)
            append_jsonl(events_path, {"ts": now_iso(), "event": "create_failed", **row})
            continue

        artifact_id = extract_artifact_id((p.stdout or "") + "\n" + (p.stderr or ""))
        if not artifact_id:
            # strict fail-fast parity with the patched studio create behavior
            msg = f"NotebookLM rejected {artifact_type} creation (no artifact returned)."
            breaker_record_failure(
                state,
                artifact_type,
                threshold=args.breaker_consecutive_failures,
                open_minutes=args.breaker_open_minutes,
            )
            row = {
                "artifact_type": artifact_type,
                "outcome": "create_failed_no_artifact",
                "reason": msg,
                "created_at": created_at,
            }
            attempts.append(row)
            append_jsonl(events_path, {"ts": now_iso(), "event": "create_failed_no_artifact", **row})
            continue

        poll_outcome, poll_reason, poll_row = poll_artifact_status(
            notebook_id=notebook_id,
            artifact_id=artifact_id,
            profile=args.profile,
            poll_seconds=args.poll_seconds,
            max_polls=args.max_polls,
            auto_refresh_auth=auto_refresh_auth,
        )

        if poll_outcome == "completed":
            breaker_record_success(state, artifact_type)
            row = {
                "artifact_type": artifact_type,
                "artifact_id": artifact_id,
                "outcome": "completed",
                "status": normalize_status((poll_row or {}).get("status") or (poll_row or {}).get("state")),
                "created_at": created_at,
            }
            attempts.append(row)
            successes.append(row)
            append_jsonl(events_path, {"ts": now_iso(), "event": "completed", **row})
            continue

        breaker_record_failure(
            state,
            artifact_type,
            threshold=args.breaker_consecutive_failures,
            open_minutes=args.breaker_open_minutes,
        )
        row = {
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "outcome": poll_outcome,
            "reason": poll_reason,
            "created_at": created_at,
        }
        attempts.append(row)
        append_jsonl(events_path, {"ts": now_iso(), "event": poll_outcome, **row})

    final_status = "ok" if len(successes) >= max(1, args.max_success) else ("degraded" if successes else "failed")

    summary = {
        "status": final_status,
        "checked_at": now_iso(),
        "notebook_id": notebook_id,
        "profile": args.profile,
        "plan": plan,
        "max_success": args.max_success,
        "resolved_source_ids": source_ids,
        "preflight": preflight_report,
        "attempts": attempts,
        "successes": successes,
        "skipped": skipped,
        "daily_budget": state.get("daily", {}),
        "breaker": state.get("breaker", {}),
        "state_file": str(state_path),
        "events_file": str(events_path),
    }

    state["last_run"] = {
        "at": now_iso(),
        "status": final_status,
        "notebook_id": notebook_id,
        "plan": plan,
        "success_count": len(successes),
        "attempt_count": len(attempts),
    }
    write_json(state_path, state)

    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
