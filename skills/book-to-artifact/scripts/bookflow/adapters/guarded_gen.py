"""Adapter for notebooklm-guarded-generator skill."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .runner import AdapterError, run_json_dict
from ..core.config import SKILL_GUARDED_GENERATOR


class GuardedGeneratorAdapter:
    def __init__(self, workspace_root: Path) -> None:
        self.workspace_root = workspace_root
        self.script_path = (workspace_root / SKILL_GUARDED_GENERATOR).resolve()
        if not self.script_path.exists():
            raise AdapterError(f"guarded generator script missing: {self.script_path}")

    def generate(
        self,
        *,
        notebook_id: str,
        source_ids: List[str],
        profile: str,
        artifact_plan: List[str],
        max_success: int,
        poll_seconds: int,
        max_polls: int,
        state_file: Path,
        events_file: Path,
        dry_run: bool = False,
        timeout: int = 7200,
    ) -> Dict[str, Any]:
        if not notebook_id.strip():
            raise AdapterError("notebook_id is required for guarded generation")
        if not source_ids:
            raise AdapterError("source_ids is required for guarded generation")
        if not artifact_plan:
            raise AdapterError("artifact_plan is required for guarded generation")

        cmd = [
            "python3",
            str(self.script_path),
            "--notebook-id",
            notebook_id,
            "--source-ids",
            ",".join(source_ids),
            "--profile",
            profile,
            "--plan",
            ",".join(artifact_plan),
            "--max-success",
            str(max_success),
            "--poll-seconds",
            str(poll_seconds),
            "--max-polls",
            str(max_polls),
            "--state-file",
            str(state_file),
            "--events-file",
            str(events_file),
        ]
        if dry_run:
            cmd.append("--dry-run")

        payload = run_json_dict(cmd, timeout=timeout)
        if not isinstance(payload.get("status"), str):
            raise AdapterError("guarded-generator payload missing status")
        return payload
