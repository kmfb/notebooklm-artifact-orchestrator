"""Adapter for notebooklm-chapter-menu skill."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .runner import AdapterError, run_json_dict
from ..core.config import SKILL_CHAPTER_MENU
from ..core.models import ChapterMenuItem


class ChapterMenuAdapter:
    def __init__(self, workspace_root: Path) -> None:
        self.workspace_root = workspace_root
        self.script_path = (workspace_root / SKILL_CHAPTER_MENU).resolve()
        if not self.script_path.exists():
            raise AdapterError(f"chapter-menu script missing: {self.script_path}")

    def prepare(
        self,
        *,
        epub: str,
        ranked_json: str,
        top_n: int,
        batch_size: int,
        select_mode: str,
        allow_random: bool,
        seed: int,
        max_per_bucket: int,
        random_pool_size: int,
        w_len: float,
        w_topic: float,
        w_visual: float,
        issue_label: str,
        out_dir: str,
        timeout: int = 2400,
    ) -> Dict[str, Any]:
        cmd = [
            "python3",
            str(self.script_path),
            "--workspace-root",
            str(self.workspace_root),
            "--top-n",
            str(top_n),
            "--batch-size",
            str(batch_size),
            "--select-mode",
            select_mode,
            "--seed",
            str(seed),
            "--max-per-bucket",
            str(max_per_bucket),
            "--random-pool-size",
            str(random_pool_size),
            "--w-len",
            str(w_len),
            "--w-topic",
            str(w_topic),
            "--w-visual",
            str(w_visual),
        ]
        if select_mode == "random" and allow_random:
            cmd.append("--allow-random")

        if ranked_json:
            cmd += ["--ranked-json", ranked_json]
        elif epub:
            cmd += ["--epub", epub]
        else:
            raise AdapterError("Either epub or ranked_json is required")

        if issue_label:
            cmd += ["--issue-label", issue_label]
        if out_dir:
            cmd += ["--out-dir", out_dir]

        payload = run_json_dict(cmd, timeout=timeout)
        self._validate_prepare_payload(payload)
        return payload

    def generate_infographics(
        self,
        *,
        ranked_json: str,
        notebook_id: str,
        chapter_ids: List[str],
        profile: str,
        poll_seconds: int,
        max_polls: int,
        chars_per_chapter: int,
        max_chapters: int,
        infographic_out_dir: str,
        publish_after_generate: bool,
        obsidian_vault_path: str,
        notion_data_source_id: str,
        gdrive_enabled: bool,
        gdrive_folder_id: str,
        timeout: int = 7200,
    ) -> Dict[str, Any]:
        if not notebook_id.strip():
            raise AdapterError("notebook_id is required for infographic generation")
        if not chapter_ids:
            raise AdapterError("chapter_ids is required for infographic generation")

        cmd = [
            "python3",
            str(self.script_path),
            "--workspace-root",
            str(self.workspace_root),
            "--ranked-json",
            ranked_json,
            "--notebook-id",
            notebook_id,
            "--chapter-ids",
            ",".join(chapter_ids),
            "--profile",
            profile,
            "--poll-seconds",
            str(poll_seconds),
            "--max-polls",
            str(max_polls),
            "--chars-per-chapter",
            str(chars_per_chapter),
            "--max-chapters",
            str(max_chapters),
        ]

        if infographic_out_dir:
            cmd += ["--infographic-out-dir", infographic_out_dir]

        if publish_after_generate:
            cmd.append("--publish-after-generate")
            if obsidian_vault_path:
                cmd += ["--obsidian-vault-path", obsidian_vault_path]
            if notion_data_source_id:
                cmd += ["--notion-data-source-id", notion_data_source_id]

            if gdrive_enabled:
                cmd.append("--gdrive-enabled")
            else:
                cmd.append("--no-gdrive")
            if gdrive_folder_id:
                cmd += ["--gdrive-folder-id", gdrive_folder_id]

        payload = run_json_dict(cmd, timeout=timeout)
        return payload

    @staticmethod
    def parse_menu(menu_rows: Any) -> List[ChapterMenuItem]:
        if not isinstance(menu_rows, list):
            return []

        menu: List[ChapterMenuItem] = []
        for row in menu_rows:
            if not isinstance(row, dict):
                continue
            try:
                menu.append(ChapterMenuItem.from_dict(row))
            except ValueError:
                continue
        return menu

    @staticmethod
    def _validate_prepare_payload(payload: Dict[str, Any]) -> None:
        if not isinstance(payload.get("status"), str):
            raise AdapterError("chapter-menu payload missing status")

        ranked_json = payload.get("ranked_json")
        if not isinstance(ranked_json, str) or not ranked_json.strip():
            raise AdapterError("chapter-menu payload missing ranked_json")

        menu_rows = payload.get("menu")
        if menu_rows is not None and not isinstance(menu_rows, list):
            raise AdapterError("chapter-menu payload.menu must be a list")
