"""SQLite-backed metadata repository for Bookflow runs."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from ..core.models import ArtifactRecord, now_iso

ENV_BOOKFLOW_DB_PATH = "BOOKFLOW_DB_PATH"
DEFAULT_BOOKFLOW_DB_PATH = "~/.openclaw/state/bookflow/bookflow.db"
DEFAULT_SQLITE_TIMEOUT_SECONDS = 30.0
DEFAULT_BUSY_TIMEOUT_MS = 8_000

_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS assets (
    asset_id TEXT PRIMARY KEY,
    asset_hash TEXT NOT NULL,
    asset_kind TEXT NOT NULL,
    asset_ref TEXT NOT NULL,
    book_title TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS object_notebooks (
    asset_id TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL,
    profile TEXT NOT NULL DEFAULT 'default',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(asset_id) REFERENCES assets(asset_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    asset_id TEXT,
    status TEXT NOT NULL,
    workspace_root TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    book_title TEXT NOT NULL DEFAULT '',
    ranked_json TEXT NOT NULL DEFAULT '',
    notebook_strategy TEXT NOT NULL DEFAULT 'run',
    active_notebook_id TEXT NOT NULL DEFAULT '',
    object_notebook_id TEXT NOT NULL DEFAULT '',
    run_notebook_id TEXT NOT NULL DEFAULT '',
    selected_chapter_ids_json TEXT NOT NULL DEFAULT '[]',
    selected_source_ids_json TEXT NOT NULL DEFAULT '[]',
    errors_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(asset_id) REFERENCES assets(asset_id)
);

CREATE TABLE IF NOT EXISTS run_notebooks (
    run_id TEXT PRIMARY KEY,
    notebook_id TEXT NOT NULL,
    profile TEXT NOT NULL DEFAULT 'default',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS run_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    chapter_id TEXT NOT NULL DEFAULT '',
    source_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    status TEXT NOT NULL,
    artifact_id TEXT NOT NULL DEFAULT '',
    chapter_id TEXT NOT NULL DEFAULT '',
    source_id TEXT NOT NULL DEFAULT '',
    path TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_assets_hash ON assets(asset_hash);
CREATE INDEX IF NOT EXISTS idx_runs_asset_notebook_updated ON runs(asset_id, active_notebook_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_run_sources_run_id ON run_sources(run_id);
CREATE INDEX IF NOT EXISTS idx_run_sources_chapter ON run_sources(chapter_id, source_id);
CREATE INDEX IF NOT EXISTS idx_artifacts_run_id ON artifacts(run_id);
"""


def resolve_db_path(explicit: str = "") -> Path:
    raw = explicit.strip() or os.environ.get(ENV_BOOKFLOW_DB_PATH, "").strip() or DEFAULT_BOOKFLOW_DB_PATH
    return Path(raw).expanduser().resolve()


def _as_json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


class BookflowStore:
    def __init__(self, db_path: str = "") -> None:
        self.db_path = resolve_db_path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=DEFAULT_SQLITE_TIMEOUT_SECONDS)
        self.conn.row_factory = sqlite3.Row
        self._configure_connection()
        self.init_schema()

    def _configure_connection(self) -> None:
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute(f"PRAGMA busy_timeout = {int(DEFAULT_BUSY_TIMEOUT_MS)}")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")

    def close(self) -> None:
        self.conn.close()

    def init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(_SCHEMA_SQL)

    def upsert_asset(
        self,
        *,
        asset_id: str,
        asset_hash: str,
        asset_kind: str,
        asset_ref: str,
        book_title: str,
    ) -> None:
        ts = now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO assets(asset_id, asset_hash, asset_kind, asset_ref, book_title, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                  asset_hash = excluded.asset_hash,
                  asset_kind = excluded.asset_kind,
                  asset_ref = excluded.asset_ref,
                  book_title = excluded.book_title,
                  updated_at = excluded.updated_at
                """,
                (asset_id, asset_hash, asset_kind, asset_ref, book_title, ts, ts),
            )

    def get_object_notebook_id(self, asset_id: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT notebook_id FROM object_notebooks WHERE asset_id = ?",
            (asset_id,),
        ).fetchone()
        if not row:
            return None
        notebook_id = str(row["notebook_id"] or "").strip()
        return notebook_id or None

    def upsert_object_notebook(self, *, asset_id: str, notebook_id: str, profile: str = "default") -> None:
        ts = now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO object_notebooks(asset_id, notebook_id, profile, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                  notebook_id = excluded.notebook_id,
                  profile = excluded.profile,
                  updated_at = excluded.updated_at
                """,
                (asset_id, notebook_id, profile, ts, ts),
            )

    def upsert_run_notebook(self, *, run_id: str, notebook_id: str, profile: str = "default") -> None:
        ts = now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO run_notebooks(run_id, notebook_id, profile, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                  notebook_id = excluded.notebook_id,
                  profile = excluded.profile,
                  updated_at = excluded.updated_at
                """,
                (run_id, notebook_id, profile, ts, ts),
            )

    def upsert_run(
        self,
        *,
        run_id: str,
        status: str,
        workspace_root: str,
        plan: Sequence[str],
        book_title: str,
        ranked_json: str,
        notebook_strategy: str,
        active_notebook_id: str,
        object_notebook_id: str,
        run_notebook_id: str,
        selected_chapter_ids: Sequence[str],
        selected_source_ids: Sequence[str],
        errors: Sequence[str],
        created_at: str,
        updated_at: str,
        asset_id: Optional[str] = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO runs(
                  run_id, asset_id, status, workspace_root, plan_json, book_title, ranked_json,
                  notebook_strategy, active_notebook_id, object_notebook_id, run_notebook_id,
                  selected_chapter_ids_json, selected_source_ids_json, errors_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                  asset_id = excluded.asset_id,
                  status = excluded.status,
                  workspace_root = excluded.workspace_root,
                  plan_json = excluded.plan_json,
                  book_title = excluded.book_title,
                  ranked_json = excluded.ranked_json,
                  notebook_strategy = excluded.notebook_strategy,
                  active_notebook_id = excluded.active_notebook_id,
                  object_notebook_id = excluded.object_notebook_id,
                  run_notebook_id = excluded.run_notebook_id,
                  selected_chapter_ids_json = excluded.selected_chapter_ids_json,
                  selected_source_ids_json = excluded.selected_source_ids_json,
                  errors_json = excluded.errors_json,
                  updated_at = excluded.updated_at
                """,
                (
                    run_id,
                    asset_id,
                    status,
                    workspace_root,
                    _as_json(list(plan)),
                    book_title,
                    ranked_json,
                    notebook_strategy,
                    active_notebook_id,
                    object_notebook_id,
                    run_notebook_id,
                    _as_json(list(selected_chapter_ids)),
                    _as_json(list(selected_source_ids)),
                    _as_json(list(errors)),
                    created_at,
                    updated_at,
                ),
            )

    def get_cached_source_map(
        self,
        *,
        asset_id: str,
        notebook_id: str,
        chapter_ids: Sequence[str],
    ) -> Dict[str, str]:
        normalized = [str(cid).strip() for cid in chapter_ids if str(cid).strip()]
        if not normalized:
            return {}
        if not asset_id.strip() or not notebook_id.strip():
            return {}

        placeholders = ",".join("?" for _ in normalized)
        sql = f"""
            SELECT rs.chapter_id, rs.source_id
            FROM run_sources rs
            JOIN runs r ON r.run_id = rs.run_id
            WHERE r.asset_id = ?
              AND r.active_notebook_id = ?
              AND rs.chapter_id IN ({placeholders})
              AND rs.chapter_id <> ''
              AND rs.source_id <> ''
            ORDER BY r.updated_at DESC, rs.id DESC
        """
        params: List[str] = [asset_id, notebook_id, *normalized]
        rows = self.conn.execute(sql, params).fetchall()

        picked: Dict[str, str] = {}
        for row in rows:
            chapter_id = str(row["chapter_id"] or "").strip()
            source_id = str(row["source_id"] or "").strip()
            if not chapter_id or not source_id:
                continue
            if chapter_id in picked:
                continue
            picked[chapter_id] = source_id

        return {chapter_id: picked[chapter_id] for chapter_id in normalized if chapter_id in picked}

    def replace_run_sources(
        self,
        *,
        run_id: str,
        chapter_ids: Sequence[str],
        source_map: Dict[str, str],
        selected_source_ids: Sequence[str],
    ) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM run_sources WHERE run_id = ?", (run_id,))
            ts = now_iso()

            used: List[str] = []
            for chapter_id in chapter_ids:
                chapter_key = str(chapter_id).strip()
                if not chapter_key:
                    continue
                source_id = str(source_map.get(chapter_key, "")).strip()
                used.append(source_id)
                self.conn.execute(
                    "INSERT INTO run_sources(run_id, chapter_id, source_id, created_at) VALUES (?, ?, ?, ?)",
                    (run_id, chapter_key, source_id, ts),
                )

            for source_id in selected_source_ids:
                token = str(source_id).strip()
                if not token or token in used:
                    continue
                self.conn.execute(
                    "INSERT INTO run_sources(run_id, chapter_id, source_id, created_at) VALUES (?, '', ?, ?)",
                    (run_id, token, ts),
                )

    def replace_artifacts(self, *, run_id: str, artifacts: Sequence[ArtifactRecord]) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM artifacts WHERE run_id = ?", (run_id,))
            ts = now_iso()
            for row in artifacts:
                self.conn.execute(
                    """
                    INSERT INTO artifacts(
                      run_id, artifact_type, status, artifact_id, chapter_id, source_id, path, error, detail_json, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        row.artifact_type,
                        row.status,
                        row.artifact_id,
                        row.chapter_id,
                        row.source_id,
                        row.path,
                        row.error,
                        _as_json(row.detail),
                        ts,
                        ts,
                    ),
                )

    def list_runs(self, *, limit: int = 50, statuses: Optional[Sequence[str]] = None) -> List[Dict[str, object]]:
        limit_value = max(1, int(limit))
        sql = """
            SELECT
              run_id,
              status,
              workspace_root,
              book_title,
              ranked_json,
              notebook_strategy,
              active_notebook_id,
              object_notebook_id,
              run_notebook_id,
              selected_chapter_ids_json,
              selected_source_ids_json,
              errors_json,
              created_at,
              updated_at
            FROM runs
        """
        params: List[object] = []
        if statuses:
            normalized = [str(s).strip() for s in statuses if str(s).strip()]
            if normalized:
                placeholders = ",".join("?" for _ in normalized)
                sql += f" WHERE status IN ({placeholders})"
                params.extend(normalized)

        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit_value)

        rows = self.conn.execute(sql, params).fetchall()
        out: List[Dict[str, object]] = []
        for row in rows:
            def _j(name: str) -> List[str]:
                raw = str(row[name] or "")
                if not raw:
                    return []
                try:
                    val = json.loads(raw)
                    if isinstance(val, list):
                        return [str(x) for x in val]
                except Exception:
                    return []
                return []

            out.append(
                {
                    "run_id": str(row["run_id"] or ""),
                    "status": str(row["status"] or ""),
                    "workspace_root": str(row["workspace_root"] or ""),
                    "book_title": str(row["book_title"] or ""),
                    "ranked_json": str(row["ranked_json"] or ""),
                    "notebook_strategy": str(row["notebook_strategy"] or ""),
                    "active_notebook_id": str(row["active_notebook_id"] or ""),
                    "object_notebook_id": str(row["object_notebook_id"] or ""),
                    "run_notebook_id": str(row["run_notebook_id"] or ""),
                    "selected_chapter_ids": _j("selected_chapter_ids_json"),
                    "selected_source_ids": _j("selected_source_ids_json"),
                    "errors": _j("errors_json"),
                    "created_at": str(row["created_at"] or ""),
                    "updated_at": str(row["updated_at"] or ""),
                }
            )

        return out
