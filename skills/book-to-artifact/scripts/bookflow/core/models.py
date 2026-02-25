"""Domain models for Bookflow run manifests."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


@dataclass
class ChapterMenuItem:
    chapter_id: str
    title: str = ""
    score: Optional[float] = None
    char_count: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChapterMenuItem":
        chapter_id = str(data.get("chapter_id", "")).strip()
        if not chapter_id:
            raise ValueError("ChapterMenuItem.chapter_id is required")

        score_raw = data.get("score")
        score = float(score_raw) if isinstance(score_raw, (int, float)) else None

        char_count_raw = data.get("char_count")
        char_count = int(char_count_raw) if isinstance(char_count_raw, (int, float)) else None

        return cls(
            chapter_id=chapter_id,
            title=str(data.get("title", "")),
            score=score,
            char_count=char_count,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chapter_id": self.chapter_id,
            "title": self.title,
            "score": self.score,
            "char_count": self.char_count,
        }


@dataclass
class ArtifactRecord:
    artifact_type: str
    status: str
    artifact_id: str = ""
    chapter_id: str = ""
    source_id: str = ""
    path: str = ""
    error: str = ""
    detail: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ArtifactRecord":
        artifact_type = str(data.get("artifact_type") or data.get("type") or "").strip()
        status = str(data.get("status") or data.get("outcome") or "unknown").strip() or "unknown"
        if not artifact_type:
            artifact_type = "unknown"

        return cls(
            artifact_type=artifact_type,
            status=status,
            artifact_id=str(data.get("artifact_id") or data.get("id") or "").strip(),
            chapter_id=str(data.get("chapter_id") or "").strip(),
            source_id=str(data.get("source_id") or "").strip(),
            path=str(data.get("path") or data.get("output_path") or "").strip(),
            error=str(data.get("error") or data.get("reason") or "").strip(),
            detail={k: v for k, v in data.items() if k not in {"artifact_type", "type", "status", "outcome", "artifact_id", "id", "chapter_id", "source_id", "path", "output_path", "error", "reason"}},
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "status": self.status,
            "artifact_id": self.artifact_id,
            "chapter_id": self.chapter_id,
            "source_id": self.source_id,
            "path": self.path,
            "error": self.error,
            "detail": self.detail,
        }


@dataclass
class RunManifest:
    run_id: str
    workspace_root: str
    plan: List[str]
    schema_version: int = 2
    status: str = "started"
    created_at: str = field(default_factory=now_iso)
    updated_at: str = field(default_factory=now_iso)
    book_title: str = ""
    notebook_id: str = ""
    notebook_strategy: str = "run"
    object_notebook_id: str = ""
    run_notebook_id: str = ""
    ranked_json: str = ""
    source_map: Dict[str, str] = field(default_factory=dict)
    selected_chapter_ids: List[str] = field(default_factory=list)
    selected_source_ids: List[str] = field(default_factory=list)
    menu: List[ChapterMenuItem] = field(default_factory=list)
    artifacts: List[ArtifactRecord] = field(default_factory=list)
    stages: Dict[str, Any] = field(default_factory=dict)
    next_action: str = ""
    errors: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunManifest":
        menu_items: List[ChapterMenuItem] = []
        for raw_item in data.get("menu", []) or []:
            if isinstance(raw_item, dict):
                try:
                    menu_items.append(ChapterMenuItem.from_dict(raw_item))
                except ValueError:
                    continue

        artifacts: List[ArtifactRecord] = []
        for raw_record in data.get("artifacts", []) or []:
            if isinstance(raw_record, dict):
                artifacts.append(ArtifactRecord.from_dict(raw_record))

        return cls(
            schema_version=int(data.get("schema_version", 2)),
            run_id=str(data.get("run_id", "")).strip(),
            workspace_root=str(data.get("workspace_root", "")).strip(),
            status=str(data.get("status", "started")).strip() or "started",
            created_at=str(data.get("created_at") or now_iso()),
            updated_at=str(data.get("updated_at") or now_iso()),
            plan=[str(x) for x in (data.get("plan") or [])],
            book_title=str(data.get("book_title") or "").strip(),
            notebook_id=str(data.get("notebook_id") or "").strip(),
            notebook_strategy=str(data.get("notebook_strategy") or "run").strip() or "run",
            object_notebook_id=str(data.get("object_notebook_id") or "").strip(),
            run_notebook_id=str(data.get("run_notebook_id") or "").strip(),
            ranked_json=str(data.get("ranked_json") or "").strip(),
            source_map={str(k): str(v) for k, v in (data.get("source_map") or {}).items()},
            selected_chapter_ids=[str(x) for x in (data.get("selected_chapter_ids") or [])],
            selected_source_ids=[str(x) for x in (data.get("selected_source_ids") or [])],
            menu=menu_items,
            artifacts=artifacts,
            stages=data.get("stages") if isinstance(data.get("stages"), dict) else {},
            next_action=str(data.get("next_action") or ""),
            errors=[str(x) for x in (data.get("errors") or [])],
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "workspace_root": self.workspace_root,
            "book_title": self.book_title,
            "notebook_id": self.notebook_id,
            "notebook_strategy": self.notebook_strategy,
            "object_notebook_id": self.object_notebook_id,
            "run_notebook_id": self.run_notebook_id,
            "plan": self.plan,
            "ranked_json": self.ranked_json,
            "selected_chapter_ids": self.selected_chapter_ids,
            "selected_source_ids": self.selected_source_ids,
            "source_map": self.source_map,
            "menu": [item.to_dict() for item in self.menu],
            "artifacts": [record.to_dict() for record in self.artifacts],
            "stages": self.stages,
            "next_action": self.next_action,
            "errors": self.errors,
        }

    def touch(self) -> None:
        self.updated_at = now_iso()
