"""Run state machine for book-to-artifact orchestration."""

from __future__ import annotations

from typing import Dict, Set

from .models import RunManifest

STATE_STARTED = "started"
STATE_FETCHED = "fetched"
STATE_PREPARED = "prepared"
STATE_AWAITING_CHAPTER_SELECTION = "awaiting_chapter_selection"
STATE_GENERATING = "generating"
STATE_COMPLETED = "completed"
STATE_PARTIAL = "partial"
STATE_FAILED = "failed"

TERMINAL_STATES = {STATE_COMPLETED, STATE_PARTIAL, STATE_FAILED, STATE_AWAITING_CHAPTER_SELECTION}

_ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    STATE_STARTED: {STATE_FETCHED, STATE_PREPARED, STATE_FAILED},
    STATE_FETCHED: {STATE_PREPARED, STATE_FAILED},
    STATE_PREPARED: {STATE_AWAITING_CHAPTER_SELECTION, STATE_GENERATING, STATE_COMPLETED, STATE_FAILED},
    STATE_AWAITING_CHAPTER_SELECTION: {STATE_GENERATING, STATE_FAILED},
    STATE_GENERATING: {STATE_COMPLETED, STATE_PARTIAL, STATE_FAILED},
    STATE_PARTIAL: {STATE_GENERATING, STATE_COMPLETED, STATE_FAILED},
    STATE_COMPLETED: set(),
    STATE_FAILED: set(),
}


def can_transition(current: str, target: str) -> bool:
    return target in _ALLOWED_TRANSITIONS.get(current, set())


def transition(manifest: RunManifest, target: str) -> None:
    current = manifest.status
    if current == target:
        manifest.touch()
        return
    if not can_transition(current, target):
        raise ValueError(f"Illegal state transition: {current} -> {target}")
    manifest.status = target
    manifest.touch()
