"""First-pass edition quality scoring for EPUB extract candidates."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Mapping, Sequence

_NOISE_PAT = re.compile(r"\b(contents?|copyright|preface|foreword|index|appendix|cover|acknowledg(e)?ments?)\b", re.I)
_SPLIT_PAT = re.compile(r"\s*([>/|]|::|\u203a|\u00bb|\s-\s)\s*")


def _normalize_title(title: str) -> str:
    cleaned = re.sub(r"\s+", " ", (title or "").strip().lower())
    cleaned = re.sub(r"[^\w\s]", "", cleaned)
    return cleaned


def _extract_titles(candidate: Mapping[str, Any]) -> List[str]:
    if isinstance(candidate.get("titles"), list):
        return [str(x) for x in candidate["titles"] if str(x).strip()]

    chapters = candidate.get("chapters")
    titles: List[str] = []
    if isinstance(chapters, list):
        for chapter in chapters:
            if not isinstance(chapter, Mapping):
                continue
            title = str(chapter.get("title") or "").strip()
            if title:
                titles.append(title)
    return titles


def _depth_for_entry(entry: Mapping[str, Any]) -> int:
    for key in ("toc_depth", "depth", "level"):
        val = entry.get(key)
        if isinstance(val, int) and val > 0:
            return val

    for key in ("toc_path", "path"):
        val = entry.get(key)
        if isinstance(val, str) and val.strip():
            parts = [p for p in _SPLIT_PAT.split(val.strip()) if p and p not in {">", "/", "|", "::", "›", "»", " - "}]
            if parts:
                return len(parts)
    return 1


def _toc_depth_proxy(candidate: Mapping[str, Any]) -> float:
    chapters = candidate.get("chapters")
    if not isinstance(chapters, list) or not chapters:
        return 1.0

    depths = [_depth_for_entry(row) for row in chapters if isinstance(row, Mapping)]
    if not depths:
        return 1.0
    return float(sum(depths)) / float(len(depths))


def score_epub_extract(candidate: Mapping[str, Any]) -> Dict[str, float]:
    titles = _extract_titles(candidate)
    if not titles:
        return {
            "overall": 0.0,
            "title_uniqueness_ratio": 0.0,
            "toc_depth_proxy": 0.0,
            "noise_ratio": 1.0,
        }

    normalized = [_normalize_title(t) for t in titles if _normalize_title(t)]
    uniqueness_ratio = (len(set(normalized)) / len(normalized)) if normalized else 0.0

    noise_hits = 0
    for title in titles:
        if _NOISE_PAT.search(title):
            noise_hits += 1
    noise_ratio = noise_hits / len(titles)

    toc_depth = _toc_depth_proxy(candidate)
    depth_norm = min(toc_depth / 4.0, 1.0)
    overall = (0.5 * uniqueness_ratio) + (0.3 * depth_norm) + (0.2 * (1.0 - noise_ratio))

    return {
        "overall": round(overall, 4),
        "title_uniqueness_ratio": round(uniqueness_ratio, 4),
        "toc_depth_proxy": round(toc_depth, 4),
        "noise_ratio": round(noise_ratio, 4),
    }


def compare_epub_extract_candidates(candidates: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for index, candidate in enumerate(candidates):
        score = score_epub_extract(candidate)
        ranked.append(
            {
                "index": index,
                "candidate_id": str(candidate.get("candidate_id") or index),
                "score": score,
                "candidate": dict(candidate),
            }
        )

    ranked.sort(key=lambda row: row["score"]["overall"], reverse=True)
    return ranked
