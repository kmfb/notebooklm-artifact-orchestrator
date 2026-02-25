#!/usr/bin/env python3
"""
Create batch execution manifest for NotebookLM workflow.
Image-first, audio-second.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def make_image_tasks(image_batches: List[Dict]) -> List[Dict]:
    tasks: List[Dict] = []
    for b in image_batches:
        tasks.append({
            "task_id": b["batch_id"],
            "phase": "image",
            "mode": "batch_chapters",
            "chapter_ids": b["chapter_ids"],
            "titles": b["titles"],
            "instruction": "为每章产出 1-2 张图像素材（封面图+信息卡），先保证封面图可用。",
        })
    return tasks


def make_audio_tasks(selected_chapters: List[Dict]) -> List[Dict]:
    tasks: List[Dict] = [
        {
            "task_id": "audio-overview-1",
            "phase": "audio",
            "mode": "issue_overview",
            "instruction": "生成整刊总览音频脚本（15-25分钟），强调本期主线与章节导览。",
        }
    ]
    for ch in selected_chapters:
        tasks.append({
            "task_id": f"audio-ch-{ch.get('chapter_id')}",
            "phase": "audio",
            "mode": "chapter_audio",
            "chapter_id": ch.get("chapter_id"),
            "title": ch.get("title"),
            "instruction": "生成章节短音频脚本（3-8分钟），突出关键事实、争议点与行动建议。",
        })
    return tasks


def main() -> None:
    p = argparse.ArgumentParser(description="Build NotebookLM batch manifest (image-first)")
    p.add_argument("--input", required=True, help="chapters_ranked.json")
    p.add_argument("--output", default=None, help="notebooklm_batches.json")
    args = p.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    data = json.loads(input_path.read_text(encoding="utf-8"))

    image_batches = data.get("image_batches", [])
    selected = data.get("selected_chapters", [])

    out = {
        "issue_label": data.get("issue_label"),
        "generated_at": now_iso(),
        "strategy": "image_first_then_audio",
        "source": str(input_path),
        "phases": [
            {
                "phase": "image",
                "description": "先产章节图像素材",
                "tasks": make_image_tasks(image_batches),
            },
            {
                "phase": "audio",
                "description": "后产整刊与章节音频",
                "tasks": make_audio_tasks(selected),
            },
        ],
    }

    output_path = Path(args.output).expanduser().resolve() if args.output else input_path.with_name("notebooklm_batches.json")
    output_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "issue_label": out["issue_label"],
        "output": str(output_path),
        "image_tasks": len(out["phases"][0]["tasks"]),
        "audio_tasks": len(out["phases"][1]["tasks"]),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
