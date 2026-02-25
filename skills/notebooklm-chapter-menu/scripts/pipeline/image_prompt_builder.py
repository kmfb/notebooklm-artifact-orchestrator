#!/usr/bin/env python3
"""
Build image prompts from ranked chapters (image-first pipeline).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def trim_text(s: str, n: int) -> str:
    s = " ".join((s or "").split())
    if len(s) <= n:
        return s
    return s[:n] + "…"


def build_prompts(ch: Dict) -> Dict:
    title = ch.get("title", "")
    preview = trim_text(ch.get("preview", ""), 120)
    chapter_id = ch.get("chapter_id")

    cover_prompt = (
        f"财经深度报道封面图，主题：{title}。"
        f"画面强调新闻现场感与信息密度，写实纪实风，noir情绪，"
        f"冷暖对比光，细节丰富，16:9，high detail，editorial illustration。"
        f"参考要点：{preview}"
    )

    card_prompt = (
        f"信息图卡片风格，主题：{title}。"
        f"一张图表达核心冲突与关键变量，结构清晰，图形化元素克制，"
        f"中文财经媒体视觉风格，16:9，clean layout，high contrast，high detail。"
        f"参考要点：{preview}"
    )

    negative = "lowres, blurry, cartoonish, childish, watermark, text clutter, distorted anatomy"

    return {
        "chapter_id": chapter_id,
        "title": title,
        "score": ch.get("score"),
        "prompts": {
            "cover": cover_prompt,
            "infocard": card_prompt,
            "negative": negative,
        },
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Generate image prompts for selected chapters")
    p.add_argument("--input", required=True, help="chapters_ranked.json")
    p.add_argument("--output", default=None, help="image_prompts.json")
    args = p.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    data = json.loads(input_path.read_text(encoding="utf-8"))

    selected = data.get("selected_chapters", [])
    prompts = [build_prompts(ch) for ch in selected]

    out = {
        "issue_label": data.get("issue_label"),
        "generated_at": now_iso(),
        "source": str(input_path),
        "count": len(prompts),
        "items": prompts,
    }

    output_path = Path(args.output).expanduser().resolve() if args.output else input_path.with_name("image_prompts.json")
    output_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines: List[str] = [
        f"# Image Prompts - {out.get('issue_label')}",
        "",
        f"- generated_at: {out['generated_at']}",
        f"- count: {len(prompts)}",
        "",
    ]

    for i, item in enumerate(prompts, 1):
        md_lines.append(f"## {i}. [{item['chapter_id']}] {item['title']}")
        md_lines.append("")
        md_lines.append("**Cover Prompt**")
        md_lines.append("")
        md_lines.append(item["prompts"]["cover"])
        md_lines.append("")
        md_lines.append("**InfoCard Prompt**")
        md_lines.append("")
        md_lines.append(item["prompts"]["infocard"])
        md_lines.append("")
        md_lines.append(f"Negative: `{item['prompts']['negative']}`")
        md_lines.append("")

    output_md = output_path.with_suffix(".md")
    output_md.write_text("\n".join(md_lines), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "issue_label": out["issue_label"],
        "output": str(output_path),
        "output_md": str(output_md),
        "count": len(prompts),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
