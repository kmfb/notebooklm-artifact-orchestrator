#!/usr/bin/env python3
"""
Rank/select extracted chapters for image-first production.
Supports weighted score mode and random selection mode.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List


TOPIC_KEYWORDS = [
    "周刊", "封面", "调查", "深度", "金融", "经济", "监管", "市场", "科技", "医药", "地产", "国际", "政策", "宏观", "产业", "企业", "人物", "访谈", "专题", "债",
]

VISUAL_KEYWORDS = [
    "天眼", "国风", "现场", "图", "人物", "城市", "工厂", "实验室", "展会", "地图", "影像", "案例", "冲突", "事故",
]

META_TITLE_PATTERNS = [
    r"目录", r"目次", r"contents", r"table\s+of\s+contents",
    r"出版说明", r"版权", r"扉页", r"封面",
    r"自\s*序", r"前言", r"后记", r"跋", r"附录", r"译后记",
    r"参考书目", r"名家推荐", r"纪事", r"读法", r"导读", r"始末", r"大历史观", r"神宗实录", r"欢呼", r"倒彩",
]

MAGAZINE_HINT_PATTERNS = [
    r"周刊", r"月刊", r"季刊", r"日报", r"晚报", r"特刊", r"增刊", r"vol\.", r"no\.", r"issue", r"第\d+期", r"magazine",
]

BOOK_HINT_PATTERNS = [
    r"^\s*第\s*[0-9一二三四五六七八九十百千]+\s*[章节回部篇]",
    r"^\s*chapter\s*\d+",
    r"^\s*\d+\s+",
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def keyword_hits(text: str, keywords: List[str]) -> int:
    return sum(1 for k in keywords if k in text)


def title_bucket(title: str) -> str:
    if not title:
        return "其他"
    return re.split(r"[｜|]", title, maxsplit=1)[0].strip() or "其他"


def is_noise_chapter(ch: Dict) -> bool:
    title = (ch.get("title") or "").strip().lower()
    text = (ch.get("text") or "")[:400].lower()

    noise_exact = {
        "contents", "table of contents", "目录", "目次", "编者的话", "卷首语", "前言", "后记", "附录",
        "本书中的物理学单位一览表",
    }
    if title in noise_exact:
        return True
    if "table of contents" in text or "目录" in text[:80]:
        return True
    return False


def meta_title_penalty(title: str, chars: int) -> float:
    t = (title or "").strip().lower()
    if not t:
        return 0.0

    hit = any(re.search(pat, t, re.I) for pat in META_TITLE_PATTERNS)
    if not hit:
        return 0.0

    # meta chapters are less reading-friendly for first-pass selection
    if chars <= 2000:
        return 8.0
    if chars <= 8000:
        return 5.0
    return 3.0


def body_length_bonus(chars: int) -> float:
    # encourage normal body chapters over tiny front/back matter
    if 6000 <= chars <= 40000:
        return 2.0
    if 3000 <= chars < 6000:
        return 0.8
    return 0.0


def score_chapter(ch: Dict, w_len: float, w_topic: float, w_visual: float) -> Dict:
    title = ch.get("title", "")
    text = ch.get("text", "")
    chars = int(ch.get("char_count", 0))

    len_score = min(45.0, math.log(max(chars, 1), 1.35))
    topic_score = min(30.0, keyword_hits(title + " " + text[:1200], TOPIC_KEYWORDS) * 4.0)
    visual_score = min(25.0, keyword_hits(title + " " + text[:800], VISUAL_KEYWORDS) * 5.0)

    base_score = w_len * len_score + w_topic * topic_score + w_visual * visual_score
    penalty = meta_title_penalty(title, chars)
    bonus = body_length_bonus(chars)
    weighted_score = round(base_score - penalty + bonus, 2)

    return {
        **ch,
        "score": weighted_score,
        "score_breakdown": {
            "len_score": round(len_score, 2),
            "topic_score": round(topic_score, 2),
            "visual_score": round(visual_score, 2),
            "meta_penalty": round(penalty, 2),
            "body_length_bonus": round(bonus, 2),
            "weights": {
                "w_len": w_len,
                "w_topic": w_topic,
                "w_visual": w_visual,
            },
        },
        "bucket": title_bucket(title),
    }


def select_score(chapters: List[Dict], top_n: int, max_per_bucket: int) -> List[Dict]:
    if max_per_bucket <= 0:
        return chapters[:top_n]

    selected: List[Dict] = []
    bucket_count: Dict[str, int] = {}

    for ch in chapters:
        b = ch.get("bucket") or "其他"
        if bucket_count.get(b, 0) >= max_per_bucket:
            continue
        selected.append(ch)
        bucket_count[b] = bucket_count.get(b, 0) + 1
        if len(selected) >= top_n:
            break

    return selected


def select_random(chapters: List[Dict], top_n: int, max_per_bucket: int, seed: int | None) -> List[Dict]:
    rng = random.Random(seed)
    pool = chapters[:]
    rng.shuffle(pool)

    if max_per_bucket <= 0:
        return pool[:top_n]

    selected: List[Dict] = []
    bucket_count: Dict[str, int] = {}
    for ch in pool:
        b = ch.get("bucket") or "其他"
        if bucket_count.get(b, 0) >= max_per_bucket:
            continue
        selected.append(ch)
        bucket_count[b] = bucket_count.get(b, 0) + 1
        if len(selected) >= top_n:
            break
    return selected


def build_batches(chapters: List[Dict], batch_size: int) -> List[Dict]:
    batches: List[Dict] = []
    for i in range(0, len(chapters), batch_size):
        part = chapters[i:i + batch_size]
        batches.append({
            "batch_id": f"img-batch-{i // batch_size + 1}",
            "chapter_ids": [x.get("chapter_id") for x in part],
            "titles": [x.get("title") for x in part],
            "count": len(part),
        })
    return batches


def is_magazine_like(issue_label: str, chapters: List[Dict]) -> bool:
    titles = [str(ch.get("title") or "") for ch in chapters[:30]]
    text = (issue_label or "") + "\n" + "\n".join(titles)
    low = text.lower()

    # Strong explicit magazine signal wins.
    if any(re.search(pat, low, re.I) for pat in MAGAZINE_HINT_PATTERNS):
        return True

    # Guard: if chapter titles look book-like, never force magazine mode.
    if titles:
        book_hits = 0
        for t in titles:
            if any(re.search(pat, t, re.I) for pat in BOOK_HINT_PATTERNS):
                book_hits += 1
        if book_hits / max(1, len(titles)) >= 0.35:
            return False

    # Conservative fallback for unknown docs: many tiny sections may indicate periodicals.
    char_counts = [int(ch.get("char_count", 0) or 0) for ch in chapters]
    if len(char_counts) >= 40:
        small = sum(1 for c in char_counts if c <= 4200)
        if small / max(1, len(char_counts)) >= 0.78:
            return True
    return False


def _reading_order_key(ch: Dict) -> tuple:
    order = ch.get("order")
    if isinstance(order, int):
        return (0, order)

    cid = str(ch.get("chapter_id") or "").strip()
    if cid.isdigit():
        return (1, int(cid))
    return (2, cid)


def main() -> None:
    p = argparse.ArgumentParser(description="Rank/select chapters for image-first workflow")
    p.add_argument("--input", required=True, help="chapters.json")
    p.add_argument("--top-n", type=int, default=8)
    p.add_argument("--batch-size", type=int, default=3)
    p.add_argument("--output", default=None, help="ranked.json output path")

    # score config
    p.add_argument("--w-len", type=float, default=1.0, help="Weight for length score")
    p.add_argument("--w-topic", type=float, default=1.0, help="Weight for topic score")
    p.add_argument("--w-visual", type=float, default=1.0, help="Weight for visual score")

    # selection config
    p.add_argument("--select-mode", choices=["score", "random"], default="score", help="Selection mode")
    p.add_argument("--max-per-bucket", type=int, default=0, help="Diversity cap per title bucket; 0 means no cap")
    p.add_argument("--random-pool-size", type=int, default=0, help="For random mode: sample from top-K scored pool; 0 means full pool")
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducible random selection")
    args = p.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    data = json.loads(input_path.read_text(encoding="utf-8"))
    chapters = data.get("chapters", [])
    chapters = [ch for ch in chapters if not is_noise_chapter(ch)]

    ranked = [score_chapter(ch, args.w_len, args.w_topic, args.w_visual) for ch in chapters]
    ranked.sort(key=lambda x: (x["score"], x.get("char_count", 0)), reverse=True)

    top_n = max(1, args.top_n)
    max_per_bucket = max(0, args.max_per_bucket)

    if args.select_mode == "random":
        if args.random_pool_size > 0:
            pool = ranked[: min(args.random_pool_size, len(ranked))]
        else:
            pool = ranked
        selected = select_random(pool, top_n=top_n, max_per_bucket=max_per_bucket, seed=args.seed)
    else:
        selected = select_score(ranked, top_n=top_n, max_per_bucket=max_per_bucket)

    magazine_like = is_magazine_like(str(data.get("issue_label") or ""), chapters)
    if not magazine_like:
        # Books default to in-book reading order. Mixed order is reserved for magazines.
        selected = sorted(selected, key=_reading_order_key)

    selection_strategy = args.select_mode if magazine_like else "sequential"

    batches = build_batches(selected, max(1, args.batch_size))

    out = {
        "issue_label": data.get("issue_label"),
        "generated_at": now_iso(),
        "source": str(input_path),
        "top_n": len(selected),
        "batch_size": args.batch_size,
        "rank_config": {
            "w_len": args.w_len,
            "w_topic": args.w_topic,
            "w_visual": args.w_visual,
            "select_mode": args.select_mode,
            "max_per_bucket": args.max_per_bucket,
            "random_pool_size": args.random_pool_size,
            "seed": args.seed,
            "magazine_like": magazine_like,
            "selection_strategy": selection_strategy,
            "selected_order": "score_or_random" if magazine_like else "reading_order",
        },
        "ranked_chapters": ranked,
        "selected_chapters": selected,
        "image_batches": batches,
    }

    output_path = Path(args.output).expanduser().resolve() if args.output else input_path.with_name("chapters_ranked.json")
    output_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({
        "status": "ok",
        "issue_label": out["issue_label"],
        "output": str(output_path),
        "selected": len(selected),
        "batches": len(batches),
        "rank_config": out["rank_config"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
