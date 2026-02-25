#!/usr/bin/env python3
"""
Extract chapter-like content from an EPUB into structured JSON/Markdown.

Supports both:
- Structured magazine layout: <n>/OEBPS/chapter1.xhtml
- Generic EPUBs via OPF spine order (fallback to html/xhtml scan)
"""

from __future__ import annotations

import argparse
import json
import re
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import List

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None


CHAPTER_RE = re.compile(r"^(\d+)/OEBPS/chapter\d+\.xhtml$", re.IGNORECASE)
HTML_RE = re.compile(r"\.(xhtml|html|htm)$", re.IGNORECASE)


@dataclass
class Chapter:
    chapter_id: str
    order: int
    source_path: str
    title: str
    char_count: int
    text: str
    preview: str


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def html_to_text(html: str) -> str:
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n")
    else:
        text = re.sub(r"<[^>]+>", " ", html)

    text = text.replace("\u00a0", " ")
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def extract_title(html: str, fallback: str = "") -> str:
    m = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    if m:
        title = re.sub(r"\s+", " ", m.group(1)).strip()
        if title:
            return title
    return fallback


def _extract_heading_candidates(html: str) -> List[str]:
    titles: List[str] = []

    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        for tag in ("h1", "h2", "h3"):
            for node in soup.find_all(tag):
                text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()
                if text:
                    titles.append(text)
    else:
        for tag in ("h1", "h2", "h3"):
            pattern = re.compile(rf"<{tag}[^>]*>(.*?)</{tag}>", re.IGNORECASE | re.DOTALL)
            for match in pattern.finditer(html):
                text = re.sub(r"<[^>]+>", " ", match.group(1))
                text = re.sub(r"\s+", " ", text).strip()
                if text:
                    titles.append(text)

    return titles


def _pick_title_from_text(text: str, html_title: str, fallback: str, heading_titles: List[str]) -> str:
    lines = [x.strip() for x in text.split("\n") if x.strip()]
    if not lines:
        return html_title or fallback

    first = lines[0]
    second = lines[1] if len(lines) > 1 else ""

    generic = {html_title.strip(), fallback.strip(), PurePosixPath(fallback).stem.strip()}
    generic = {g for g in generic if g}

    for candidate in heading_titles:
        candidate = candidate.strip()
        if candidate and candidate not in generic:
            return candidate[:80]

    # Common conversion issue: every HTML <title> is the same book title.
    # In that case, the first body line repeats it and the second line is the chapter heading.
    if first in generic and second:
        return second[:80]

    # Prefer explicit chapter-like heading when present.
    chapter_pat = re.compile(r"^(前言|后记|序言|导言|附录|第[一二三四五六七八九十百零〇\d]+[章节回卷部].{0,50})$")
    for line in lines[:6]:
        if chapter_pat.match(line):
            return line[:80]

    return first[:80]


def clean_text(raw: str) -> str:
    """
    Keep paragraph/heading structure for downstream LLM retrieval.
    Do NOT flatten all whitespace to a single line.
    """
    lines = [x.replace("\u00a0", " ").strip() for x in raw.split("\n")]

    cleaned: List[str] = []
    for line in lines:
        if not line:
            cleaned.append("")
            continue

        # known boilerplate / prompt noise
        if "请务必在总结开头增加这段话" in line:
            continue
        if "不代表财新观点和立场" in line and "本文由第三方AI" in line:
            continue

        # normalize inner spaces but keep line boundaries
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            cleaned.append(line)

    # collapse excessive blank lines, keep paragraph separation
    compact: List[str] = []
    blank_run = 0
    for line in cleaned:
        if not line:
            blank_run += 1
            if blank_run <= 1:
                compact.append("")
            continue
        blank_run = 0
        compact.append(line)

    text = "\n".join(compact)

    # remove known one-line boilerplate if present
    text = re.sub(
        r"本文由第三方AI基于财新文章\[[^\]]+\]\([^\)]+\)提炼总结而成，可能与原文真实意图存在偏差。",
        "",
        text,
    )

    # de-duplicate accidentally repeated long segments
    text = re.sub(r"(.{20,80}?)\1+", r"\1", text)
    return text.strip()


def detect_issue_label(epub_path: Path) -> str:
    m = re.search(r"第(\d+)期(20\d{2})", epub_path.name)
    if m:
        issue, year = m.group(1), m.group(2)
        return f"{year}-{issue}"
    return epub_path.stem


def natural_key(s: str):
    parts = re.split(r"(\d+)", s.lower())
    return [int(p) if p.isdigit() else p for p in parts]


def _resolve_posix(base: str, href: str) -> str:
    p = (PurePosixPath(base).parent / href).as_posix()
    return PurePosixPath(p).as_posix()


def list_content_docs(zf: zipfile.ZipFile) -> List[str]:
    names = [n for n in zf.namelist() if not n.endswith("/")]
    name_set = set(names)

    # 1) Keep structured chapter layout behavior first.
    structured_chapters = [n for n in names if CHAPTER_RE.match(n)]
    if structured_chapters:
        return sorted(structured_chapters, key=lambda x: int(CHAPTER_RE.match(x).group(1)))

    # 2) Generic EPUB: use OPF spine order.
    try:
        container_xml = zf.read("META-INF/container.xml").decode("utf-8", "ignore")
        root = ET.fromstring(container_xml)
        rootfile = None
        for rf in root.findall(".//{*}rootfile"):
            fp = rf.attrib.get("full-path")
            if fp:
                rootfile = fp
                break

        if rootfile and rootfile in name_set:
            opf_xml = zf.read(rootfile).decode("utf-8", "ignore")
            opf_root = ET.fromstring(opf_xml)

            manifest = {}
            for item in opf_root.findall(".//{*}manifest/{*}item"):
                iid = item.attrib.get("id")
                href = item.attrib.get("href", "")
                media = item.attrib.get("media-type", "")
                if not iid or not href:
                    continue
                if media in ("application/xhtml+xml", "text/html") or HTML_RE.search(href):
                    manifest[iid] = href

            docs: List[str] = []
            for itemref in opf_root.findall(".//{*}spine/{*}itemref"):
                iid = itemref.attrib.get("idref")
                href = manifest.get(iid or "")
                if not href:
                    continue
                path = _resolve_posix(rootfile, href)
                if path in name_set:
                    docs.append(path)

            if docs:
                # de-dup keep order
                seen = set()
                out = []
                for d in docs:
                    if d in seen:
                        continue
                    seen.add(d)
                    out.append(d)
                return out

            # No spine sequence -> fallback to manifest order
            for href in manifest.values():
                path = _resolve_posix(rootfile, href)
                if path in name_set:
                    docs.append(path)
            if docs:
                seen = set()
                out = []
                for d in docs:
                    if d in seen:
                        continue
                    seen.add(d)
                    out.append(d)
                return out
    except Exception:
        pass

    # 3) Last fallback: scan all html/xhtml files.
    fallback = [n for n in names if HTML_RE.search(n)]
    fallback = [n for n in fallback if not re.search(r"/(toc|nav)\.(xhtml|html|htm)$", n, re.I)]
    return sorted(fallback, key=natural_key)


def extract(epub_path: Path, min_chars: int) -> List[Chapter]:
    chapters: List[Chapter] = []

    with zipfile.ZipFile(epub_path) as zf:
        docs = list_content_docs(zf)

        for idx, name in enumerate(docs, start=1):
            try:
                html = zf.read(name).decode("utf-8", "ignore")
            except KeyError:
                continue

            m = CHAPTER_RE.match(name)
            if m:
                order = int(m.group(1))
                chapter_id = m.group(1)
            else:
                order = idx
                stem = PurePosixPath(name).stem
                n = re.search(r"(\d+)$", stem)
                chapter_id = str(int(n.group(1))) if n else str(idx)

            fallback_title = PurePosixPath(name).stem
            html_title = extract_title(html, fallback=fallback_title)
            heading_titles = _extract_heading_candidates(html)
            raw = html_to_text(html)
            text = clean_text(raw)
            if len(text) < min_chars:
                continue
            title = _pick_title_from_text(
                text=text,
                html_title=html_title,
                fallback=fallback_title,
                heading_titles=heading_titles,
            )

            preview = text[:180] + ("…" if len(text) > 180 else "")
            chapters.append(
                Chapter(
                    chapter_id=str(chapter_id),
                    order=order,
                    source_path=name,
                    title=title,
                    char_count=len(text),
                    text=text,
                    preview=preview,
                )
            )

    chapters.sort(key=lambda c: c.order)
    return chapters


def write_outputs(out_dir: Path, epub_path: Path, issue_label: str, chapters: List[Chapter]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "issue_label": issue_label,
        "epub_path": str(epub_path),
        "extracted_at": now_iso(),
        "chapter_count": len(chapters),
        "chapters": [asdict(c) for c in chapters],
    }

    json_path = out_dir / "chapters.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        f"# EPUB Extract - {issue_label}",
        "",
        f"- Source: `{epub_path}`",
        f"- Extracted at: {payload['extracted_at']}",
        f"- Chapters kept: {len(chapters)}",
        "",
        "## Chapters",
        "",
    ]

    for c in chapters:
        md_lines.append(f"### {c.order}. {c.title}")
        md_lines.append(f"- chars: {c.char_count}")
        md_lines.append(f"- source: `{c.source_path}`")
        md_lines.append(f"- preview: {c.preview}")
        md_lines.append("")

    (out_dir / "chapters.md").write_text("\n".join(md_lines), encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser(description="Extract chapter text from EPUB")
    p.add_argument("--epub", required=True, help="Path to EPUB file")
    p.add_argument("--out-dir", default=None, help="Output directory")
    p.add_argument("--issue-label", default=None, help="Issue label override")
    p.add_argument("--min-chars", type=int, default=500, help="Drop chapters shorter than this")
    args = p.parse_args()

    epub_path = Path(args.epub).expanduser().resolve()
    if not epub_path.exists():
        raise SystemExit(f"EPUB not found: {epub_path}")

    issue_label = args.issue_label or detect_issue_label(epub_path)
    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else Path(__file__).resolve().parents[2] / "data" / "notebooklm_pipeline" / issue_label
    )

    chapters = extract(epub_path=epub_path, min_chars=args.min_chars)
    write_outputs(out_dir=out_dir, epub_path=epub_path, issue_label=issue_label, chapters=chapters)

    print(json.dumps({
        "status": "ok",
        "issue_label": issue_label,
        "out_dir": str(out_dir),
        "chapter_count": len(chapters),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
