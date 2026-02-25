#!/usr/bin/env python3
"""Audit, clean, and package notebooklm-artifact-orchestrator skills."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

TARGET_SKILLS = [
    "book-to-artifact",
    "telegram-book-fetch",
    "notebooklm-chapter-menu",
    "notebooklm-guarded-generator",
]

BANNED_PATTERNS = [
    re.compile(r"/Users/[A-Za-z0-9._-]+"),
    re.compile(r"fd15bf2a-2011-4d72-a8ed-566743b41adc"),
    re.compile(r"0AMb0gmswimNYUk9PVA"),
    re.compile(r"825784726437756948"),
    re.compile(r"18683613290"),
]

TEXT_EXTS = {".md", ".py", ".json", ".txt", ".sh", ".yaml", ".yml"}


def _read_text_safe(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _iter_files(root: Path) -> List[Path]:
    files: List[Path] = []
    for path in root.rglob("*"):
        if path.is_file():
            files.append(path)
    return files


def _scan_skill(skill_dir: Path) -> Dict[str, Any]:
    findings: List[Dict[str, Any]] = []
    pycache_paths: List[str] = []
    pyc_files: List[str] = []

    for path in skill_dir.rglob("*"):
        rel = str(path.relative_to(skill_dir))
        if path.is_dir() and path.name == "__pycache__":
            pycache_paths.append(rel)
            continue
        if not path.is_file():
            continue

        if path.suffix == ".pyc":
            pyc_files.append(rel)

        if path.suffix.lower() not in TEXT_EXTS:
            continue

        text = _read_text_safe(path)
        if not text:
            continue
        for pattern in BANNED_PATTERNS:
            for match in pattern.finditer(text):
                findings.append(
                    {
                        "file": rel,
                        "pattern": pattern.pattern,
                        "match": match.group(0),
                    }
                )

    return {
        "findings": findings,
        "pycache_dirs": pycache_paths,
        "pyc_files": pyc_files,
    }


def _clean_source_caches(skill_dir: Path) -> Dict[str, int]:
    removed_dirs = 0
    removed_files = 0

    for path in sorted(skill_dir.rglob("__pycache__")):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            removed_dirs += 1

    for path in sorted(skill_dir.rglob("*.pyc")):
        if path.is_file():
            try:
                path.unlink()
                removed_files += 1
            except Exception:
                pass

    return {"removed_pycache_dirs": removed_dirs, "removed_pyc_files": removed_files}


def _copy_clean(src: Path, dst: Path) -> None:
    def _ignore(_dir: str, names: List[str]) -> set[str]:
        ignored = set()
        for name in names:
            if name == "__pycache__":
                ignored.add(name)
            elif name.endswith(".pyc") or name == ".DS_Store":
                ignored.add(name)
        return ignored

    shutil.copytree(src, dst, ignore=_ignore)


def _package_skill(skill_stage_dir: Path, out_dir: Path) -> Path:
    skill_name = skill_stage_dir.name
    out_path = out_dir / f"{skill_name}.skill"
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(skill_stage_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(skill_stage_dir.parent)
            zf.write(path, rel)
    return out_path


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    workspace = Path(args.workspace_root).expanduser().resolve()
    skills_root = workspace / "skills"
    dist_root = Path(args.output_dir).expanduser().resolve()
    stage_root = workspace / "tmp" / "notebooklm_artifact_orchestrator_bundle_stage"

    if stage_root.exists():
        shutil.rmtree(stage_root)
    stage_root.mkdir(parents=True, exist_ok=True)
    dist_root.mkdir(parents=True, exist_ok=True)

    report: Dict[str, Any] = {
        "workspace": str(workspace),
        "skills": {},
        "bundle": {},
    }

    packaged_files: List[Path] = []
    errors: List[str] = []

    for name in TARGET_SKILLS:
        src = skills_root / name
        if not src.exists():
            errors.append(f"missing skill directory: {src}")
            continue
        if not (src / "SKILL.md").exists():
            errors.append(f"missing SKILL.md: {src}")
            continue

        cleanup_result = {"removed_pycache_dirs": 0, "removed_pyc_files": 0}
        if args.clean_source_caches:
            cleanup_result = _clean_source_caches(src)

        scan = _scan_skill(src)
        report["skills"][name] = {
            "source": str(src),
            "cleanup": cleanup_result,
            "findings": scan["findings"],
            "source_pycache_dirs": scan["pycache_dirs"],
            "source_pyc_files": scan["pyc_files"],
        }

        if scan["findings"] and args.strict:
            errors.append(f"sensitive pattern findings in {name}")
            continue

        staged = stage_root / name
        _copy_clean(src, staged)

        # post-copy assert
        staged_pycache = [str(p.relative_to(staged)) for p in staged.rglob("__pycache__") if p.is_dir()]
        staged_pyc = [str(p.relative_to(staged)) for p in staged.rglob("*.pyc") if p.is_file()]
        report["skills"][name]["staged_pycache_dirs"] = staged_pycache
        report["skills"][name]["staged_pyc_files"] = staged_pyc

        if (staged_pycache or staged_pyc) and args.strict:
            errors.append(f"staging still contains cache files for {name}")
            continue

        packaged = _package_skill(staged, dist_root)
        packaged_files.append(packaged)
        report["skills"][name]["package"] = str(packaged)

    report["errors"] = errors

    if errors and args.strict:
        report_path = dist_root / "bundle_audit_report.json"
        _write_text(report_path, json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps({"status": "failed", "report": str(report_path), "errors": errors}, ensure_ascii=False))
        return 1

    install_order = [
        "telegram-book-fetch",
        "notebooklm-chapter-menu",
        "notebooklm-guarded-generator",
        "book-to-artifact",
    ]

    bundle_manifest = {
        "bundle": args.bundle_name,
        "skills": TARGET_SKILLS,
        "install_order": install_order,
        "notes": [
            "Install dependency skills first, then book-to-artifact.",
            "Each .skill file is a zip with skill folder at archive root.",
        ],
    }

    manifest_path = dist_root / "bundle_manifest.json"
    _write_text(manifest_path, json.dumps(bundle_manifest, ensure_ascii=False, indent=2))

    readme_path = dist_root / "README.md"
    _write_text(
        readme_path,
        "\n".join(
            [
                "# notebooklm-artifact-orchestrator bundle",
                "",
                "Includes:",
                "- telegram-book-fetch",
                "- notebooklm-chapter-menu",
                "- notebooklm-guarded-generator",
                "- book-to-artifact",
                "",
                "## Install",
                "1. Unzip each `.skill` into your `skills/` directory.",
                "2. Follow install order in `bundle_manifest.json`.",
            ]
        ),
    )

    bundle_zip = dist_root / f"{args.bundle_name}.zip"
    with zipfile.ZipFile(bundle_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(packaged_files):
            zf.write(p, p.name)
        zf.write(manifest_path, manifest_path.name)
        zf.write(readme_path, readme_path.name)

    report["bundle"] = {
        "zip": str(bundle_zip),
        "manifest": str(manifest_path),
        "readme": str(readme_path),
        "skill_packages": [str(p) for p in packaged_files],
    }

    report_path = dist_root / "bundle_audit_report.json"
    _write_text(report_path, json.dumps(report, ensure_ascii=False, indent=2))

    print(
        json.dumps(
            {
                "status": "ok",
                "bundle_zip": str(bundle_zip),
                "report": str(report_path),
                "skill_packages": [str(p) for p in packaged_files],
            },
            ensure_ascii=False,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Package cleaned notebooklm-artifact-orchestrator dependency bundle")
    p.add_argument("--workspace-root", default=os.getcwd())
    p.add_argument("--output-dir", default="dist/notebooklm-artifact-orchestrator-bundle")
    p.add_argument("--bundle-name", default="notebooklm-artifact-orchestrator-bundle")
    p.add_argument("--clean-source-caches", action="store_true", default=True)
    p.add_argument("--no-clean-source-caches", dest="clean_source_caches", action="store_false")
    p.add_argument("--strict", action="store_true", default=True)
    p.add_argument("--no-strict", dest="strict", action="store_false")
    return p


def main() -> None:
    args = build_parser().parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    main()
