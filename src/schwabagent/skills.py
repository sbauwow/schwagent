"""Skills framework — declarative, file-driven skill discovery and loading.

Skills are SKILL.md files with YAML frontmatter organized in a directory tree:

    ~/.schwagent/skills/
    ├── category/
    │   └── skill-name/
    │       ├── SKILL.md           # Main instructions (required)
    │       ├── references/        # Supporting documentation
    │       ├── templates/         # Output templates
    │       └── scripts/           # Executable scripts
    └── another-skill/
        └── SKILL.md

SKILL.md format:

    ---
    name: skill-name              # Required, max 64 chars
    description: Brief description # Required, max 1024 chars
    version: 1.0.0                # Optional
    tags: [etf, analysis]         # Optional
    platforms: [linux]            # Optional — restrict to OS
    requires_env: [API_KEY]       # Optional — env vars needed
    related_skills: [other-skill] # Optional
    ---

    # Skill Title

    Full instructions and content here...

Usage:
    from schwabagent.skills import SkillsManager

    mgr = SkillsManager()
    skills = mgr.list_skills()
    skill = mgr.load_skill("market-analysis")
    content = mgr.load_skill_file("market-analysis", "references/indicators.md")
"""
from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_EXCLUDED_DIRS = frozenset((".git", ".github", "__pycache__"))
_PLATFORM_MAP = {"macos": "darwin", "linux": "linux", "windows": "win32"}

# ── YAML loading ─────────────────────────────────────────────────────────────

_yaml_load_fn = None


def _yaml_load(content: str) -> dict:
    global _yaml_load_fn
    if _yaml_load_fn is None:
        import yaml
        loader = getattr(yaml, "CSafeLoader", None) or yaml.SafeLoader
        _yaml_load_fn = lambda v: yaml.load(v, Loader=loader)
    result = _yaml_load_fn(content)
    return result if isinstance(result, dict) else {}


def _parse_frontmatter(content: str) -> tuple[dict[str, Any], str]:
    """Parse YAML frontmatter from markdown. Returns (frontmatter, body)."""
    if not content.startswith("---"):
        return {}, content
    end = re.search(r"\n---\s*\n", content[3:])
    if not end:
        return {}, content
    yaml_str = content[3 : end.start() + 3]
    body = content[end.end() + 3 :]
    try:
        fm = _yaml_load(yaml_str)
    except Exception:
        fm = {}
    return fm, body


# ── Data models ──────────────────────────────────────────────────────────────

@dataclass
class SkillMeta:
    """Lightweight skill metadata for listing."""
    name: str
    description: str
    category: str = ""
    version: str = ""
    tags: list[str] = field(default_factory=list)
    path: str = ""  # relative path within skills dir


@dataclass
class Skill:
    """Full skill with content and linked files."""
    name: str
    description: str
    content: str  # full SKILL.md body (markdown)
    category: str = ""
    version: str = ""
    tags: list[str] = field(default_factory=list)
    related_skills: list[str] = field(default_factory=list)
    requires_env: list[str] = field(default_factory=list)
    path: str = ""
    linked_files: dict[str, list[str]] = field(default_factory=dict)
    missing_env: list[str] = field(default_factory=list)

    @property
    def is_ready(self) -> bool:
        return len(self.missing_env) == 0


# ── Skills Manager ───────────────────────────────────────────────────────────

class SkillsManager:
    """Discover, list, and load skills from the skills directory."""

    def __init__(self, skills_dirs: list[str | Path] | None = None):
        if skills_dirs:
            self._dirs = [Path(d).expanduser() for d in skills_dirs]
        else:
            self._dirs = [Path("~/.schwagent/skills").expanduser()]

    def list_skills(self, category: str | None = None) -> list[SkillMeta]:
        """List all available skills with metadata only (token-efficient)."""
        skills = []
        for skills_dir in self._dirs:
            if not skills_dir.is_dir():
                continue
            for skill_md in self._iter_skill_files(skills_dir):
                fm, _ = self._read_frontmatter(skill_md)
                if not fm.get("name"):
                    continue
                if not self._matches_platform(fm):
                    continue

                cat = self._get_category(skill_md, skills_dir)
                if category and cat != category:
                    continue

                skills.append(SkillMeta(
                    name=fm["name"],
                    description=str(fm.get("description", ""))[:1024],
                    category=cat,
                    version=str(fm.get("version", "")),
                    tags=self._as_list(fm.get("tags")),
                    path=str(skill_md.parent.relative_to(skills_dir)),
                ))
        return sorted(skills, key=lambda s: (s.category, s.name))

    def get_categories(self) -> list[str]:
        """Return sorted list of skill categories."""
        cats = set()
        for skills_dir in self._dirs:
            if not skills_dir.is_dir():
                continue
            for skill_md in self._iter_skill_files(skills_dir):
                cats.add(self._get_category(skill_md, skills_dir))
        return sorted(cats)

    def load_skill(self, name: str) -> Skill | None:
        """Load a skill by name. Returns full content + linked files."""
        skill_md = self._find_skill(name)
        if skill_md is None:
            return None

        content = skill_md.read_text(encoding="utf-8")
        fm, body = _parse_frontmatter(content)

        requires = self._as_list(fm.get("requires_env"))
        missing = [v for v in requires if not os.environ.get(v)]

        # Discover linked files
        skill_dir = skill_md.parent
        linked = {}
        for subdir in ("references", "templates", "scripts", "assets"):
            d = skill_dir / subdir
            if d.is_dir():
                linked[subdir] = sorted(
                    str(f.relative_to(d)) for f in d.rglob("*") if f.is_file()
                )

        skills_dir = self._find_skills_dir(skill_md)
        return Skill(
            name=fm.get("name", name),
            description=str(fm.get("description", ""))[:1024],
            content=body,
            category=self._get_category(skill_md, skills_dir) if skills_dir else "",
            version=str(fm.get("version", "")),
            tags=self._as_list(fm.get("tags")),
            related_skills=self._as_list(fm.get("related_skills")),
            requires_env=requires,
            path=str(skill_md.parent.relative_to(skills_dir)) if skills_dir else "",
            linked_files=linked,
            missing_env=missing,
        )

    def load_skill_file(self, name: str, file_path: str) -> str | None:
        """Load a specific file within a skill (e.g. 'references/api.md')."""
        skill_md = self._find_skill(name)
        if skill_md is None:
            return None
        target = skill_md.parent / file_path
        if not target.is_file():
            return None
        # Prevent path traversal
        try:
            target.resolve().relative_to(skill_md.parent.resolve())
        except ValueError:
            return None
        return target.read_text(encoding="utf-8")

    # ── Internal ─────────────────────────────────────────────────────────

    def _find_skill(self, name: str) -> Path | None:
        """Find a SKILL.md by skill name (searches all dirs)."""
        name_lower = name.lower().strip()
        for skills_dir in self._dirs:
            if not skills_dir.is_dir():
                continue
            for skill_md in self._iter_skill_files(skills_dir):
                fm, _ = self._read_frontmatter(skill_md)
                if fm.get("name", "").lower().strip() == name_lower:
                    return skill_md
                # Also match directory name
                if skill_md.parent.name.lower() == name_lower:
                    return skill_md
        return None

    def _find_skills_dir(self, skill_md: Path) -> Path | None:
        for d in self._dirs:
            try:
                skill_md.relative_to(d)
                return d
            except ValueError:
                continue
        return None

    def _iter_skill_files(self, skills_dir: Path):
        """Walk skills_dir yielding SKILL.md paths."""
        for root, dirs, files in os.walk(skills_dir):
            dirs[:] = [d for d in dirs if d not in _EXCLUDED_DIRS]
            if "SKILL.md" in files:
                yield Path(root) / "SKILL.md"

    def _read_frontmatter(self, path: Path) -> tuple[dict, str]:
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            return {}, ""
        return _parse_frontmatter(content)

    def _get_category(self, skill_md: Path, skills_dir: Path) -> str:
        rel = skill_md.parent.relative_to(skills_dir)
        parts = rel.parts
        return parts[0] if len(parts) > 1 else ""

    @staticmethod
    def _matches_platform(fm: dict) -> bool:
        platforms = fm.get("platforms")
        if not platforms:
            return True
        if not isinstance(platforms, list):
            platforms = [platforms]
        current = sys.platform
        for p in platforms:
            mapped = _PLATFORM_MAP.get(str(p).lower().strip(), str(p).lower().strip())
            if current.startswith(mapped):
                return True
        return False

    @staticmethod
    def _as_list(val) -> list[str]:
        if not val:
            return []
        if isinstance(val, str):
            return [val]
        return [str(v) for v in val if v]
