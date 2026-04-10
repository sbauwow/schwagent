"""SkillsLoader: loads scenario guides from the skills_lib/ directory.

Uses progressive disclosure:
- System prompt only injects one-line summaries (get_descriptions).
- Full docs loaded on demand (get_content, called by the load_skill tool).

Adapted from HKUDS/vibe-trading (MIT licensed).
Each skill is a self-contained directory with a SKILL.md file containing
YAML frontmatter (name, description, category) and a markdown body with
methodology, code examples, and references.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Skill:
    """Single skill definition.

    Attributes:
        name: Skill name (unique identifier).
        description: One-line summary shown in the system prompt.
        category: Category for grouped display (e.g. "strategy", "analysis").
        body: Full SKILL.md body text (loaded only via get_content).
        dir_path: Skill directory path for on-demand supporting files.
        metadata: Parsed YAML frontmatter.
    """

    name: str
    description: str = ""
    category: str = "other"
    body: str = ""
    dir_path: Path | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def load_support_file(self, filename: str) -> str | None:
        """Load a supporting file on demand (e.g. examples.md).

        Args:
            filename: File name relative to the skill directory.

        Returns:
            File content, or None if missing.
        """
        if not self.dir_path:
            return None
        path = self.dir_path / filename
        if not path.exists():
            return None
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            return None


def _parse_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Parse YAML frontmatter and body from a markdown file.

    Args:
        text: Full markdown text.

    Returns:
        Tuple of (metadata dict, body text).
    """
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)", text, re.DOTALL)
    if not match:
        return {}, text.strip()

    meta: dict[str, Any] = {}
    for line in match.group(1).strip().split("\n"):
        line = line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value.startswith("[") and value.endswith("]"):
            items = [item.strip().strip("'\"") for item in value[1:-1].split(",")]
            meta[key] = [i for i in items if i]
        elif value.lower() in ("true", "false"):
            meta[key] = value.lower() == "true"
        else:
            meta[key] = value

    return meta, match.group(2).strip()


def _load_skill_dir(dir_path: Path) -> Skill | None:
    """Load a single skill from a directory containing SKILL.md.

    Args:
        dir_path: Skill directory path.

    Returns:
        Skill instance, or None if the directory lacks a valid SKILL.md.
    """
    skill_file = dir_path / "SKILL.md"
    if not skill_file.exists():
        return None
    try:
        text = skill_file.read_text(encoding="utf-8")
    except OSError:
        return None

    meta, body = _parse_frontmatter(text)
    name = meta.get("name", dir_path.name)
    if not name:
        return None

    return Skill(
        name=name,
        description=meta.get("description", ""),
        category=meta.get("category", "other"),
        body=body,
        dir_path=dir_path,
        metadata=meta,
    )


class SkillsLoader:
    """Load and serve skill documentation with progressive disclosure.

    Usage:
        loader = SkillsLoader()
        system_prompt_snippet = loader.get_descriptions()  # one-liners
        full_doc = loader.get_content("technical-basic")   # on-demand
    """

    # Display order for categories (unlisted categories appear at the end).
    _CATEGORY_ORDER = [
        "strategy",
        "analysis",
        "asset-class",
        "data-source",
        "flow",
        "tool",
        "other",
    ]

    def __init__(self, skills_dir: Path | None = None) -> None:
        """Initialize the loader.

        Args:
            skills_dir: Directory containing skill subdirectories.
                Defaults to src/schwabagent/intelligence/skills_lib/.
        """
        self.skills_dir = (
            skills_dir or Path(__file__).resolve().parent / "skills_lib"
        )
        self.skills: list[Skill] = []
        self._load()

    def _load(self) -> None:
        """Scan skills_dir for subdirectories containing SKILL.md files."""
        if not self.skills_dir.exists():
            return
        for path in sorted(self.skills_dir.iterdir()):
            if path.is_dir() and (path / "SKILL.md").exists():
                skill = _load_skill_dir(path)
                if skill:
                    self.skills.append(skill)

    def get(self, name: str) -> Skill | None:
        """Return a skill by name, or None if not found."""
        for skill in self.skills:
            if skill.name == name:
                return skill
        return None

    def names(self) -> list[str]:
        """Return all skill names."""
        return [s.name for s in self.skills]

    def by_category(self) -> dict[str, list[Skill]]:
        """Group skills by category, preserving display order."""
        groups: dict[str, list[Skill]] = {}
        for skill in self.skills:
            groups.setdefault(skill.category, []).append(skill)
        return groups

    def get_descriptions(self) -> str:
        """Return skills grouped by category for the system prompt.

        This is the progressive-disclosure entry point: only one-line
        summaries are emitted, so the LLM knows what's available without
        blowing the context budget. Full content is loaded via get_content.

        Returns:
            Multi-line string with category headers and skill summaries.
        """
        if not self.skills:
            return "(no skills)"

        groups = self.by_category()
        ordered_cats = [c for c in self._CATEGORY_ORDER if c in groups]
        ordered_cats += [c for c in sorted(groups) if c not in ordered_cats]

        lines: list[str] = []
        for cat in ordered_cats:
            lines.append(f"\n### {cat}")
            for skill in groups[cat]:
                lines.append(f"  - {skill.name}: {skill.description}")
        return "\n".join(lines)

    def get_content(self, name: str) -> str:
        """Return the full documentation for a skill.

        Args:
            name: Skill name.

        Returns:
            XML-wrapped full skill document, or an error message listing
            available skills if the name is unknown.
        """
        skill = self.get(name)
        if skill is None:
            available = ", ".join(s.name for s in self.skills)
            return f"Error: Unknown skill '{name}'. Available: {available}"
        return f'<skill name="{name}">\n{skill.body}\n</skill>'
