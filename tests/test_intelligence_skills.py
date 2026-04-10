"""Tests for the intelligence skills loader."""
from __future__ import annotations

from pathlib import Path

import pytest

from schwabagent.intelligence.skills import (
    Skill,
    SkillsLoader,
    _load_skill_dir,
    _parse_frontmatter,
)


# ── Frontmatter parsing ──────────────────────────────────────────────────────

def test_parse_frontmatter_basic():
    text = """---
name: my-skill
description: A test skill
category: strategy
---
# Body content
Some text here."""
    meta, body = _parse_frontmatter(text)
    assert meta["name"] == "my-skill"
    assert meta["description"] == "A test skill"
    assert meta["category"] == "strategy"
    assert body == "# Body content\nSome text here."


def test_parse_frontmatter_list_values():
    text = """---
name: foo
tags: [one, two, 'three']
---
body"""
    meta, _ = _parse_frontmatter(text)
    assert meta["tags"] == ["one", "two", "three"]


def test_parse_frontmatter_bool_values():
    text = """---
name: foo
enabled: true
deprecated: false
---
body"""
    meta, _ = _parse_frontmatter(text)
    assert meta["enabled"] is True
    assert meta["deprecated"] is False


def test_parse_frontmatter_missing():
    text = "# Just a body\nNo frontmatter."
    meta, body = _parse_frontmatter(text)
    assert meta == {}
    assert body == text.strip()


# ── Skill directory loading ──────────────────────────────────────────────────

def test_load_skill_dir_valid(tmp_path: Path):
    skill_dir = tmp_path / "test-skill"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text(
        "---\nname: test-skill\ndescription: desc\ncategory: strategy\n---\nBody."
    )
    skill = _load_skill_dir(skill_dir)
    assert skill is not None
    assert skill.name == "test-skill"
    assert skill.description == "desc"
    assert skill.category == "strategy"
    assert skill.body == "Body."
    assert skill.dir_path == skill_dir


def test_load_skill_dir_missing_file(tmp_path: Path):
    empty = tmp_path / "empty"
    empty.mkdir()
    assert _load_skill_dir(empty) is None


def test_load_skill_dir_fallback_name_from_dir(tmp_path: Path):
    skill_dir = tmp_path / "dir-name"
    skill_dir.mkdir()
    # No name in frontmatter — should fall back to dir name
    (skill_dir / "SKILL.md").write_text(
        "---\ndescription: has no name\n---\nBody."
    )
    skill = _load_skill_dir(skill_dir)
    assert skill is not None
    assert skill.name == "dir-name"


# ── SkillsLoader ─────────────────────────────────────────────────────────────

@pytest.fixture
def temp_skills_dir(tmp_path: Path) -> Path:
    """Create a minimal fake skills_lib for testing."""
    def make(name: str, category: str, desc: str, body: str = "Body"):
        d = tmp_path / name
        d.mkdir()
        (d / "SKILL.md").write_text(
            f"---\nname: {name}\ndescription: {desc}\ncategory: {category}\n---\n{body}"
        )

    make("alpha", "strategy", "First strategy")
    make("beta", "analysis", "First analysis")
    make("gamma", "strategy", "Second strategy")
    return tmp_path


def test_loader_loads_all_skills(temp_skills_dir: Path):
    loader = SkillsLoader(skills_dir=temp_skills_dir)
    assert len(loader.skills) == 3
    assert set(loader.names()) == {"alpha", "beta", "gamma"}


def test_loader_get_by_name(temp_skills_dir: Path):
    loader = SkillsLoader(skills_dir=temp_skills_dir)
    skill = loader.get("alpha")
    assert skill is not None
    assert skill.category == "strategy"
    assert loader.get("nonexistent") is None


def test_loader_by_category(temp_skills_dir: Path):
    loader = SkillsLoader(skills_dir=temp_skills_dir)
    groups = loader.by_category()
    assert set(groups.keys()) == {"strategy", "analysis"}
    assert len(groups["strategy"]) == 2
    assert len(groups["analysis"]) == 1


def test_loader_get_descriptions_grouped(temp_skills_dir: Path):
    loader = SkillsLoader(skills_dir=temp_skills_dir)
    output = loader.get_descriptions()
    # strategy category should come before analysis per display order
    strategy_idx = output.find("### strategy")
    analysis_idx = output.find("### analysis")
    assert strategy_idx >= 0 and analysis_idx >= 0
    assert strategy_idx < analysis_idx
    assert "alpha: First strategy" in output
    assert "beta: First analysis" in output
    assert "gamma: Second strategy" in output


def test_loader_get_content_known(temp_skills_dir: Path):
    loader = SkillsLoader(skills_dir=temp_skills_dir)
    content = loader.get_content("alpha")
    assert '<skill name="alpha">' in content
    assert "Body" in content
    assert content.endswith("</skill>")


def test_loader_get_content_unknown_lists_available(temp_skills_dir: Path):
    loader = SkillsLoader(skills_dir=temp_skills_dir)
    result = loader.get_content("nonexistent")
    assert "Error" in result
    assert "alpha" in result
    assert "beta" in result
    assert "gamma" in result


def test_loader_empty_dir(tmp_path: Path):
    loader = SkillsLoader(skills_dir=tmp_path)
    assert loader.skills == []
    assert loader.get_descriptions() == "(no skills)"


def test_loader_nonexistent_dir(tmp_path: Path):
    loader = SkillsLoader(skills_dir=tmp_path / "does-not-exist")
    assert loader.skills == []


# ── Skill support file loading ───────────────────────────────────────────────

def test_skill_load_support_file(tmp_path: Path):
    skill_dir = tmp_path / "test"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text("---\nname: test\n---\nBody")
    (skill_dir / "examples.md").write_text("example content")

    skill = _load_skill_dir(skill_dir)
    assert skill is not None
    assert skill.load_support_file("examples.md") == "example content"
    assert skill.load_support_file("missing.md") is None


def test_skill_support_file_no_dir_path():
    skill = Skill(name="test")  # no dir_path set
    assert skill.load_support_file("anything.md") is None


# ── Bundled skills smoke test ────────────────────────────────────────────────

def test_bundled_skills_load_successfully():
    """The packaged skills_lib/ should load without errors."""
    loader = SkillsLoader()  # default path
    assert len(loader.skills) >= 20, "expected at least 20 bundled skills"
    # Every skill should have a name and description
    for skill in loader.skills:
        assert skill.name, f"skill missing name: {skill}"
        assert skill.description, f"skill {skill.name} missing description"
        assert skill.category, f"skill {skill.name} missing category"


def test_bundled_skills_expected_categories():
    loader = SkillsLoader()
    cats = set(loader.by_category().keys())
    # Core categories from the curated subset
    assert "strategy" in cats
    assert "analysis" in cats
    assert "asset-class" in cats
