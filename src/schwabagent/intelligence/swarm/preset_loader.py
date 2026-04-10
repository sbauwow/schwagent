"""YAML preset loader.

Loads a swarm preset YAML file (agents + tasks + variables) and returns
a SwarmPreset instance. PyYAML is already a dependency of schwab-agent
(used by pydantic-settings).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from schwabagent.intelligence.swarm.models import (
    SwarmAgentSpec,
    SwarmPreset,
    SwarmTask,
)


def load_preset(path: str | Path) -> SwarmPreset:
    """Load a swarm preset from a YAML file.

    Args:
        path: Path to the YAML file.

    Returns:
        SwarmPreset instance.

    Raises:
        FileNotFoundError: If the preset file does not exist.
        ValueError: If the preset structure is invalid.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Preset not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _build_preset(data)


def load_preset_by_name(name: str, presets_dir: Path | None = None) -> SwarmPreset:
    """Load a preset by name from the bundled presets directory.

    Args:
        name: Preset name (matches the YAML filename without extension).
        presets_dir: Optional override for the presets directory.

    Returns:
        SwarmPreset instance.
    """
    presets_dir = presets_dir or Path(__file__).resolve().parent / "presets"
    path = presets_dir / f"{name}.yaml"
    return load_preset(path)


def list_presets(presets_dir: Path | None = None) -> list[str]:
    """Return the names of all bundled presets (filenames without .yaml)."""
    presets_dir = presets_dir or Path(__file__).resolve().parent / "presets"
    if not presets_dir.exists():
        return []
    return sorted(p.stem for p in presets_dir.glob("*.yaml"))


def _build_preset(data: dict[str, Any]) -> SwarmPreset:
    """Build a SwarmPreset from a parsed YAML dict.

    Raises ValueError on missing required fields or invalid shapes.
    """
    if not isinstance(data, dict):
        raise ValueError("preset YAML must be a mapping at the top level")

    name = data.get("name")
    if not name:
        raise ValueError("preset missing required field: name")

    agents_raw = data.get("agents") or []
    tasks_raw = data.get("tasks") or []

    agents: list[SwarmAgentSpec] = []
    for a in agents_raw:
        if not a.get("id"):
            raise ValueError("agent missing required field: id")
        agents.append(
            SwarmAgentSpec(
                id=a["id"],
                role=a.get("role", ""),
                system_prompt=a.get("system_prompt", ""),
                skills=list(a.get("skills", []) or []),
                timeout_seconds=int(a.get("timeout_seconds", 300)),
                max_tokens=int(a.get("max_tokens", 2048)),
            )
        )

    tasks: list[SwarmTask] = []
    for t in tasks_raw:
        if not t.get("id"):
            raise ValueError("task missing required field: id")
        if not t.get("agent_id"):
            raise ValueError(f"task '{t['id']}' missing required field: agent_id")
        tasks.append(
            SwarmTask(
                id=t["id"],
                agent_id=t["agent_id"],
                prompt_template=t.get("prompt_template", ""),
                depends_on=list(t.get("depends_on", []) or []),
                input_from=dict(t.get("input_from", {}) or {}),
            )
        )

    return SwarmPreset(
        name=name,
        title=data.get("title", name),
        description=data.get("description", ""),
        agents=agents,
        tasks=tasks,
        variables=list(data.get("variables", []) or []),
    )
