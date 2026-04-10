"""Swarm data models — dataclass-based for zero new dependencies.

Adapted from HKUDS/vibe-trading (MIT licensed). The original uses
pydantic; we use dataclasses to avoid adding a dependency since
schwab-agent's pydantic usage is already limited to config.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    """Task lifecycle status."""

    pending = "pending"
    blocked = "blocked"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class RunStatus(str, Enum):
    """Run lifecycle status."""

    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


@dataclass
class SwarmAgentSpec:
    """Role definition for a single agent in a swarm.

    Attributes:
        id: Unique identifier (e.g. "bull_advocate").
        role: Short role description.
        system_prompt: System prompt injected into the LLM.
        skills: Whitelist of skill names the agent can reference.
        timeout_seconds: Worker timeout in seconds.
        max_tokens: Max output tokens for the LLM call.
    """

    id: str
    role: str
    system_prompt: str
    skills: list[str] = field(default_factory=list)
    timeout_seconds: int = 300
    max_tokens: int = 2048


@dataclass
class SwarmTask:
    """A task node in the swarm DAG.

    Attributes:
        id: Unique task identifier.
        agent_id: ID of the agent that executes this task.
        prompt_template: User prompt template supporting {var} placeholders.
        depends_on: List of upstream task IDs that must complete first.
        input_from: Mapping of template key → upstream task ID, used to
            inject upstream task outputs into the prompt under those keys.
        status: Current task status.
        summary: Task output text after completion.
        error: Error message on failure.
        started_at: ISO timestamp when the task started.
        completed_at: ISO timestamp when the task finished.
    """

    id: str
    agent_id: str
    prompt_template: str
    depends_on: list[str] = field(default_factory=list)
    input_from: dict[str, str] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.pending
    summary: str | None = None
    error: str | None = None
    started_at: str | None = None
    completed_at: str | None = None


@dataclass
class SwarmPreset:
    """A named multi-agent workflow loaded from YAML.

    Attributes:
        name: Preset name.
        title: Human-readable title.
        description: What this preset does.
        agents: List of agent specs.
        tasks: List of tasks forming the DAG.
        variables: List of user-provided variable definitions
            (each dict has keys: name, description, required, default).
    """

    name: str
    title: str
    description: str
    agents: list[SwarmAgentSpec]
    tasks: list[SwarmTask]
    variables: list[dict[str, Any]] = field(default_factory=list)

    def get_agent(self, agent_id: str) -> SwarmAgentSpec | None:
        """Return an agent by ID, or None if not found."""
        for agent in self.agents:
            if agent.id == agent_id:
                return agent
        return None


@dataclass
class SwarmRun:
    """A single execution of a swarm preset.

    Attributes:
        id: Unique run identifier.
        preset_name: Name of the preset being executed.
        status: Overall run status.
        user_vars: User-provided variables for template rendering.
        tasks: All task entries (copied from the preset and mutated).
        created_at: ISO timestamp when the run started.
        completed_at: ISO timestamp when the run finished.
        final_report: Text of the final aggregated task, or None.
    """

    id: str
    preset_name: str
    status: RunStatus = RunStatus.pending
    user_vars: dict[str, str] = field(default_factory=dict)
    tasks: list[SwarmTask] = field(default_factory=list)
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    completed_at: str | None = None
    final_report: str | None = None

    def get_task(self, task_id: str) -> SwarmTask | None:
        """Return a task by ID, or None if not found."""
        for task in self.tasks:
            if task.id == task_id:
                return task
        return None
