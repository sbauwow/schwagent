"""Swarm multi-agent orchestration — DAG-based task execution.

Adapted from HKUDS/vibe-trading (MIT licensed).

This simplified port uses single LLM calls per task (no ReAct loop).
Each task is bound to an agent spec (system prompt + allowed skills),
renders a prompt template with user variables + upstream task outputs,
and produces a text summary that downstream tasks can reference.
"""
from schwabagent.intelligence.swarm.models import (
    RunStatus,
    SwarmAgentSpec,
    SwarmPreset,
    SwarmRun,
    SwarmTask,
    TaskStatus,
)
from schwabagent.intelligence.swarm.preset_loader import (
    list_presets,
    load_preset,
    load_preset_by_name,
)
from schwabagent.intelligence.swarm.runtime import SwarmRuntime
from schwabagent.intelligence.swarm.task_store import (
    topological_layers,
    validate_dag,
)

__all__ = [
    "RunStatus",
    "SwarmAgentSpec",
    "SwarmPreset",
    "SwarmRun",
    "SwarmRuntime",
    "SwarmTask",
    "TaskStatus",
    "list_presets",
    "load_preset",
    "load_preset_by_name",
    "topological_layers",
    "validate_dag",
]
