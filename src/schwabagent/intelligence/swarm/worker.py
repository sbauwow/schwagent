"""Swarm worker — executes a single task via one LLM call.

This is a simplified port of vibe-trading's worker: instead of running
a full ReAct loop with tool calls, we do one LLM generation per task.
The agent's system prompt is augmented with the relevant skill catalog
(progressive disclosure) and the user prompt is rendered from the
template with variables + upstream task outputs.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from schwabagent.intelligence.swarm.models import (
    SwarmAgentSpec,
    SwarmTask,
    TaskStatus,
)

logger = logging.getLogger(__name__)


def run_task(
    task: SwarmTask,
    agent: SwarmAgentSpec,
    user_vars: dict[str, str],
    upstream_outputs: dict[str, str],
    llm: Any,
) -> SwarmTask:
    """Execute a single swarm task.

    Args:
        task: The task to run (will be mutated with status/summary/timestamps).
        agent: The agent spec bound to this task.
        user_vars: User-provided variables for template rendering.
        upstream_outputs: Mapping of upstream task_id → output text.
        llm: An LLMClient instance exposing .generate(prompt, system, max_tokens).

    Returns:
        The mutated SwarmTask with status/summary/timestamps filled in.
    """
    task.status = TaskStatus.in_progress
    task.started_at = datetime.now(timezone.utc).isoformat()

    try:
        system_prompt = _build_system_prompt(agent, llm)
        user_prompt = _render_prompt(
            task.prompt_template,
            user_vars=user_vars,
            input_from=task.input_from,
            upstream_outputs=upstream_outputs,
        )

        logger.info("[swarm] %s → %s (agent=%s)", task.id, agent.id, agent.id)
        output = llm.generate(
            prompt=user_prompt,
            system=system_prompt,
            max_tokens=agent.max_tokens,
        )

        task.summary = output
        task.status = TaskStatus.completed
        task.completed_at = datetime.now(timezone.utc).isoformat()
        return task

    except Exception as e:
        logger.error("[swarm] task %s failed: %s", task.id, e)
        task.error = str(e)
        task.status = TaskStatus.failed
        task.completed_at = datetime.now(timezone.utc).isoformat()
        return task


def _build_system_prompt(agent: SwarmAgentSpec, llm: Any) -> str:
    """Augment the agent's system prompt with its skill subset.

    Only the skills listed in agent.skills are injected. If the agent has
    no skills, we return the raw system prompt unchanged.
    """
    if not agent.skills:
        return agent.system_prompt

    try:
        loader = llm.skills  # lazy-loaded SkillsLoader
    except Exception:
        return agent.system_prompt

    # Filter the loader to just this agent's whitelisted skills
    lines: list[str] = []
    for name in agent.skills:
        skill = loader.get(name)
        if skill:
            lines.append(f"  - {skill.name}: {skill.description}")

    if not lines:
        return agent.system_prompt

    skills_block = (
        "\n\n## Skills Available (call load_skill(name) for full methodology)\n"
        + "\n".join(lines)
    )
    return agent.system_prompt + skills_block


def _render_prompt(
    template: str,
    user_vars: dict[str, str],
    input_from: dict[str, str],
    upstream_outputs: dict[str, str],
) -> str:
    """Render a prompt template with variables + upstream task outputs.

    Supports {var_name} placeholders for user vars. After substitution,
    if input_from is non-empty, upstream task outputs are appended as
    a labeled "Upstream Context" section.

    Args:
        template: Raw template with {var_name} placeholders.
        user_vars: User-provided variable map.
        input_from: Mapping of {template_key: upstream_task_id}.
        upstream_outputs: Mapping of {upstream_task_id: output_text}.

    Returns:
        Fully rendered prompt string.
    """
    # Substitute user vars — use format_map with a defaulting dict so
    # missing keys don't raise.
    rendered = _safe_format(template, user_vars)

    if input_from:
        sections = ["\n\n## Upstream Context\n"]
        for label, upstream_id in input_from.items():
            output = upstream_outputs.get(upstream_id, "").strip()
            if output:
                sections.append(f"\n### {label} (from {upstream_id})\n{output}\n")
        rendered += "".join(sections)

    return rendered


class _DefaultDict(dict):
    """Dict that returns '{key}' for missing keys so templates don't crash."""

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _safe_format(template: str, mapping: dict[str, str]) -> str:
    """str.format_map with a defaulting dict — missing keys stay as {key}."""
    try:
        return template.format_map(_DefaultDict(mapping))
    except Exception:
        return template
