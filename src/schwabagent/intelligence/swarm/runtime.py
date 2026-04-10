"""Swarm runtime — orchestrates preset execution across a task DAG.

This is a simplified port of vibe-trading's SwarmRuntime. Key simplifications:
- No file-based persistence (in-memory only — results are printed and
  returned; can be serialized via dataclasses.asdict if needed).
- No background thread / event streaming (runs synchronously).
- No ReAct loop per task — just one LLM call per task.

The DAG scheduling logic (topological layers + parallel execution
within a layer via ThreadPoolExecutor) is preserved.
"""
from __future__ import annotations

import copy
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from schwabagent.intelligence.swarm.models import (
    RunStatus,
    SwarmPreset,
    SwarmRun,
    SwarmTask,
    TaskStatus,
)
from schwabagent.intelligence.swarm.task_store import (
    topological_layers,
    validate_dag,
)
from schwabagent.intelligence.swarm.worker import run_task

logger = logging.getLogger(__name__)


class SwarmRuntime:
    """Execute a swarm preset against an LLM client.

    Usage:
        runtime = SwarmRuntime(llm)
        run = runtime.execute(preset, user_vars={"target": "SPY"})
        print(run.final_report)
    """

    def __init__(self, llm: Any, max_workers: int = 4) -> None:
        """Initialize the runtime.

        Args:
            llm: LLMClient instance with .generate(prompt, system, max_tokens)
                and .skills property.
            max_workers: Maximum parallel task workers within a single layer.
        """
        self.llm = llm
        self.max_workers = max_workers

    def execute(
        self,
        preset: SwarmPreset,
        user_vars: dict[str, str] | None = None,
    ) -> SwarmRun:
        """Run a swarm preset to completion.

        Args:
            preset: The loaded SwarmPreset.
            user_vars: User-provided variables for prompt template rendering.

        Returns:
            SwarmRun with all task outputs populated and final_report set.
        """
        user_vars = user_vars or {}

        # Deep copy tasks so the preset stays pristine across multiple runs
        tasks = [copy.deepcopy(t) for t in preset.tasks]

        run = SwarmRun(
            id=str(uuid.uuid4())[:8],
            preset_name=preset.name,
            status=RunStatus.running,
            user_vars=user_vars,
            tasks=tasks,
        )

        logger.info(
            "[swarm] Starting run %s (preset=%s, %d tasks)",
            run.id, preset.name, len(tasks),
        )

        try:
            self._check_required_vars(preset, user_vars)
            validate_dag(tasks)
            layers = topological_layers(tasks)
            logger.info("[swarm] %d layers: %s", len(layers), layers)

            outputs: dict[str, str] = {}

            for layer_idx, layer in enumerate(layers):
                logger.info(
                    "[swarm] Layer %d/%d: %d task(s) in parallel",
                    layer_idx + 1, len(layers), len(layer),
                )
                self._run_layer(preset, run, layer, outputs)

                # Bail out early if any task in this layer failed
                failed = [
                    t for t in run.tasks
                    if t.id in layer and t.status == TaskStatus.failed
                ]
                if failed:
                    raise RuntimeError(
                        f"Layer {layer_idx + 1} had {len(failed)} failed task(s): "
                        + ", ".join(t.id for t in failed)
                    )

            # Final report = output of the last task (typically the aggregator)
            if layers and layers[-1]:
                last_task_id = layers[-1][-1]
                last_task = run.get_task(last_task_id)
                if last_task and last_task.summary:
                    run.final_report = last_task.summary

            run.status = RunStatus.completed
            run.completed_at = datetime.now(timezone.utc).isoformat()
            logger.info("[swarm] Run %s completed successfully", run.id)
            return run

        except Exception as e:
            logger.error("[swarm] Run %s failed: %s", run.id, e)
            run.status = RunStatus.failed
            run.completed_at = datetime.now(timezone.utc).isoformat()
            return run

    # ── Layer execution ──────────────────────────────────────────────────

    def _run_layer(
        self,
        preset: SwarmPreset,
        run: SwarmRun,
        layer: list[str],
        outputs: dict[str, str],
    ) -> None:
        """Run all tasks in a single layer in parallel.

        Mutates run.tasks in place and populates outputs dict.
        """
        futures_to_task: dict[Any, SwarmTask] = {}

        with ThreadPoolExecutor(
            max_workers=min(self.max_workers, len(layer))
        ) as executor:
            for task_id in layer:
                task = run.get_task(task_id)
                if task is None:
                    continue
                agent = preset.get_agent(task.agent_id)
                if agent is None:
                    task.status = TaskStatus.failed
                    task.error = f"Unknown agent: {task.agent_id}"
                    continue

                future = executor.submit(
                    run_task,
                    task=task,
                    agent=agent,
                    user_vars=run.user_vars,
                    upstream_outputs=outputs,
                    llm=self.llm,
                )
                futures_to_task[future] = task

            for future in as_completed(futures_to_task):
                finished_task = future.result()
                if finished_task.summary:
                    outputs[finished_task.id] = finished_task.summary

    # ── Validation ───────────────────────────────────────────────────────

    @staticmethod
    def _check_required_vars(
        preset: SwarmPreset, user_vars: dict[str, str]
    ) -> None:
        """Verify that all variables marked required are provided.

        Raises ValueError with a helpful message listing missing vars.
        """
        missing: list[str] = []
        for var in preset.variables:
            name = var.get("name")
            if var.get("required") and name and name not in user_vars:
                missing.append(name)
        if missing:
            raise ValueError(
                f"Preset '{preset.name}' requires variables: {', '.join(missing)}"
            )
