"""DAG algorithms — cycle detection and topological layering.

Adapted from HKUDS/vibe-trading (MIT licensed).
"""
from __future__ import annotations

from collections import defaultdict, deque

from schwabagent.intelligence.swarm.models import SwarmTask


def validate_dag(tasks: list[SwarmTask]) -> None:
    """DFS cycle detection to ensure the task DAG is acyclic.

    Also validates that every depends_on reference points to a known task.

    Args:
        tasks: List of SwarmTask.

    Raises:
        ValueError: If a cycle is detected or a dependency is unknown.
    """
    all_ids = {t.id for t in tasks}

    for task in tasks:
        for dep in task.depends_on:
            if dep not in all_ids:
                raise ValueError(
                    f"Task '{task.id}' depends on unknown task '{dep}'"
                )

    graph: dict[str, list[str]] = {t.id: list(t.depends_on) for t in tasks}

    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {tid: WHITE for tid in all_ids}
    path: list[str] = []

    def dfs(node: str) -> None:
        color[node] = GRAY
        path.append(node)
        for neighbor in graph.get(node, []):
            if color[neighbor] == GRAY:
                cycle_start = path.index(neighbor)
                cycle = path[cycle_start:] + [neighbor]
                raise ValueError(
                    f"Cycle detected in task DAG: {' -> '.join(cycle)}"
                )
            if color[neighbor] == WHITE:
                dfs(neighbor)
        path.pop()
        color[node] = BLACK

    for tid in all_ids:
        if color[tid] == WHITE:
            dfs(tid)


def topological_layers(tasks: list[SwarmTask]) -> list[list[str]]:
    """Kahn's algorithm topological layering.

    Tasks within the same returned layer have no dependencies on each other
    and can safely run in parallel.

    Args:
        tasks: List of SwarmTask (must form a valid acyclic DAG).

    Returns:
        List of layers in execution order. Each layer is a list of task IDs.

    Raises:
        ValueError: If the DAG contains a cycle (layering cannot complete).
    """
    in_degree: dict[str, int] = {t.id: 0 for t in tasks}
    dependents: dict[str, list[str]] = defaultdict(list)

    for task in tasks:
        in_degree[task.id] = len(task.depends_on)
        for dep in task.depends_on:
            dependents[dep].append(task.id)

    queue: deque[str] = deque(tid for tid, deg in in_degree.items() if deg == 0)

    layers: list[list[str]] = []
    processed = 0

    while queue:
        layer: list[str] = sorted(queue)
        queue.clear()
        layers.append(layer)
        processed += len(layer)

        for tid in layer:
            for downstream in dependents[tid]:
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

    if processed != len(tasks):
        raise ValueError(
            f"DAG contains a cycle: processed {processed}/{len(tasks)} tasks"
        )

    return layers
