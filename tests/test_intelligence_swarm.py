"""Tests for the swarm orchestration layer."""
from __future__ import annotations

from pathlib import Path

import pytest

from schwabagent.intelligence.swarm import (
    SwarmAgentSpec,
    SwarmPreset,
    SwarmRuntime,
    SwarmTask,
    TaskStatus,
    list_presets,
    load_preset,
    load_preset_by_name,
    topological_layers,
    validate_dag,
)
from schwabagent.intelligence.swarm.worker import _render_prompt, _safe_format


# ── DAG validation ───────────────────────────────────────────────────────────

def test_validate_dag_acyclic_ok():
    tasks = [
        SwarmTask(id="a", agent_id="x", prompt_template="", depends_on=[]),
        SwarmTask(id="b", agent_id="x", prompt_template="", depends_on=["a"]),
        SwarmTask(id="c", agent_id="x", prompt_template="", depends_on=["a", "b"]),
    ]
    validate_dag(tasks)  # should not raise


def test_validate_dag_cycle_detection():
    tasks = [
        SwarmTask(id="a", agent_id="x", prompt_template="", depends_on=["c"]),
        SwarmTask(id="b", agent_id="x", prompt_template="", depends_on=["a"]),
        SwarmTask(id="c", agent_id="x", prompt_template="", depends_on=["b"]),
    ]
    with pytest.raises(ValueError, match="Cycle detected"):
        validate_dag(tasks)


def test_validate_dag_unknown_dependency():
    tasks = [
        SwarmTask(id="a", agent_id="x", prompt_template="", depends_on=["nonexistent"]),
    ]
    with pytest.raises(ValueError, match="unknown task"):
        validate_dag(tasks)


def test_validate_dag_self_loop():
    tasks = [
        SwarmTask(id="a", agent_id="x", prompt_template="", depends_on=["a"]),
    ]
    with pytest.raises(ValueError, match="Cycle detected"):
        validate_dag(tasks)


# ── Topological layers ──────────────────────────────────────────────────────

def test_topological_layers_parallel_roots():
    """Two independent roots should be in the same first layer."""
    tasks = [
        SwarmTask(id="a", agent_id="x", prompt_template=""),
        SwarmTask(id="b", agent_id="x", prompt_template=""),
        SwarmTask(id="c", agent_id="x", prompt_template="", depends_on=["a", "b"]),
    ]
    layers = topological_layers(tasks)
    assert len(layers) == 2
    assert set(layers[0]) == {"a", "b"}
    assert layers[1] == ["c"]


def test_topological_layers_diamond():
    """Classic diamond: a → {b,c} → d."""
    tasks = [
        SwarmTask(id="a", agent_id="x", prompt_template=""),
        SwarmTask(id="b", agent_id="x", prompt_template="", depends_on=["a"]),
        SwarmTask(id="c", agent_id="x", prompt_template="", depends_on=["a"]),
        SwarmTask(id="d", agent_id="x", prompt_template="", depends_on=["b", "c"]),
    ]
    layers = topological_layers(tasks)
    assert layers[0] == ["a"]
    assert set(layers[1]) == {"b", "c"}
    assert layers[2] == ["d"]


def test_topological_layers_linear_chain():
    tasks = [
        SwarmTask(id="a", agent_id="x", prompt_template=""),
        SwarmTask(id="b", agent_id="x", prompt_template="", depends_on=["a"]),
        SwarmTask(id="c", agent_id="x", prompt_template="", depends_on=["b"]),
    ]
    layers = topological_layers(tasks)
    assert layers == [["a"], ["b"], ["c"]]


# ── Prompt rendering ────────────────────────────────────────────────────────

def test_safe_format_substitutes_vars():
    out = _safe_format("hello {name}", {"name": "world"})
    assert out == "hello world"


def test_safe_format_leaves_unknown_keys():
    out = _safe_format("hello {name} and {other}", {"name": "world"})
    assert "hello world" in out
    assert "{other}" in out  # unknown key should remain


def test_render_prompt_with_upstream():
    rendered = _render_prompt(
        template="Analyze {target}.",
        user_vars={"target": "SPY"},
        input_from={"bull_case": "task-bull", "bear_case": "task-bear"},
        upstream_outputs={
            "task-bull": "SPY is bullish because X.",
            "task-bear": "SPY is bearish because Y.",
        },
    )
    assert "Analyze SPY" in rendered
    assert "## Upstream Context" in rendered
    assert "bull_case" in rendered
    assert "SPY is bullish because X." in rendered
    assert "SPY is bearish because Y." in rendered


def test_render_prompt_no_upstream():
    rendered = _render_prompt(
        template="Analyze {target}.",
        user_vars={"target": "SPY"},
        input_from={},
        upstream_outputs={},
    )
    assert rendered == "Analyze SPY."
    assert "Upstream Context" not in rendered


# ── Preset loading ──────────────────────────────────────────────────────────

def test_list_bundled_presets():
    presets = list_presets()
    assert len(presets) >= 3
    assert "investment_committee" in presets
    assert "technical_analysis_panel" in presets
    assert "etf_allocation_desk" in presets


def test_bundled_preset_loads_and_validates():
    for name in list_presets():
        preset = load_preset_by_name(name)
        assert preset.name == name
        assert len(preset.agents) > 0
        assert len(preset.tasks) > 0
        validate_dag(preset.tasks)
        # Every task must reference a known agent
        agent_ids = {a.id for a in preset.agents}
        for task in preset.tasks:
            assert task.agent_id in agent_ids


def test_preset_loader_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_preset(tmp_path / "nope.yaml")


def test_preset_loader_missing_name(tmp_path: Path):
    path = tmp_path / "bad.yaml"
    path.write_text("title: X\ndescription: Y\nagents: []\ntasks: []\n")
    with pytest.raises(ValueError, match="missing required field: name"):
        load_preset(path)


def test_preset_loader_task_missing_agent_id(tmp_path: Path):
    path = tmp_path / "bad.yaml"
    path.write_text(
        "name: test\n"
        "agents:\n"
        "  - id: a1\n"
        "    role: analyst\n"
        "tasks:\n"
        "  - id: t1\n"
        "    prompt_template: hello\n"  # missing agent_id
    )
    with pytest.raises(ValueError, match="missing required field: agent_id"):
        load_preset(path)


# ── Runtime execution (with mock LLM) ───────────────────────────────────────

class _MockLLM:
    """Mock LLM that echoes the prompt back with a label."""

    def __init__(self):
        self.calls: list[dict] = []
        self._skills = None

    @property
    def skills(self):
        # Return a mock SkillsLoader that returns None for any skill name
        class _MockLoader:
            def get(self, name):
                return None
        return _MockLoader()

    def generate(self, prompt: str, system: str = "", max_tokens: int | None = None) -> str:
        self.calls.append({
            "prompt": prompt,
            "system": system,
            "max_tokens": max_tokens,
        })
        # Return a deterministic summary based on which task this is
        if "Analyze" in prompt:
            return f"ANALYSIS: {prompt[:80]}"
        return f"OUTPUT for prompt starting: {prompt[:60]}"


def _make_simple_preset() -> SwarmPreset:
    """Build a diamond preset for testing."""
    agents = [
        SwarmAgentSpec(id="ag1", role="r1", system_prompt="sys1"),
        SwarmAgentSpec(id="ag2", role="r2", system_prompt="sys2"),
    ]
    tasks = [
        SwarmTask(id="t-a", agent_id="ag1", prompt_template="Analyze {target} A"),
        SwarmTask(id="t-b", agent_id="ag1", prompt_template="Analyze {target} B"),
        SwarmTask(
            id="t-c",
            agent_id="ag2",
            prompt_template="Synthesize {target}",
            depends_on=["t-a", "t-b"],
            input_from={"a": "t-a", "b": "t-b"},
        ),
    ]
    return SwarmPreset(
        name="test",
        title="Test Preset",
        description="diamond",
        agents=agents,
        tasks=tasks,
        variables=[{"name": "target", "required": True}],
    )


def test_runtime_executes_diamond_preset():
    preset = _make_simple_preset()
    llm = _MockLLM()
    runtime = SwarmRuntime(llm, max_workers=2)

    run = runtime.execute(preset, user_vars={"target": "SPY"})

    assert run.status.value == "completed"
    # All 3 tasks should have run
    assert len(llm.calls) == 3
    # All tasks completed
    for task in run.tasks:
        assert task.status == TaskStatus.completed
        assert task.summary is not None
    # Final report = output of last task
    assert run.final_report is not None
    # The aggregator should have received upstream context
    aggregator_call = next(c for c in llm.calls if "Synthesize" in c["prompt"])
    assert "Upstream Context" in aggregator_call["prompt"]
    assert "t-a" in aggregator_call["prompt"]
    assert "t-b" in aggregator_call["prompt"]


def test_runtime_missing_required_var():
    preset = _make_simple_preset()
    llm = _MockLLM()
    runtime = SwarmRuntime(llm)

    run = runtime.execute(preset, user_vars={})
    # Missing required var → run fails cleanly
    assert run.status.value == "failed"


def test_runtime_unknown_agent_fails_task():
    preset = SwarmPreset(
        name="broken",
        title="broken",
        description="",
        agents=[SwarmAgentSpec(id="only", role="r", system_prompt="s")],
        tasks=[
            SwarmTask(id="t1", agent_id="missing", prompt_template="hi"),
        ],
    )
    llm = _MockLLM()
    runtime = SwarmRuntime(llm)
    run = runtime.execute(preset)
    assert run.status.value == "failed"
    assert run.tasks[0].status == TaskStatus.failed
    assert "Unknown agent" in (run.tasks[0].error or "")


def test_runtime_preset_is_not_mutated_across_runs():
    """Running a preset twice should not leave residue in its task state."""
    preset = _make_simple_preset()
    llm = _MockLLM()
    runtime = SwarmRuntime(llm, max_workers=2)

    runtime.execute(preset, user_vars={"target": "SPY"})
    # preset.tasks[0] should still be pending (the runtime deep-copies)
    assert preset.tasks[0].status == TaskStatus.pending
    assert preset.tasks[0].summary is None

    run2 = runtime.execute(preset, user_vars={"target": "AAPL"})
    assert run2.status.value == "completed"
    assert len(llm.calls) == 6  # 3 from first run + 3 from second
