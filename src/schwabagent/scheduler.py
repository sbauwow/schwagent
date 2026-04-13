"""Cron scheduler — persistent scheduled job queue.

Replaces manual `./run.sh loop` with a proper scheduler that survives
restarts. Jobs are persisted to ~/.schwagent/cron.json.

Supports:
- Cron expressions (e.g., "30 9 * * 1-5" = 9:30 weekdays)
- Interval-based (e.g., "every 5m", "every 1h")
- One-shot (e.g., "in 30m", "at 2024-04-05 15:00")

Usage:
    sched = Scheduler(config)
    sched.add_job("scan", "30 9 * * 1-5", runner.scan_only)
    sched.add_job("etf_check", "0 15 * * 1-5", runner.run_once)
    sched.add_job("pnl_report", "0 16 * * 1-5", send_daily_pnl)
    sched.start()  # background thread
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from schwabagent.config import Config

logger = logging.getLogger(__name__)

# Try to import croniter for cron expression parsing
try:
    from croniter import croniter
    _HAS_CRONITER = True
except ImportError:
    _HAS_CRONITER = False
    logger.debug("croniter not installed — cron expressions won't work, only interval schedules")


@dataclass
class Job:
    """A scheduled job."""
    name: str
    schedule: str             # cron expression or "every Nm/Nh"
    callback: Callable | None = None  # set at runtime, not persisted
    enabled: bool = True
    last_run: str = ""        # ISO timestamp
    next_run: str = ""        # ISO timestamp
    run_count: int = 0
    last_error: str = ""
    one_shot: bool = False    # if True, remove after first execution


class Scheduler:
    """Persistent cron-like scheduler running in a background thread."""

    def __init__(self, config: Config):
        self._config = config
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._path = Path(config.STATE_DIR).expanduser() / "cron.json"
        self._tick_interval = 10  # check every 10 seconds

        self._load()

    # ── Job management ───────────────────────────────────────────────────

    def add_job(
        self,
        name: str,
        schedule: str,
        callback: Callable,
        enabled: bool = True,
        one_shot: bool = False,
    ) -> None:
        """Add or update a scheduled job."""
        with self._lock:
            existing = self._jobs.get(name)
            if existing:
                existing.schedule = schedule
                existing.callback = callback
                existing.enabled = enabled
                existing.one_shot = one_shot
            else:
                job = Job(
                    name=name,
                    schedule=schedule,
                    callback=callback,
                    enabled=enabled,
                    one_shot=one_shot,
                )
                job.next_run = self._compute_next_run(schedule)
                self._jobs[name] = job

        self._save()
        logger.info("Job '%s' scheduled: %s (next: %s)", name, schedule, self._jobs[name].next_run)

    def remove_job(self, name: str) -> None:
        with self._lock:
            self._jobs.pop(name, None)
        self._save()

    def enable_job(self, name: str) -> None:
        with self._lock:
            if name in self._jobs:
                self._jobs[name].enabled = True
        self._save()

    def disable_job(self, name: str) -> None:
        with self._lock:
            if name in self._jobs:
                self._jobs[name].enabled = False
        self._save()

    def list_jobs(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "name": j.name,
                    "schedule": j.schedule,
                    "enabled": j.enabled,
                    "last_run": j.last_run,
                    "next_run": j.next_run,
                    "run_count": j.run_count,
                    "last_error": j.last_error,
                    "one_shot": j.one_shot,
                }
                for j in sorted(self._jobs.values(), key=lambda x: x.next_run or "")
            ]

    # ── Lifecycle ────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="scheduler",
        )
        self._thread.start()
        logger.info("Scheduler started with %d jobs", len(self._jobs))

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Scheduler stopped")

    def _loop(self) -> None:
        while self._running:
            try:
                self._tick()
            except Exception as e:
                logger.error("Scheduler tick failed: %s", e)
            for _ in range(self._tick_interval):
                if not self._running:
                    break
                time.sleep(1)

    # ── Tick — check for due jobs ────────────────────────────────────────

    def _tick(self) -> None:
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()

        with self._lock:
            jobs = list(self._jobs.values())

        for job in jobs:
            if not job.enabled or not job.callback:
                continue
            if not job.next_run:
                job.next_run = self._compute_next_run(job.schedule)
                continue

            if job.next_run <= now_iso:
                self._execute_job(job)

    def _execute_job(self, job: Job) -> None:
        """Run a job and update its state."""
        logger.info("Executing job '%s'", job.name)
        start = time.monotonic()

        try:
            job.callback()
            job.last_error = ""
        except Exception as e:
            job.last_error = str(e)[:200]
            logger.error("Job '%s' failed: %s", job.name, e)

        job.last_run = datetime.now(timezone.utc).isoformat()
        job.run_count += 1
        duration = time.monotonic() - start
        logger.info("Job '%s' completed in %.1fs (run #%d)", job.name, duration, job.run_count)

        if job.one_shot:
            with self._lock:
                self._jobs.pop(job.name, None)
        else:
            job.next_run = self._compute_next_run(job.schedule)

        self._save()

    # ── Schedule parsing ─────────────────────────────────────────────────

    def _compute_next_run(self, schedule: str) -> str:
        """Compute the next run time from a schedule expression."""
        now = datetime.now(timezone.utc)
        schedule = schedule.strip()

        # Interval: "every 5m", "every 1h", "every 30s"
        if schedule.startswith("every "):
            return self._parse_interval(schedule, now)

        # One-shot: "in 30m", "in 2h"
        if schedule.startswith("in "):
            return self._parse_in(schedule, now)

        # Cron expression
        if _HAS_CRONITER:
            try:
                cron = croniter(schedule, now)
                return cron.get_next(datetime).replace(tzinfo=timezone.utc).isoformat()
            except Exception as e:
                logger.error("Invalid cron expression '%s': %s", schedule, e)

        # Fallback: try as interval
        return (now + timedelta(minutes=5)).isoformat()

    @staticmethod
    def _parse_interval(schedule: str, now: datetime) -> str:
        """Parse 'every 5m', 'every 1h', 'every 30s'."""
        part = schedule.replace("every ", "").strip()
        if part.endswith("m"):
            delta = timedelta(minutes=int(part[:-1]))
        elif part.endswith("h"):
            delta = timedelta(hours=int(part[:-1]))
        elif part.endswith("s"):
            delta = timedelta(seconds=int(part[:-1]))
        else:
            delta = timedelta(minutes=int(part))
        return (now + delta).isoformat()

    @staticmethod
    def _parse_in(schedule: str, now: datetime) -> str:
        """Parse 'in 30m', 'in 2h'."""
        part = schedule.replace("in ", "").strip()
        if part.endswith("m"):
            delta = timedelta(minutes=int(part[:-1]))
        elif part.endswith("h"):
            delta = timedelta(hours=int(part[:-1]))
        else:
            delta = timedelta(minutes=int(part))
        return (now + delta).isoformat()

    # ── Persistence ──────────────────────────────────────────────────────

    def _save(self) -> None:
        with self._lock:
            data = {
                name: {
                    "name": j.name, "schedule": j.schedule, "enabled": j.enabled,
                    "last_run": j.last_run, "next_run": j.next_run,
                    "run_count": j.run_count, "last_error": j.last_error,
                    "one_shot": j.one_shot,
                }
                for name, j in self._jobs.items()
            }
        try:
            self._path.write_text(json.dumps(data, indent=2))
        except OSError as e:
            logger.warning("Failed to save scheduler state: %s", e)

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text())
            for name, d in data.items():
                self._jobs[name] = Job(
                    name=d["name"], schedule=d["schedule"], enabled=d.get("enabled", True),
                    last_run=d.get("last_run", ""), next_run=d.get("next_run", ""),
                    run_count=d.get("run_count", 0), last_error=d.get("last_error", ""),
                    one_shot=d.get("one_shot", False),
                )
            if self._jobs:
                logger.info("Restored %d scheduled jobs from disk", len(self._jobs))
        except Exception as e:
            logger.warning("Failed to load scheduler state: %s", e)

    # ── Default schedule ─────────────────────────────────────────────────

    def setup_defaults(self, runner: Any) -> None:
        """Register the standard trading day schedule.

        Call this after the runner is initialized to wire up all default jobs.
        """
        if not _HAS_CRONITER:
            logger.warning("croniter not installed — using interval schedules instead of cron")
            self.add_job("scan", "every 5m", runner.scan_only)
            self.add_job("execute", "every 5m", runner.run_once)
            self.add_job("dreamcycle", "every 30m", runner.dreamcycle.run_once)
            return

        # Weekday schedule (Mon-Fri)
        self.add_job("morning_scan", "35 9 * * 1-5", runner.scan_only)
        self.add_job("execute_cycle", "40 9 * * 1-5", runner.run_once)
        self.add_job("midday_scan", "0 12 * * 1-5", runner.scan_only)
        self.add_job("etf_rotation", "0 15 * * 1-5", runner.run_once)
        self.add_job("dreamcycle", "0 16 * * 1-5", runner.dreamcycle.run_once)

        logger.info("Default trading schedule configured")
