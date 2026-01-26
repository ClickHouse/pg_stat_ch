"""Load runner with concurrent workers."""

import random
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

import psycopg
from psycopg import Connection

from .workloads import DEFAULT_WORKLOADS, WorkloadConfig


@dataclass
class WorkerStats:
    """Statistics for a single worker."""

    queries: int = 0
    errors: int = 0
    total_time_ms: float = 0.0


@dataclass
class RunStats:
    """Aggregated statistics for a benchmark run."""

    workload_stats: dict[str, WorkerStats] = field(default_factory=dict)
    start_time: float = 0.0
    end_time: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add_query(self, workload: str, time_ms: float) -> None:
        with self.lock:
            if workload not in self.workload_stats:
                self.workload_stats[workload] = WorkerStats()
            self.workload_stats[workload].queries += 1
            self.workload_stats[workload].total_time_ms += time_ms

    def add_error(self, workload: str) -> None:
        with self.lock:
            if workload not in self.workload_stats:
                self.workload_stats[workload] = WorkerStats()
            self.workload_stats[workload].errors += 1

    @property
    def total_queries(self) -> int:
        return sum(s.queries for s in self.workload_stats.values())

    @property
    def total_errors(self) -> int:
        return sum(s.errors for s in self.workload_stats.values())

    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time

    @property
    def qps(self) -> float:
        if self.duration_seconds > 0:
            return self.total_queries / self.duration_seconds
        return 0.0


class Worker(threading.Thread):
    """A worker thread that executes queries."""

    def __init__(
        self,
        worker_id: int,
        conninfo: str,
        workload: WorkloadConfig,
        duration: float,
        stats: RunStats,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.conninfo = conninfo
        self.workload = workload
        self.duration = duration
        self.stats = stats
        self.stop_event = stop_event
        self.conn: Connection | None = None

    def run(self) -> None:
        try:
            # Connect with application name for visibility
            app_name = f"loadgen_{self.workload.name.lower()}_{self.worker_id}"
            self.conn = psycopg.connect(
                self.conninfo,
                autocommit=True,
                options=f"-c application_name={app_name}",
            )

            end_time = time.monotonic() + self.duration

            while time.monotonic() < end_time and not self.stop_event.is_set():
                # Pick a random query from this workload
                query_fn = random.choice(self.workload.queries)
                try:
                    start = time.monotonic()
                    with self.conn.cursor() as cur:
                        query_fn(cur)
                    elapsed_ms = (time.monotonic() - start) * 1000
                    self.stats.add_query(self.workload.name, elapsed_ms)
                except Exception as e:
                    self.stats.add_error(self.workload.name)

        except Exception as e:
            self.stats.add_error(self.workload.name)
        finally:
            if self.conn:
                self.conn.close()


class LoadRunner:
    """Orchestrates concurrent load generation."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5433,
        database: str = "benchmark",
        user: str = "postgres",
        password: str = "postgres",
    ):
        self.conninfo = f"host={host} port={port} dbname={database} user={user} password={password}"
        self.host = host
        self.port = port
        self.database = database
        self.user = user

    def get_stats(self) -> dict:
        """Get pg_stat_ch stats from the database."""
        try:
            with psycopg.connect(self.conninfo, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM pg_stat_ch_stats()")
                    cols = [desc[0] for desc in cur.description]
                    row = cur.fetchone()
                    if row:
                        return dict(zip(cols, row))
        except Exception as e:
            return {"error": str(e)}
        return {}

    def run(
        self,
        connections: int = 64,
        duration: int = 60,
        workloads: dict[str, WorkloadConfig] | None = None,
        progress_callback: Callable[[RunStats], None] | None = None,
    ) -> RunStats:
        """
        Run the benchmark.

        Args:
            connections: Total number of concurrent connections
            duration: Duration in seconds
            workloads: Workload configurations (defaults to DEFAULT_WORKLOADS)
            progress_callback: Called periodically with current stats

        Returns:
            RunStats with the benchmark results
        """
        if workloads is None:
            workloads = DEFAULT_WORKLOADS

        stats = RunStats()
        stop_event = threading.Event()
        workers: list[Worker] = []

        # Calculate connections per workload based on weights
        total_weight = sum(w.weight for w in workloads.values())
        remaining_conns = connections

        workload_conns: dict[str, int] = {}
        for name, config in workloads.items():
            conns = max(1, int(connections * config.weight / total_weight))
            workload_conns[name] = conns
            remaining_conns -= conns

        # Distribute any remaining connections
        if remaining_conns > 0:
            for name in workload_conns:
                if remaining_conns <= 0:
                    break
                workload_conns[name] += 1
                remaining_conns -= 1

        # Create workers
        worker_id = 0
        for name, config in workloads.items():
            num_conns = workload_conns[name]
            for _ in range(num_conns):
                worker = Worker(
                    worker_id=worker_id,
                    conninfo=self.conninfo,
                    workload=config,
                    duration=duration,
                    stats=stats,
                    stop_event=stop_event,
                )
                workers.append(worker)
                worker_id += 1

        # Start all workers
        stats.start_time = time.monotonic()
        for worker in workers:
            worker.start()

        # Progress reporting
        try:
            while any(w.is_alive() for w in workers):
                time.sleep(1)
                if progress_callback:
                    progress_callback(stats)
        except KeyboardInterrupt:
            stop_event.set()

        # Wait for all workers to finish
        for worker in workers:
            worker.join(timeout=5)

        stats.end_time = time.monotonic()
        return stats
