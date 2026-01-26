"""CLI for pg_stat_ch load generator."""

import click
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.text import Text

from .runner import LoadRunner, RunStats
from .workloads import DEFAULT_WORKLOADS

console = Console()


def format_stats_table(stats: RunStats, duration: int, elapsed: float) -> Table:
    """Create a table showing current benchmark stats."""
    table = Table(title="Workload Progress", box=None)
    table.add_column("Workload", style="cyan")
    table.add_column("Queries", justify="right", style="green")
    table.add_column("Errors", justify="right", style="red")
    table.add_column("Avg (ms)", justify="right", style="yellow")

    for name, ws in sorted(stats.workload_stats.items()):
        avg_ms = ws.total_time_ms / ws.queries if ws.queries > 0 else 0
        table.add_row(
            name,
            str(ws.queries),
            str(ws.errors) if ws.errors > 0 else "-",
            f"{avg_ms:.1f}",
        )

    table.add_section()
    table.add_row(
        "TOTAL",
        str(stats.total_queries),
        str(stats.total_errors) if stats.total_errors > 0 else "-",
        "",
        style="bold",
    )

    return table


def format_pg_stat_ch_stats(stats: dict) -> Table:
    """Create a table showing pg_stat_ch stats."""
    table = Table(title="pg_stat_ch Stats", box=None)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right", style="green")

    if "error" in stats:
        table.add_row("Error", stats["error"], style="red")
    else:
        # Show key metrics
        key_metrics = [
            ("exported_events", "Exported Events"),
            ("dropped_events", "Dropped Events"),
            ("queue_size", "Queue Size"),
            ("consecutive_failures", "Consecutive Failures"),
            ("last_success_ts", "Last Success"),
        ]
        for key, label in key_metrics:
            if key in stats:
                value = stats[key]
                style = None
                if key == "dropped_events" and value and int(value) > 0:
                    style = "red"
                elif key == "consecutive_failures" and value and int(value) > 0:
                    style = "yellow"
                table.add_row(label, str(value) if value is not None else "-", style=style)

    return table


@click.command()
@click.option(
    "-c", "--connections",
    default=64,
    help="Number of concurrent connections",
    show_default=True,
)
@click.option(
    "-d", "--duration",
    default=60,
    help="Duration in seconds",
    show_default=True,
)
@click.option(
    "-h", "--host",
    default="localhost",
    help="PostgreSQL host",
    show_default=True,
)
@click.option(
    "-p", "--port",
    default=5433,
    help="PostgreSQL port",
    show_default=True,
)
@click.option(
    "-D", "--database",
    default="benchmark",
    help="Database name",
    show_default=True,
)
@click.option(
    "-U", "--user",
    default="postgres",
    help="Database user",
    show_default=True,
)
@click.option(
    "-W", "--password",
    default="postgres",
    help="Database password",
    show_default=True,
)
@click.option(
    "--show-stats/--no-show-stats",
    default=True,
    help="Show pg_stat_ch stats before/after",
)
def main(
    connections: int,
    duration: int,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    show_stats: bool,
) -> None:
    """
    Generate load for pg_stat_ch benchmark testing.

    Runs concurrent workloads against PostgreSQL with different query types:

    \b
    - SELECT (40%): lookups, joins, aggregations
    - INSERT (25%): new orders, transactions
    - UPDATE (25%): balances, statuses
    - DELETE (10%): cleanup operations
    """
    runner = LoadRunner(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )

    # Header
    console.print()
    console.print(Panel.fit(
        "[bold cyan]pg_stat_ch Load Generator[/]",
        subtitle=f"{host}:{port}/{database}",
    ))
    console.print()

    # Configuration
    config_table = Table(box=None, show_header=False)
    config_table.add_column("Key", style="dim")
    config_table.add_column("Value")
    config_table.add_row("Connections", str(connections))
    config_table.add_row("Duration", f"{duration}s")

    # Show workload distribution
    total_weight = sum(w.weight for w in DEFAULT_WORKLOADS.values())
    workload_info = ", ".join(
        f"{w.name} {w.weight}%" for w in DEFAULT_WORKLOADS.values()
    )
    config_table.add_row("Workloads", workload_info)
    console.print(config_table)
    console.print()

    # Initial stats
    if show_stats:
        console.print("[dim]Fetching initial pg_stat_ch stats...[/]")
        initial_stats = runner.get_stats()
        console.print(format_pg_stat_ch_stats(initial_stats))
        console.print()

    # Run benchmark with live progress
    console.print("[bold green]Starting benchmark...[/]")
    console.print()

    elapsed = [0.0]

    def progress_callback(stats: RunStats) -> None:
        elapsed[0] = stats.end_time - stats.start_time if stats.end_time else 0

    with Live(console=console, refresh_per_second=2) as live:
        def update_display(stats: RunStats) -> None:
            current_elapsed = stats.duration_seconds if stats.end_time else (
                __import__("time").monotonic() - stats.start_time
            )

            # Create progress display
            progress_pct = min(100, (current_elapsed / duration) * 100)

            display = Table.grid()
            display.add_row(
                f"[bold]Progress:[/] {progress_pct:.0f}% ({current_elapsed:.0f}s / {duration}s)"
            )
            display.add_row("")
            display.add_row(format_stats_table(stats, duration, current_elapsed))
            display.add_row("")
            display.add_row(f"[dim]QPS: {stats.qps:.1f}[/]")

            live.update(display)

        stats = runner.run(
            connections=connections,
            duration=duration,
            progress_callback=update_display,
        )

    console.print()
    console.print("[bold green]Benchmark complete![/]")
    console.print()

    # Final summary
    summary_table = Table(title="Summary", box=None)
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", justify="right", style="green")
    summary_table.add_row("Total Queries", f"{stats.total_queries:,}")
    summary_table.add_row("Total Errors", f"{stats.total_errors:,}")
    summary_table.add_row("Duration", f"{stats.duration_seconds:.1f}s")
    summary_table.add_row("QPS", f"{stats.qps:,.1f}")
    console.print(summary_table)
    console.print()

    # Workload breakdown
    breakdown_table = Table(title="Breakdown by Workload", box=None)
    breakdown_table.add_column("Workload", style="cyan")
    breakdown_table.add_column("Queries", justify="right")
    breakdown_table.add_column("Errors", justify="right")
    breakdown_table.add_column("Avg Latency", justify="right")
    breakdown_table.add_column("QPS", justify="right")

    for name, ws in sorted(stats.workload_stats.items()):
        avg_ms = ws.total_time_ms / ws.queries if ws.queries > 0 else 0
        qps = ws.queries / stats.duration_seconds if stats.duration_seconds > 0 else 0
        breakdown_table.add_row(
            name,
            f"{ws.queries:,}",
            str(ws.errors) if ws.errors > 0 else "-",
            f"{avg_ms:.2f}ms",
            f"{qps:.1f}",
        )

    console.print(breakdown_table)
    console.print()

    # Final pg_stat_ch stats
    if show_stats:
        console.print("[dim]Fetching final pg_stat_ch stats...[/]")
        final_stats = runner.get_stats()
        console.print(format_pg_stat_ch_stats(final_stats))

        # Delta
        if "error" not in initial_stats and "error" not in final_stats:
            if "exported_events" in initial_stats and "exported_events" in final_stats:
                initial = int(initial_stats["exported_events"] or 0)
                final = int(final_stats["exported_events"] or 0)
                delta = final - initial
                console.print(f"\n[bold]Events exported during benchmark:[/] {delta:,}")

    console.print()


if __name__ == "__main__":
    main()
