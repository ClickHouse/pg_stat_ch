# pg_stat_ch Load Generator

A Python-based load generator for benchmarking `pg_stat_ch` extension.

## Installation

```bash
cd benchmark/loadgen
uv sync
```

## Usage

```bash
# Basic usage (64 connections, 60 seconds)
uv run loadgen

# Custom configuration
uv run loadgen -c 128 -d 300  # 128 connections for 5 minutes

# Connect to different host/port
uv run loadgen -h localhost -p 5433 -D benchmark

# Skip pg_stat_ch stats display
uv run loadgen --no-show-stats
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --connections` | 64 | Number of concurrent connections |
| `-d, --duration` | 60 | Duration in seconds |
| `-h, --host` | localhost | PostgreSQL host |
| `-p, --port` | 5433 | PostgreSQL port |
| `-D, --database` | benchmark | Database name |
| `-U, --user` | postgres | Database user |
| `-W, --password` | postgres | Database password |
| `--show-stats` | enabled | Show pg_stat_ch stats before/after |

## Workload Distribution

The load generator runs four types of workloads with the following distribution:

| Workload | Weight | Description |
|----------|--------|-------------|
| SELECT | 40% | Lookups, joins, aggregations, pattern search |
| INSERT | 25% | New orders, transactions |
| UPDATE | 25% | Balance updates, status changes |
| DELETE | 10% | Cleanup of old records |

Each worker runs queries continuously for the specified duration, randomly selecting
from the available queries in its workload type.

## Output

The tool provides:
- Live progress with QPS and per-workload stats
- Final summary with total queries, errors, and latency breakdown
- pg_stat_ch stats before and after the benchmark (events exported, queue size, etc.)

## Example Output

```
╭─────────────────────────────╮
│ pg_stat_ch Load Generator   │
╰─────────────────────────────╯

Connections  64
Duration     60s
Workloads    SELECT 40%, INSERT 25%, UPDATE 25%, DELETE 10%

Progress: 100% (60s / 60s)

Workload   Queries   Errors   Avg (ms)
SELECT      24521        -       0.8
INSERT      15012        -       0.5
UPDATE      14998        -       0.4
DELETE       5892        -       0.3

QPS: 1007.1
```

## Development

```bash
# Install in development mode
uv sync

# Run directly
uv run python -m loadgen.cli --help
```
