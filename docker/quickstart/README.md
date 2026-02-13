# Quickstart Stack (Local PostgreSQL + ClickHouse)

This stack runs local PostgreSQL (with `pg_stat_ch`) and local ClickHouse with schema preloaded.

From repo root:

```bash
./scripts/quickstart.sh up
./scripts/quickstart.sh check
```

Connect without local clients installed:

```bash
./scripts/quickstart.sh pg
./scripts/quickstart.sh ch
```

Stop:

```bash
./scripts/quickstart.sh down
```

Service endpoints:
- PostgreSQL: `localhost:55432`
- ClickHouse native: `localhost:29000`
- ClickHouse HTTP: `localhost:28123`
