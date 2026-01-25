# pg_stat_ch Benchmark Environment

Docker-based benchmark environment for testing pg_stat_ch with ClickHouse Cloud.

## Prerequisites

- Docker and Docker Compose
- ClickHouse Cloud instance (or self-hosted ClickHouse)
- The `events_raw` table created in ClickHouse (see `docker/init/00-schema.sql`)

## Quick Start

1. **Configure ClickHouse connection:**

   ```bash
   cp .env.example .env
   # Edit .env with your ClickHouse Cloud credentials
   ```

2. **Build and start:**

   ```bash
   docker compose up -d --build
   ```

3. **Verify extension is loaded:**

   ```bash
   docker compose exec postgres psql -U postgres -d benchmark -c "SELECT * FROM pg_stat_ch_stats();"
   ```

4. **Run benchmark workload:**

   ```bash
   ./scripts/generate_load.sh -c 128 -d 60
   ```

5. **Verify events in ClickHouse:**

   ```bash
   clickhouse client --host your-instance.clickhouse.cloud --secure \
     -q "SELECT count() FROM pg_stat_ch.events_raw WHERE ts_start > now() - INTERVAL 5 MINUTE"
   ```

## Configuration

### Environment Variables (.env)

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_HOST` | ClickHouse server hostname | localhost |
| `CLICKHOUSE_PORT` | ClickHouse native protocol port | 9000 (9440 for TLS) |
| `CLICKHOUSE_USER` | ClickHouse username | default |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | (empty) |
| `CLICKHOUSE_DATABASE` | Target database | pg_stat_ch |
| `CLICKHOUSE_USE_TLS` | Enable TLS | false |
| `CLICKHOUSE_SKIP_TLS_VERIFY` | Skip TLS verification | false |

### Resource Limits

The container is limited to:
- 1 vCPU
- 2GB RAM

Adjust in `docker-compose.yml` if needed.

## Benchmark Schema

The benchmark database includes:

- `bench.users` - 10,000 seeded users
- `bench.inventory` - 1,000 products
- `bench.orders` - Order records
- `bench.transactions` - Transaction log

## Load Generator

The `scripts/generate_load.sh` script uses pgbench with custom SQL scripts:

```bash
# Run with 128 connections for 60 seconds
./scripts/generate_load.sh -c 128 -d 60

# Run with 64 connections for 5 minutes
./scripts/generate_load.sh -c 64 -d 300
```

### Workload Distribution

- 40% SELECT (reads, aggregations, joins)
- 25% INSERT (orders, transactions)
- 25% UPDATE (balances, statuses)
- 10% DELETE (cleanup)

## Useful Commands

```bash
# View logs
docker compose logs -f postgres

# Connect to PostgreSQL
docker compose exec postgres psql -U postgres -d benchmark

# Check pg_stat_ch stats
docker compose exec postgres psql -U postgres -d benchmark \
  -c "SELECT * FROM pg_stat_ch_stats();"

# Stop and remove
docker compose down -v

# Rebuild after code changes
docker compose up -d --build --force-recreate
```

## Troubleshooting

### Extension not loading

Check PostgreSQL logs:
```bash
docker compose logs postgres | grep -i "pg_stat_ch"
```

### ClickHouse connection issues

Verify connectivity from container:
```bash
docker compose exec postgres bash -c "
  echo 'SELECT 1' | timeout 5 nc -w 3 \$CLICKHOUSE_HOST \$CLICKHOUSE_PORT
"
```

### TLS certificate errors

If connecting to ClickHouse Cloud with self-signed certs or development instances:
```bash
# In .env
CLICKHOUSE_SKIP_TLS_VERIFY=true
```
