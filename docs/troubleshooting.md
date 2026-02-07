# Troubleshooting

## Extension Won't Load

```
WARNING:  pg_stat_ch must be loaded via shared_preload_libraries
```

Add `shared_preload_libraries = 'pg_stat_ch'` to `postgresql.conf` and restart PostgreSQL.

## Events Not Appearing in ClickHouse

1. Check connection settings:
   ```sql
   SHOW pg_stat_ch.clickhouse_host;
   SHOW pg_stat_ch.clickhouse_port;
   ```

2. Check stats for errors:
   ```sql
   SELECT * FROM pg_stat_ch_stats();
   ```

3. Check PostgreSQL logs for connection errors.

## High Queue Usage

If `queue_usage_pct` is consistently high:
- Increase `pg_stat_ch.queue_capacity` (restart required)
- Decrease `pg_stat_ch.flush_interval_ms`
- Increase `pg_stat_ch.batch_max`
- Ensure ClickHouse is healthy and reachable

## Dropped Events

Check the `dropped_events` counter:
```sql
SELECT dropped_events FROM pg_stat_ch_stats();
```

Dropped events indicate the queue filled faster than the background worker could export. This is safe (queries continue unaffected) but means some telemetry is lost.
