# Installing pg_stat_ch

1. Copy files to PostgreSQL directories:
   ```bash
   sudo cp pg_stat_ch.so $(pg_config --pkglibdir)/
   sudo cp pg_stat_ch.control pg_stat_ch--*.sql $(pg_config --sharedir)/extension/
   ```

2. Add to postgresql.conf:
   ```
   shared_preload_libraries = 'pg_stat_ch'
   ```

3. Restart PostgreSQL and create extension:
   ```sql
   CREATE EXTENSION pg_stat_ch;
   ```
