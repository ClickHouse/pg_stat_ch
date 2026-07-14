# Installing pg_stat_ch

All dependencies are statically linked except OpenSSL, which resolves to system `libssl.so.3`/`libcrypto.so.3` at runtime (OpenSSL 3.x, present on any SSL-enabled PostgreSQL host).

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
