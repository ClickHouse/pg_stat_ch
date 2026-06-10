# Design: memory-aware OTel exporter, C conversion, and GUC consolidation

**Branch:** `otel-rewrite` · **Date:** 2026-06-10 · **Status:** approved direction, pre-implementation
**Trigger:** production SIGABRT 2026-06-10 (gRPC `gpr_malloc` → `abort()` on NULL malloc under
`vm.overcommit_memory=2`; shmem-attached bgworker death → 49 s database-wide crash recovery).
Problem statement: `pg_stat_ch-sigabrt-problem-statement.md` (Downloads).

## 1. Goals and non-goals

**Goal:** no code path inside the postgres process may call `abort()` under memory pressure;
telemetry failure costs at most a bgworker restart (`proc_exit(1)`, `bgw_restart_time=10s`),
never database-wide crash recovery. Steady-state export performs **zero heap allocation**.

**Non-goals:** changing the wire contract (collector pipeline and `datagres_otel.query_logs_arrow`
ingestion must keep working unchanged); out-of-process export; keeping gRPC.

**Decisions (user-approved):**
1. In-process rewrite, no gRPC — single-threaded, hand-rolled OTLP client on the bgworker main thread
2. OTLP/HTTP (`POST <endpoint>/v1/logs`, `application/x-protobuf`), identical payload
3. Preallocated fixed memory budget; hot path cannot allocate
4. nanoarrow (vendored amalgamation) + custom IPC emit layer (see §4)
5. Entire project converts C++ → C (only 4 `.cc` files exist, all in `src/export/`)
6. Memory GUCs consolidate around `pg_stat_ch.memory_limit` (see §6)

## 2. What discovery established (oracles for the rewrite)

- **Wire payload (Arrow path):** Arrow IPC *streaming* format, V5, little-endian: schema message,
  13 dictionary batches (ids 0–12 in field order, `isDelta=false`, fresh per flush), 1 record
  batch, 8-byte EOS. 56 fields, exact names/types/order in `/tmp/wf1-code-arrow-batch.md`
  (traps: `shared_blks_written` BEFORE `dirtied`; `jit_deform_time_us` last among jit; `pid`/`query_id`
  are decimal strings; `severity`/`body`/`trace_id`/`span_id` constant `""`; `null_count=0` everywhere).
  Every body buffer ZSTD-framed: `BodyCompression{ZSTD, BUFFER}`, int64-LE uncompressed-length
  prefix + ZSTD frame (Arrow C++ never emits the `-1` raw escape). `ts = (ts_start + 946684800000000) * 1000` ns.
- **OTLP envelope:** resource attrs `service.name=pg_stat_ch`, `service.version`, `host.name`
  (GUC > `$HOSTNAME` > `gethostname()`), `pg_stat_ch.block_format=arrow_ipc`; scope
  `pg_stat_ch`/version; one LogRecord, body = `bytes_value(IPC)`, attrs `pg_stat_ch.block_format` +
  `pg_stat_ch.block_rows`. Per-record path: attributes-only records, no body, zero-valued int attrs
  skipped, duration dual-encoded (`db.client.operation.duration` seconds double + `duration_us` int).
- **OTLP/HTTP:** pin opentelemetry-proto **v1.9.0** field surface (never emit `strindex`,
  `EntityRef`, LogRecord field 4, ResourceLogs field 1000). Identity encoding legal. Retry only
  429/502/503/504 + connection-drop, honor `Retry-After` delta-seconds. 200-with-`partial_success`
  = do not retry, log rejects. otelcol HTTP body cap 20 MiB default — applies to **decompressed** body.
- **nanoarrow 0.8.0:** IPC writer exists but supports neither write-side compression nor dictionary
  batches → custom IPC emit layer (§4). Built with `NDEBUG` + `FLATCC_NO_ASSERT`: zero `abort()`
  paths, all OOM → `ENOMEM`. Vendor via `bundle.py --with-ipc --with-flatcc --symbol-namespace=PgStatCh`.
- **Today's biggest memory bug:** `DequeueEvents` reserves `batch_max × sizeof(PschEvent)` =
  200000 × 4600 B ≈ **877 MiB transient heap per export cycle** (`stats_exporter.cc:99-106`) —
  a larger overcommit hazard than the gRPC allocation that aborted. The rewrite deletes it.
- **Failure-semantics holes:** destructive dequeue before export (post-dequeue failure silently
  drops up to batch_max events, uncounted); backoff counter only increments on wire-send failure
  (bad_alloc/connect/build failures retry at 500 ms → the 2/sec log flood).
- **Verified sizes:** `sizeof(PschRingEntry)` = 520 B (not 512!), `sizeof(PschEvent)` ≈ 4600 B,
  intern HTAB sized at `queue_capacity` entries ≈ 592 B/slot amortized (ring+intern).

## 3. Architecture (all C)

```
hooks (C, unchanged) → shmem ring (C, + peek/commit) → bgworker (C)
                                                          ├─ stats_exporter.c   driver: chunked peek → dispatch → commit/requeue
                                                          ├─ exporter.h         PschExporterOps fn-pointer table + PschExportStatus
                                                          ├─ clickhouse_exporter.c  (port; clickhouse-c, already C underneath)
                                                          ├─ otel_exporter.c    NEW: OTLP/HTTP client (sockets + optional OpenSSL)
                                                          ├─ otlp_encode.c      NEW: protobuf wire encoder (v1.9.0 surface)
                                                          ├─ arrow_batch.c      NEW: nanoarrow arrays + custom IPC emitter
                                                          └─ third_party/nanoarrow/  vendored amalgamation (+ flatcc)
```

**Interface** (`src/export/exporter.h`, replaces `exporter_interface.h`): `PschExporterOps`
function-pointer table — `connect`, `is_connected`, `export_events(self, const PschEvent*, int n, int* exported)`,
`send_arrow(self, const uint8*, size_t, int rows)` (NULL = unsupported), `consecutive_failures`,
`reset_failures`, `destroy`. Backends consume `PschEvent` directly — the Column<T>/factory
abstraction and all per-row `std::string` temporaries are deleted, not ported. Status enum:
`OK / EMPTY / ERR_NOMEM / ERR_CONN / ERR_SEND / ERR_INTERNAL`.

**Threading:** strictly single-threaded; reuse proven CH-path patterns (`ConnectWithTimeout`
EINTR loop, `SO_RCVTIMEO/SNDTIMEO` + monotonic deadlines, `ProcDiePending` cancel callback).
No library may spawn threads in the bgworker.

## 4. Arrow batch builder (`arrow_batch.c`)

nanoarrow for schema/array building and buffer management; custom IPC message emitter because
nanoarrow's encoder can't write dictionary batches or compressed buffers:

- All `ArrowBuffer`s caller-owned, allocators installed via `ArrowBufferSetAllocator`, pre-reserved
  at worker start to high-water sizes; flatcc builder pages warm-up-allocated then reused
  (`flatcc_builder_reset` keeps capacity). Steady state: zero alloc.
- IPC emitter uses the flatbuffer builders already in the bundled `flatcc_generated.h`
  (`BodyCompression_create`, `RecordBatch_compression_add`, DictionaryBatch) to emit:
  schema msg → 13 dictionary batches → record batch → EOS, each message framed
  `0xFFFFFFFF | int32 LE len | flatbuffer (8-aligned) | body`.
- ZSTD per-buffer: preallocated `ZSTD_CCtx` (static alloc via `ZSTD_initStaticCCtx`, with
  `ZSTD_estimateCCtxSize` verify-at-start + windowLog degrade-and-LOG), output via
  `ZSTD_compressBound`-sized region, compress directly into the network/encode buffer.
  Match Arrow C++ behavior: compress every buffer unconditionally, level 1, never emit `-1` escape.
- Dictionary columns: fixed-capacity open-addressing memo table sized for full-chunk cardinality
  (worst case = rows per chunk), preallocated.
- Value transforms preserved exactly (clamps 63/63/45/2048/2048, `strnlen(sqlstate,5)`,
  negative→0 with 1/sec rate-limited WARNING, `PschCmdTypeToString`, extra_attributes parsing,
  first-wins dup keys).
- Oracle: capture fixture payloads from the **old** implementation via `debug_arrow_dump_dir`
  before deletion; `t/026_arrow_dump.pl` (pyarrow) must pass unchanged; add a decoded-equality
  comparison old-vs-new on identical synthetic events.

## 5. OTLP/HTTP exporter (`otel_exporter.c` + `otlp_encode.c`)

- Single-pass protobuf encoding into a preallocated buffer (length-delimited submessages via
  reserved fixed-width varint slots or leaf-first assembly — decided at implementation, both
  zero-alloc). Never emit post-v1.9.0 fields.
- HTTP/1.1 keep-alive POST `<endpoint>/v1/logs`, `Content-Type: application/x-protobuf`, no
  `Content-Encoding`. Parse status line + headers from a fixed response buffer; decode
  `partial_success` (log, don't retry) and `google.rpc.Status` (diagnostics).
- Retry/backoff: 429/502/503/504 and connection-drop → failure + requeue (§5a) + consecutive-failure
  backoff (existing 1 s × 2^n cap 60 s curve), honoring `Retry-After` (delta-seconds only).
  All other 4xx/5xx → permanent failure for that batch (consume + count as export-drop).
- `otel_endpooint` → URL semantics: scheme decides TLS (`http://localhost:4318` default); new GUC
  for static headers (auth parity with `OTEL_EXPORTER_OTLP_HEADERS`); CA-file GUC for TLS.
  OpenSSL only on the TLS path (allocation failures return NULL — checked).
- Request deadline: `export_timeout` (renamed from `otel_log_delay_ms`), default **1000 ms**
  (100 ms was gRPC-tuned; cannot absorb TLS reconnect), SIGHUP.
- Quiesce-on-OOM: `ERR_NOMEM` enters backoff like any failure; no allocation is attempted during
  backoff (buffers are static — the abort window is structurally gone).

### 5a. Failure semantics (changed deliberately, fixes both holes)

- `shmem.c` gains two-phase consume: `PschPeekEvents(buf, max)` / `PschConsumeEvents(n)` —
  safe, bgworker is the ring's sole consumer.
- Connection check still precedes peek. `ERR_CONN`/`ERR_SEND` → do NOT consume (events survive
  collector outages — upgrade over today's silent drop); `ERR_NOMEM`/`ERR_INTERNAL`, or send
  failure with `consecutive_failures ≥ 10` → consume anyway (poison-batch valve) + count in a
  new `export_dropped` stat (distinct from enqueue-overflow `dropped`).
- **Every** failure mode increments the backoff counter (kills the 2/sec flood).
- `PschExportBatch` returns 0 on any failure (drain-loop contract, `bgworker.c:133`); drain loop
  gets an explicit per-cycle iteration bound to preserve flush cadence and barrier processing.
- Staging: fixed chunk array (4096 events ≈ 18 MiB) preallocated at init — replaces the 877 MiB reserve.
- SIGABRT handler installed in `PschBgworkerMain` before `BackgroundWorkerUnblockSignals`:
  async-signal-safe `write(2, msg)` + `_exit(1)` only. Covers residual aborts (libc heap
  corruption paths keep SIGSEGV/SIGBUS full-crash semantics — only SIGABRT is trapped).
  Ring-sanity check on worker start compensates for skipped shmem-corruption reinit.
- `ereport(FATAL)` allowed only at init (exporter creation/preallocation failure → clean worker restart).
- Mid-batch longjmp recovery: backends reset batch state on entry; an `in_flight` flag forces
  reconnect instead of resuming a poisoned CH connection.

## 6. GUC consolidation (panel synthesis, critique-adjusted)

Surface: **8 memory knobs → 1 operator knob + 3 expert overrides.**

| GUC | Type/context | Default | Semantics |
|---|---|---|---|
| `memory_limit` | int MB, POSTMASTER | **160** | Total budget: ring + intern HTAB + DSA strings + worker export arena. 160 MB is the *verified* equivalence point of today's defaults (131072×520 B ring + intern + 64 MB DSA + ~19 MiB arena). `-1` = opt-in auto `clamp(shared_buffers/16, 48MB, 256MB)` — documented as changing sizes, never the boot default. |
| `queue_capacity` | int, POSTMASTER | -1 = auto | Auto: largest pow2 with full per-slot cost (520 B ring + ~72 B intern) ≤ ~50% of budget. Explicit values: check hook **rounds** to pow2 (no more startup error). |
| `string_area_size` | int MB, POSTMASTER | -1 = auto | Auto: DSA absorbs the budget remainder after ring+intern+arena (rounding loss becomes string capacity). |
| `export_buffer_size` | int MB, POSTMASTER | -1 = auto | Worker arena: staging chunk + arrow scratch + encode buffer + ZSTD cctx + network buffer. Encode ceiling 16 MiB **uncompressed** (otelcol 20 MiB decompressed cap). Replaces `otel_max_block_bytes`, `otel_log_max_bytes`, `otel_log_batch_size`, `batch_max`. |

Policy details (each traces to a critique finding):
- **Overrides auto-raise the budget with WARNING — never FATAL, never sibling-starvation.**
  Old fleet templates (`queue_capacity=131072` + `string_area_size=64`) must boot byte-identically.
- **Write-back** resolved values via `SetConfigOption(PGC_S_DYNAMIC_DEFAULT)` so `SHOW` reports
  effective sizes (wal_buffers idiom) — keeps auto-raise honest.
- **Intern HTAB is charged** per-slot inside the budget (it's sized by queue_capacity).
- Migration: dead shims (`otel_log_queue_size`, `otel_metric_interval_ms`) deleted now; live-folded
  GUCs get a one-release Citus-style bridge (hidden, honored iff successor = -1, deprecation
  WARNING on explicit set). Release notes must flag: prefix is reserved, so post-deletion
  `SET`/`ALTER SYSTEM` of old names **errors** — fleet template + `postgresql.auto.conf` scrub required.
- `otel_log_delay_ms` → `export_timeout` (SIGHUP, 1000 ms), bridged.
- `normalize_cache_max` unchanged this release (per-backend ≠ shared budget; documented exclusion
  with ×max_connections worst case; named AllocSet for `pg_backend_memory_contexts` visibility).
  Bytes-denominated rename is follow-up work.
- Observability ships in the same release: `pg_stat_ch_memory()` view (per-component
  budget/used/high-water/source: auto|override|derived), reason-split drop counters
  (`dropped_queue_full` / `dropped_string_oom` / `export_dropped`), startup LOG printing the full
  resolved derivation, overflow errhint rewritten to name `memory_limit` (restart) and
  `sample_rate`/`min_duration_us` (no restart). View = SQL surface → `.control` bump 0.3 → 0.4 + migration script.

## 7. Build / tooling

- Staged: ports land file-by-file under the existing dual glob; flip `project(LANGUAGES C)` +
  inverted stray-C++ FATAL check + delete `cxx_std_17`, `-include libintl.h`, `-Wglobal-constructors`
  block in one final commit. `C_STANDARD 17` + `C_EXTENSIONS ON` codified in CMake (replaces env CFLAGS).
- **Keep vcpkg for now** (3 deps: openssl, lz4, zstd) — release tarballs stay static/pinned;
  dropping vcpkg for system libs is a flagged follow-up decision (OpenSSL 1.1-era distro risk).
  Delete `opentelemetry-cpp` + `arrow` manifest entries (gRPC/protobuf/abseil go transitively).
- nanoarrow vendored as checked-in amalgamation under `third_party/nanoarrow/` (not a submodule)
  + `VERSION` pin; SYSTEM include; compiled as dedicated TUs (clickhouse-c pattern).
- `.clang-format`: keep Google-ish (`Language: Cpp` formats C); `.clang-tidy`: drop
  `modernize-*`/`google-*`, add `bugprone-*`; mise/CI format globs gain `-e c` — **one-time reformat
  of existing .c files as an isolated commit** (they were never format-enforced).
- CI: drop g++/CXX launcher/triplet CXX bits; keep clang as second compiler; verify with `nm`
  that no `__cxa_*`/`_ZSt` symbols remain in `pg_stat_ch.so`.
- Docs/skills: CLAUDE.md language/deps rewrite, README compiler line, retire cpp-* skills.

## 8. Sequencing

1. **Prep:** vendor nanoarrow; formatting-glob fix + bulk reformat (isolated commit); capture
   Arrow fixture payloads from old implementation.
2. **New C modules:** `otlp_encode.c` → `arrow_batch.c` (+IPC emitter) → `otel_exporter.c`.
3. **Ports:** `exporter.h`, `clickhouse_exporter.c`, `stats_exporter.c`, shmem peek/commit,
   SIGABRT handler. Old `.cc` files deleted in the same series (terminate handler lives until the
   last `.cc` dies; mangled bridge symbols force the interface flip to be atomic with the ports).
4. **Build flip:** LANGUAGES C, dep pruning, CI updates.
5. **GUC consolidation** + `pg_stat_ch_memory` view (+ control bump 0.3→0.4, migration script,
   `guc.out` update).
6. **Verification:** PG 16/17/18 builds (gcc+clang), regress/isolation/TAP (incl. tap-enabled
   local PG), byte-compat fixtures, OOM-injection (NULL-malloc shim), failure/backoff TAP test,
   valgrind batch loop, adversarial multi-agent review with an explicit abort-surface audit
   (`nm` + grep for abort/assert in linked objects).

## 9. Risk register (top)

| Risk | Mitigation |
|---|---|
| IPC byte-compat drift (dicts, ZSTD framing, padding) | old-impl fixtures + pyarrow decoded-equality + t/026 unchanged |
| Requeue introduces poison-batch wedge | consume-after-N-failures valve + `export_dropped` counter + TAP test |
| Per-request 100 ms deadline regression under TLS | `export_timeout` default 1000 ms |
| Old fleet templates vs new budget | auto-raise + WARNING + write-back; never FATAL |
| flatcc/nanoarrow hidden allocs on hot path | warm-up at init + `FLATCC_ALLOC` override option; verify with alloc-counting shim in tests |
| Drain-loop livelock without batch_max | explicit per-cycle iteration/time bound |
