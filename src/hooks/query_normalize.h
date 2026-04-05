// Query normalization: replace literal constants with $N placeholders
//
// Ported from PostgreSQL's pg_stat_statements extension. Uses the JumbleState
// produced by core's post_parse_analyze_hook to locate constants in the query
// text, then replaces them with positional parameters ($1, $2, ...).
//
// This prevents sensitive literal values (passwords, PII, tokens) from being
// exported to ClickHouse or OpenTelemetry while preserving query structure,
// table names, column names, and operators.
#ifndef PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_H_
#define PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#if PG_VERSION_NUM >= 140000
#include "nodes/queryjumble.h"
#endif

// Generate a normalized query string with constants replaced by $1, $2, ...
// Returns a palloc'd string (caller must pfree). Returns NULL if jstate is
// NULL or has no constant locations.
//
// query:     the original query text
// query_loc: byte offset of the statement within query (for multi-statement)
// query_len: in/out — input length, updated to normalized length on return
// jstate:    JumbleState from post_parse_analyze_hook (contains constant locations)
char* PschNormalizeQuery(const char* query, int query_loc, int* query_len, JumbleState* jstate);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_HOOKS_QUERY_NORMALIZE_H_
