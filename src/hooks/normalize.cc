// Query normalization functions copied from PostgreSQL's
// contrib/pg_stat_statements/pg_stat_statements.c (PG18.1)
// These replace constants with $1, $2, ... placeholders.
// PostgreSQL does not export these functions, so every extension
// that needs normalization must copy them (pg_stat_monitor does the same).

extern "C" {
#include "postgres.h"

#include "common/int.h"
#include "common/keywords.h"
#include "nodes/queryjumble.h"
#include "parser/scanner.h"
}

#include "hooks/normalize.h"

extern "C" {

static void FillInConstantLengths(JumbleState* jstate, const char* query, int query_loc);
static int CompLocation(const void* a, const void* b);

static char* GenerateNormalizedQuery(JumbleState* jstate, const char* query, int query_loc,
                                     int* query_len_p) {
  char* norm_query;
  int query_len = *query_len_p;
  int norm_query_buflen;
  int len_to_wrt;
  int quer_loc = 0;
  int n_quer_loc = 0;
  int last_off = 0;
  int last_tok_len = 0;
  int num_constants_replaced = 0;

  FillInConstantLengths(jstate, query, query_loc);

  // Allow for $n symbols to be longer than the constants they replace.
  norm_query_buflen = query_len + jstate->clocations_count * 10;

  norm_query = static_cast<char*>(palloc(norm_query_buflen + 1));

  for (int i = 0; i < jstate->clocations_count; i++) {
    int off;
    int tok_len;

#if PG_VERSION_NUM >= 180000
    // PG18+: skip external params unless we have squashed lists
    if (jstate->clocations[i].extern_param && !jstate->has_squashed_lists)
      continue;
#endif

    off = jstate->clocations[i].location;
    off -= query_loc;

    tok_len = jstate->clocations[i].length;

    if (tok_len < 0)
      continue;

    len_to_wrt = off - last_off;
    len_to_wrt -= last_tok_len;
    Assert(len_to_wrt >= 0);
    memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
    n_quer_loc += len_to_wrt;

#if PG_VERSION_NUM >= 180000
    n_quer_loc +=
        sprintf(norm_query + n_quer_loc, "$%d%s",
                num_constants_replaced + 1 + jstate->highest_extern_param_id,
                jstate->clocations[i].squashed ? " /*, ... */" : "");
#else
    n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d",
                          num_constants_replaced + 1 + jstate->highest_extern_param_id);
#endif
    num_constants_replaced++;

    quer_loc = off + tok_len;
    last_off = off;
    last_tok_len = tok_len;
  }

  len_to_wrt = query_len - quer_loc;
  Assert(len_to_wrt >= 0);
  memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
  n_quer_loc += len_to_wrt;

  Assert(n_quer_loc <= norm_query_buflen);
  norm_query[n_quer_loc] = '\0';

  *query_len_p = n_quer_loc;
  return norm_query;
}

static void FillInConstantLengths(JumbleState* jstate, const char* query, int query_loc) {
  LocationLen* locs;
  core_yyscan_t yyscanner;
  core_yy_extra_type yyextra;
  core_YYSTYPE yylval;
  YYLTYPE yylloc;

  if (jstate->clocations_count > 1)
    qsort(jstate->clocations, jstate->clocations_count, sizeof(LocationLen), CompLocation);
  locs = jstate->clocations;

  yyscanner = scanner_init(query, &yyextra, &ScanKeywords, ScanKeywordTokens);

  yyextra.escape_string_warning = false;

  for (int i = 0; i < jstate->clocations_count; i++) {
    int loc;
    int tok;

    if (i > 0 && locs[i].location == locs[i - 1].location) {
      locs[i].length = -1;
      continue;
    }

#if PG_VERSION_NUM >= 180000
    if (locs[i].squashed)
      continue;
#endif

    loc = locs[i].location - query_loc;
    Assert(loc >= 0);

    for (;;) {
      tok = core_yylex(&yylval, &yylloc, yyscanner);

      if (tok == 0)
        break;

      if (yylloc >= loc) {
        if (query[loc] == '-') {
          tok = core_yylex(&yylval, &yylloc, yyscanner);
          if (tok == 0)
            break;
        }
        locs[i].length = strlen(yyextra.scanbuf + loc);
        break;
      }
    }

    if (tok == 0)
      break;
  }

  scanner_finish(yyscanner);
}

static int CompLocation(const void* a, const void* b) {
  int l = (static_cast<const LocationLen*>(a))->location;
  int r = (static_cast<const LocationLen*>(b))->location;
#if PG_VERSION_NUM >= 170000
  return pg_cmp_s32(l, r);
#else
  return (l > r) - (l < r);
#endif
}

char* PschNormalizeQuery(JumbleState* jstate, const char* query, int query_loc, int* query_len_p) {
  if (jstate == nullptr || jstate->clocations_count <= 0)
    return nullptr;

  return GenerateNormalizedQuery(jstate, query, query_loc, query_len_p);
}

}  // extern "C"
