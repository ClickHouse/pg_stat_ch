// Query normalization — ported from pg_stat_statements
//
// The two core functions (fill_in_constant_lengths, generate_normalized_query)
// are static in contrib/pg_stat_statements/pg_stat_statements.c. We reproduce
// them here so pg_stat_ch can normalize independently of pg_stat_statements.

extern "C" {
#include "postgres.h"

#include "nodes/queryjumble.h"
#include "parser/scanner.h"
}

#include "hooks/query_normalize.h"

// Comparator for qsorting LocationLen structs by location.
static int CompLocation(const void* a, const void* b) {
  int l = (static_cast<const LocationLen*>(a))->location;
  int r = (static_cast<const LocationLen*>(b))->location;
  if (l < r) {
    return -1;
  }
  return (l > r) ? 1 : 0;
}

// Populate the length field of LocationLen entries using the PostgreSQL lexer.
// Core only provides locations; we need to lex the query to find token lengths.
// Ported from pg_stat_statements fill_in_constant_lengths().
static void FillInConstantLengths(JumbleState* jstate, const char* query, int query_loc) {
  LocationLen* locs;
  core_yyscan_t yyscanner;
  core_yy_extra_type yyextra;
  core_YYSTYPE yylval;
  YYLTYPE yylloc;

  if (jstate->clocations_count > 1) {
    qsort(jstate->clocations, jstate->clocations_count, sizeof(LocationLen), CompLocation);
  }
  locs = jstate->clocations;

  yyscanner = scanner_init(query, &yyextra, &ScanKeywords, ScanKeywordTokens);

  for (int i = 0; i < jstate->clocations_count; i++) {
    int loc;
    int tok;

    // Ignore duplicate constants at the same location
    if (i > 0 && locs[i].location == locs[i - 1].location) {
      locs[i].length = -1;
      continue;
    }

    if (locs[i].squashed) {
      continue;
    }

    loc = locs[i].location - query_loc;
    Assert(loc >= 0);

    for (;;) {
      tok = core_yylex(&yylval, &yylloc, yyscanner);
      if (tok == 0) {
        break;
      }
      if (yylloc >= loc) {
        if (query[loc] == '-') {
          // Negative numeric constant — consume the number token too
          tok = core_yylex(&yylval, &yylloc, yyscanner);
          if (tok == 0) {
            break;
          }
        }
        locs[i].length = static_cast<int>(strlen(yyextra.scanbuf + loc));
        break;
      }
    }

    if (tok == 0) {
      break;
    }
  }

  scanner_finish(yyscanner);
}

char* PschNormalizeQuery(const char* query, int query_loc, int* query_len_p, JumbleState* jstate) {
  if (jstate == nullptr || jstate->clocations_count <= 0) {
    return nullptr;
  }

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

  // Allow for $N symbols to be longer than replaced constants.
  // A constant is at least 1 byte; $N is at most 11 bytes (INT_MAX).
  norm_query_buflen = query_len + (jstate->clocations_count * 10);
  norm_query = static_cast<char*>(palloc(norm_query_buflen + 1));

  for (int i = 0; i < jstate->clocations_count; i++) {
    int off;
    int tok_len;

    // If we have an external param at this location but no squashed lists,
    // skip it so the original $N text is preserved.
    if (jstate->clocations[i].extern_param && !jstate->has_squashed_lists) {
      continue;
    }

    off = jstate->clocations[i].location;
    off -= query_loc;
    tok_len = jstate->clocations[i].length;

    if (tok_len < 0) {
      continue;  // duplicate, ignore
    }

    // Copy the chunk between last constant and this one
    len_to_wrt = off - last_off - last_tok_len;
    Assert(len_to_wrt >= 0);
    memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
    n_quer_loc += len_to_wrt;

    // Insert $N placeholder (and squashed-list comment if applicable)
    n_quer_loc += sprintf(norm_query + n_quer_loc, "$%d%s",
                          num_constants_replaced + 1 + jstate->highest_extern_param_id,
                          jstate->clocations[i].squashed ? " /*, ... */" : "");
    num_constants_replaced++;

    quer_loc = off + tok_len;
    last_off = off;
    last_tok_len = tok_len;
  }

  // Copy remaining query text after the last constant
  len_to_wrt = query_len - quer_loc;
  Assert(len_to_wrt >= 0);
  memcpy(norm_query + n_quer_loc, query + quer_loc, len_to_wrt);
  n_quer_loc += len_to_wrt;

  Assert(n_quer_loc <= norm_query_buflen);
  norm_query[n_quer_loc] = '\0';

  *query_len_p = n_quer_loc;
  return norm_query;
}
