// Query normalization — ported from pg_stat_statements
//
// The two core functions (fill_in_constant_lengths, generate_normalized_query)
// are static in contrib/pg_stat_statements/pg_stat_statements.c. We reproduce
// them here so pg_stat_ch can normalize independently of pg_stat_statements.

#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/queryjumble.h"
#include "parser/scanner.h"

#include "hooks/query_normalize.h"

// Comparator for qsorting LocationLen structs by location.
static int CompLocation(const void* a, const void* b) {
  int l = ((const LocationLen*)a)->location;
  int r = ((const LocationLen*)b)->location;
  if (l < r) {
    return -1;
  }
  return (l > r) ? 1 : 0;
}

#if PG_VERSION_NUM >= 180000
static bool IsSquashedConstant(const LocationLen* loc) {
  return loc->squashed;
}

static bool ShouldPreserveExternalParam(const JumbleState* jstate, int index) {
  return jstate->clocations[index].extern_param && !jstate->has_squashed_lists;
}
#else
static bool IsSquashedConstant(const LocationLen* loc) {
  (void)loc;
  return false;
}

static bool ShouldPreserveExternalParam(const JumbleState* jstate, int index) {
  (void)jstate;
  (void)index;
  return false;
}
#endif

static void InitNormalizedQueryBuffer(StringInfoData* norm_query, int query_len) {
  initStringInfo(norm_query);
  enlargeStringInfo(norm_query, Max(query_len, 32));
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
  yyextra.escape_string_warning = false;

  for (int i = 0; i < jstate->clocations_count; i++) {
    int loc;
    int tok;

    // Ignore duplicate constants at the same location
    if (i > 0 && locs[i].location == locs[i - 1].location) {
      locs[i].length = -1;
      continue;
    }

    if (IsSquashedConstant(&locs[i])) {
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
        locs[i].length = (int)strlen(yyextra.scanbuf + loc);
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
  if (jstate == NULL || jstate->clocations_count <= 0) {
    return NULL;
  }

  int query_len = *query_len_p;
  int len_to_wrt;
  int quer_loc = 0;
  int last_off = 0;
  int last_tok_len = 0;
  int num_constants_replaced = 0;
  StringInfoData norm_query;

  FillInConstantLengths(jstate, query, query_loc);

  InitNormalizedQueryBuffer(&norm_query, query_len + 1);

  for (int i = 0; i < jstate->clocations_count; i++) {
    int off;
    int tok_len;

    // If we have an external param at this location but no squashed lists,
    // skip it so the original $N text is preserved.
    if (ShouldPreserveExternalParam(jstate, i)) {
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
    appendBinaryStringInfo(&norm_query, query + quer_loc, len_to_wrt);

    // Insert $N placeholder (and squashed-list comment if applicable)
    appendStringInfo(&norm_query, "$%d%s",
                     num_constants_replaced + 1 + jstate->highest_extern_param_id,
                     IsSquashedConstant(&jstate->clocations[i]) ? " /*, ... */" : "");
    num_constants_replaced++;

    quer_loc = off + tok_len;
    last_off = off;
    last_tok_len = tok_len;
  }

  // Copy remaining query text after the last constant
  len_to_wrt = query_len - quer_loc;
  Assert(len_to_wrt >= 0);
  appendBinaryStringInfo(&norm_query, query + quer_loc, len_to_wrt);

  *query_len_p = norm_query.len;
  return norm_query.data;
}
