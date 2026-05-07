// String utility helpers for pg_stat_ch hooks.
//
// Small functions for copying, trimming, and truncating strings that appear
// in exported events (query text, database/user names, error messages).

#ifndef PG_STAT_CH_SRC_HOOKS_STRING_UTILS_H_
#define PG_STAT_CH_SRC_HOOKS_STRING_UTILS_H_

#include <ctype.h>
#include <string.h>

#include "postgres.h"

// Copy src into a fixed-size name buffer.  Returns the number of bytes
// written (excluding the NUL), clamped to dst_size - 1.
static inline uint8 PschCopyName(char* dst, size_t dst_size, const char* src) {
  size_t len = strlcpy(dst, src, dst_size);
  return (uint8)(Min(len, dst_size - 1));
}

// Trim trailing whitespace in-place.  Returns the new length.
static inline size_t PschTrimTrailing(char* str, size_t len) {
  while (len > 0 && isspace((unsigned char)(str[len - 1]))) {
    len--;
  }
  str[len] = '\0';
  return len;
}

// Copy src to dst, skipping leading whitespace and trimming trailing.
// Returns the final length of the trimmed string in dst.
static inline size_t PschCopyTrimmed(char* dst, size_t dst_size, const char* src) {
  if (src == NULL || dst_size == 0) {
    if (dst_size > 0) {
      dst[0] = '\0';
    }
    return 0;
  }
  while (*src != '\0' && isspace((unsigned char)(*src))) {
    src++;
  }
  size_t src_len = strlcpy(dst, src, dst_size);
  size_t len = Min(src_len, dst_size - 1);
  return PschTrimTrailing(dst, len);
}

// Allocate a trimmed copy of a statement slice (leading + trailing whitespace
// stripped).  Returns a palloc'd string, or NULL if the input is empty.
static inline char* PschCopyTrimmedStatement(const char* query_text, int query_len) {
  if (query_text == NULL || query_len <= 0) {
    return NULL;
  }

  while (query_len > 0 && isspace((unsigned char)(*query_text))) {
    query_text++;
    query_len--;
  }

  while (query_len > 0 && isspace((unsigned char)(query_text[query_len - 1]))) {
    query_len--;
  }

  char* dst = (char*)(palloc(query_len + 1));
  if (query_len > 0) {
    memcpy(dst, query_text, query_len);
  }
  dst[query_len] = '\0';
  return dst;
}

#endif  // PG_STAT_CH_SRC_HOOKS_STRING_UTILS_H_
