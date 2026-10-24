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

// Copy at most max_len bytes of src to dst, without terminator, skipping
// leading whitespace and trimming trailing.  Returns bytes copied.
static inline size_t PschCopyTrimmed(char* dst, size_t max_len, const char* src) {
  if (src == NULL) {
    return 0;
  }
  while (isspace((unsigned char)(*src))) {
    src++;
  }
  size_t len = strnlen(src, max_len);
  while (len > 0 && isspace((unsigned char)(src[len - 1]))) {
    len--;
  }
  memcpy(dst, src, len);
  return len;
}

// Narrow a statement slice to exclude leading and trailing whitespace
static inline void PschTrimStatement(const char** query_text, int* query_len) {
  while (*query_len > 0 && isspace((unsigned char)(**query_text))) {
    (*query_text)++;
    (*query_len)--;
  }
  while (*query_len > 0 && isspace((unsigned char)((*query_text)[*query_len - 1]))) {
    (*query_len)--;
  }
}

#endif  // PG_STAT_CH_SRC_HOOKS_STRING_UTILS_H_
