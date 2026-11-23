// String utility helpers for pg_stat_ch hooks.
//
// Small functions for copying, trimming, and truncating strings that appear
// in exported events (query text, database/user names, error messages).

#ifndef PG_STAT_CH_SRC_HOOKS_STRING_UTILS_H_
#define PG_STAT_CH_SRC_HOOKS_STRING_UTILS_H_

#include <ctype.h>
#include <string.h>

#include "postgres.h"

#include "mb/pg_wchar.h"

// Copy at most dst_size bytes of src to dst, without terminator.  Returns bytes copied
static inline uint8 PschCopyName(char* dst, size_t dst_size, const char* src) {
  size_t len = strnlen(src, dst_size);
  memcpy(dst, src, len);
  return (uint8)len;
}

// Copy at most max_len bytes of src to dst, without terminator, skipping
// leading whitespace and trimming trailing, never splitting a multibyte
// character.  Returns bytes copied.
static inline size_t PschCopyTrimmed(char* dst, size_t max_len, const char* src) {
  if (src == NULL) {
    return 0;
  }
  while (isspace((unsigned char)(*src))) {
    src++;
  }
  size_t len = strnlen(src, max_len);
  if (len == max_len && src[len] != '\0') {
    len = (size_t)pg_mbcliplen(src, (int)len, (int)len);
  }
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
