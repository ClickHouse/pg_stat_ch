// Statement-scoped normalized query registry.

#include "postgres.h"

#include "common/hashfn.h"
#include "utils/memutils.h"

#include "hooks/query_normalize_state.h"

// Compute a hash for a statement key combining text, location, and length.
// Used as a fast prefilter before bytewise comparison.
static size_t ComputeStatementHash(const char* source_text, size_t source_text_len,
                                   int stmt_location, int stmt_len) {
  uint32 h = hash_bytes((const unsigned char*)source_text, (int)source_text_len);
  h = h * 31 + (uint32)stmt_location;
  h = h * 31 + (uint32)stmt_len;
  return (size_t)h;
}

struct PschNormalizedQueryEntry {
  PschStatementKey key;
  char* source_text_copy;
  char* normalized_query;
  int normalized_len;
  PschNormalizedQueryEntry* next;
};

PschStatementKey PschMakeStatementKey(const char* source_text, int stmt_location, int stmt_len) {
  PschStatementKey key;
  key.source_text = source_text;
  key.source_text_len = source_text != NULL ? strlen(source_text) : 0;
  key.stmt_location = stmt_location;
  key.stmt_len = stmt_len;
  key.statement_hash =
      (source_text != NULL)
          ? ComputeStatementHash(source_text, key.source_text_len, stmt_location, stmt_len)
          : 0;
  return key;
}

static bool ExactStatementMatch(const PschNormalizedQueryEntry* entry, PschStatementKey key) {
  if (entry == NULL || key.source_text == NULL) {
    return false;
  }

  if (entry->key.statement_hash != key.statement_hash ||
      entry->key.source_text_len != key.source_text_len ||
      entry->key.stmt_location != key.stmt_location || entry->key.stmt_len != key.stmt_len) {
    return false;
  }

  if (entry->key.source_text == key.source_text) {
    return true;
  }

  return memcmp(entry->source_text_copy, key.source_text, key.source_text_len) == 0;
}

static void RemoveEntry(PschNormalizedQueryState* state, PschNormalizedQueryEntry* prev,
                        PschNormalizedQueryEntry* entry) {
  if (prev != NULL) {
    prev->next = entry->next;
  } else {
    state->head = entry->next;
  }

  pfree(entry->source_text_copy);
  pfree(entry->normalized_query);
  pfree(entry);
}

static bool CopyEntryText(const PschNormalizedQueryEntry* entry, char* dst, size_t dst_size,
                          uint16* out_len) {
  size_t len;

  if (entry == NULL || dst == NULL || dst_size == 0 || out_len == NULL) {
    return false;
  }

  len = Min((size_t)entry->normalized_len, dst_size - 1);
  memcpy(dst, entry->normalized_query, len);
  dst[len] = '\0';
  *out_len = (uint16)len;
  return true;
}

void PschRememberNormalizedQuery(PschNormalizedQueryState* state, PschStatementKey key,
                                 char* normalized_query, int normalized_len) {
  PschNormalizedQueryEntry* entry;

  if (state == NULL || key.source_text == NULL || normalized_query == NULL) {
    return;
  }

  for (entry = state->head; entry != NULL; entry = entry->next) {
    if (!ExactStatementMatch(entry, key)) {
      continue;
    }

    entry->key.source_text = key.source_text;
    pfree(entry->normalized_query);
    entry->normalized_query = normalized_query;
    entry->normalized_len = normalized_len;
    return;
  }

  entry = (PschNormalizedQueryEntry*)MemoryContextAlloc(TopMemoryContext, sizeof(*entry));
  entry->key = key;
  entry->source_text_copy = MemoryContextStrdup(TopMemoryContext, key.source_text);
  entry->normalized_query = normalized_query;
  entry->normalized_len = normalized_len;
  entry->next = state->head;
  state->head = entry;
}

bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len,
                                         PschStatementKey key, bool consume) {
  PschNormalizedQueryEntry* prev = NULL;
  PschNormalizedQueryEntry* entry;

  if (state == NULL || key.source_text == NULL) {
    return false;
  }

  for (entry = state->head; entry != NULL; prev = entry, entry = entry->next) {
    if (!ExactStatementMatch(entry, key)) {
      continue;
    }

    bool copied = CopyEntryText(entry, dst, dst_size, out_len);
    if (copied && consume) {
      RemoveEntry(state, prev, entry);
    }
    return copied;
  }

  return false;
}

void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state, PschStatementKey key) {
  PschNormalizedQueryEntry* prev = NULL;
  PschNormalizedQueryEntry* entry;

  if (state == NULL || key.source_text == NULL) {
    return;
  }

  entry = state->head;
  while (entry != NULL) {
    PschNormalizedQueryEntry* next = entry->next;
    if (ExactStatementMatch(entry, key)) {
      RemoveEntry(state, prev, entry);
      return;
    } else {
      prev = entry;
    }
    entry = next;
  }
}
