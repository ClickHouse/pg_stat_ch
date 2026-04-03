// Statement-scoped normalized query registry.

#include <cstddef>
#include <cstring>

extern "C" {
#include "postgres.h"

#include "utils/memutils.h"
}

#include "hooks/query_normalize_state.h"

struct PschNormalizedQueryEntry {
  const char* source_text;
  char* source_text_copy;
  size_t source_text_len;
  int stmt_location;
  int stmt_len;
  char* normalized_query;
  int normalized_len;
  PschNormalizedQueryEntry* next;
};

static bool SourceTextMatch(const PschNormalizedQueryEntry* entry, const char* source_text) {
  if (entry == nullptr || source_text == nullptr) {
    return false;
  }

  if (entry->source_text == source_text) {
    return true;
  }

  size_t source_text_len = strlen(source_text);
  return entry->source_text_len == source_text_len &&
         memcmp(entry->source_text_copy, source_text, source_text_len) == 0;
}

static bool ExactStatementMatch(const PschNormalizedQueryEntry* entry, const char* source_text,
                                int stmt_location, int stmt_len) {
  return SourceTextMatch(entry, source_text) && entry->stmt_location == stmt_location &&
         entry->stmt_len == stmt_len;
}

static void RemoveEntry(PschNormalizedQueryState* state, PschNormalizedQueryEntry* prev,
                        PschNormalizedQueryEntry* entry) {
  if (prev != nullptr) {
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
  if (entry == nullptr || dst == nullptr || dst_size == 0 || out_len == nullptr) {
    return false;
  }

  size_t len = Min(static_cast<size_t>(entry->normalized_len), dst_size - 1);
  memcpy(dst, entry->normalized_query, len);
  dst[len] = '\0';
  *out_len = static_cast<uint16>(len);
  return true;
}

void PschRememberNormalizedQuery(PschNormalizedQueryState* state, const char* source_text,
                                 int stmt_location, int stmt_len, char* normalized_query,
                                 int normalized_len) {
  if (state == nullptr || source_text == nullptr || normalized_query == nullptr) {
    return;
  }

  for (PschNormalizedQueryEntry* entry = state->head; entry != nullptr; entry = entry->next) {
    if (!ExactStatementMatch(entry, source_text, stmt_location, stmt_len)) {
      continue;
    }

    entry->source_text = source_text;
    pfree(entry->normalized_query);
    entry->normalized_query = normalized_query;
    entry->normalized_len = normalized_len;
    return;
  }

  PschNormalizedQueryEntry* entry =
      static_cast<PschNormalizedQueryEntry*>(MemoryContextAlloc(TopMemoryContext, sizeof(*entry)));
  entry->source_text = source_text;
  entry->source_text_copy = MemoryContextStrdup(TopMemoryContext, source_text);
  entry->source_text_len = strlen(source_text);
  entry->stmt_location = stmt_location;
  entry->stmt_len = stmt_len;
  entry->normalized_query = normalized_query;
  entry->normalized_len = normalized_len;
  entry->next = state->head;
  state->head = entry;
}

bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len, const char* source_text,
                                         int stmt_location, int stmt_len, bool consume) {
  if (state == nullptr) {
    return false;
  }

  PschNormalizedQueryEntry* prev = nullptr;
  for (PschNormalizedQueryEntry* entry = state->head; entry != nullptr;
       prev = entry, entry = entry->next) {
    if (!ExactStatementMatch(entry, source_text, stmt_location, stmt_len)) {
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

void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state, const char* source_text,
                                           int stmt_location, int stmt_len) {
  if (state == nullptr || source_text == nullptr) {
    return;
  }

  PschNormalizedQueryEntry* prev = nullptr;
  PschNormalizedQueryEntry* entry = state->head;
  while (entry != nullptr) {
    PschNormalizedQueryEntry* next = entry->next;
    if (ExactStatementMatch(entry, source_text, stmt_location, stmt_len)) {
      RemoveEntry(state, prev, entry);
      return;
    } else {
      prev = entry;
    }
    entry = next;
  }
}
