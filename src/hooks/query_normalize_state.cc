// Statement-scoped normalized query registry.

#include <cstddef>
#include <cstring>
#include <string_view>

#include "absl/hash/hash.h"

extern "C" {
#include "postgres.h"

#include "utils/memutils.h"
}

#include "hooks/query_normalize_state.h"

struct PschStatementKeyHash {
  size_t operator()(const PschStatementKey& key) const {
    return absl::HashOf(
        std::string_view(key.source_text != nullptr ? key.source_text : "", key.source_text_len),
        key.stmt_location, key.stmt_len);
  }
};

struct PschNormalizedQueryEntry {
  PschStatementKey key;
  char* source_text_copy;
  char* normalized_query;
  int normalized_len;
  PschNormalizedQueryEntry* next;
};

PschStatementKey PschMakeStatementKey(const char* source_text, int stmt_location, int stmt_len) {
  PschStatementKey key = {source_text, source_text != nullptr ? strlen(source_text) : 0, 0,
                          stmt_location, stmt_len};
  key.statement_hash = (source_text != nullptr) ? PschStatementKeyHash{}(key) : 0;
  return key;
}

static bool ExactStatementMatch(const PschNormalizedQueryEntry* entry,
                                const PschStatementKey& key) {
  if (entry == nullptr || key.source_text == nullptr) {
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

void PschRememberNormalizedQuery(PschNormalizedQueryState* state, const PschStatementKey& key,
                                 char* normalized_query, int normalized_len) {
  if (state == nullptr || key.source_text == nullptr || normalized_query == nullptr) {
    return;
  }

  for (PschNormalizedQueryEntry* entry = state->head; entry != nullptr; entry = entry->next) {
    if (!ExactStatementMatch(entry, key)) {
      continue;
    }

    entry->key.source_text = key.source_text;
    pfree(entry->normalized_query);
    entry->normalized_query = normalized_query;
    entry->normalized_len = normalized_len;
    return;
  }

  PschNormalizedQueryEntry* entry =
      static_cast<PschNormalizedQueryEntry*>(MemoryContextAlloc(TopMemoryContext, sizeof(*entry)));
  entry->key = key;
  entry->source_text_copy = MemoryContextStrdup(TopMemoryContext, key.source_text);
  entry->normalized_query = normalized_query;
  entry->normalized_len = normalized_len;
  entry->next = state->head;
  state->head = entry;
}

bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len,
                                         const PschStatementKey& key, bool consume) {
  if (state == nullptr || key.source_text == nullptr) {
    return false;
  }

  PschNormalizedQueryEntry* prev = nullptr;
  for (PschNormalizedQueryEntry* entry = state->head; entry != nullptr;
       prev = entry, entry = entry->next) {
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

void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state,
                                           const PschStatementKey& key) {
  if (state == nullptr || key.source_text == nullptr) {
    return;
  }

  PschNormalizedQueryEntry* prev = nullptr;
  PschNormalizedQueryEntry* entry = state->head;
  while (entry != nullptr) {
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
