// Statement-scoped normalized query registry.

#include <cstddef>

extern "C" {
#include "postgres.h"

#include "utils/memutils.h"
}

#include "hooks/query_normalize_state.h"

struct PschNormalizedQueryEntry {
  const char* source_text;
  int64 query_id;
  int stmt_location;
  int stmt_len;
  char* normalized_query;
  int normalized_len;
  PschNormalizedQueryEntry* next;
};

static bool ExactStatementMatch(const PschNormalizedQueryEntry* entry, const char* source_text,
                                int stmt_location, int stmt_len) {
  return entry->source_text == source_text && entry->stmt_location == stmt_location &&
         entry->stmt_len == stmt_len;
}

static bool CursorMatch(const PschNormalizedQueryEntry* entry, const char* source_text,
                        int cursorpos) {
  if (source_text == nullptr || cursorpos <= 0 || entry->source_text != source_text) {
    return false;
  }

  int stmt_start = (entry->stmt_location >= 0) ? entry->stmt_location : 0;
  int cursor_offset = cursorpos - 1;
  if (cursor_offset < stmt_start) {
    return false;
  }

  if (entry->stmt_len > 0 && cursor_offset >= stmt_start + entry->stmt_len) {
    return false;
  }

  return true;
}

static void RemoveEntry(PschNormalizedQueryState* state, PschNormalizedQueryEntry* prev,
                        PschNormalizedQueryEntry* entry) {
  if (prev != nullptr) {
    prev->next = entry->next;
  } else {
    state->head = entry->next;
  }

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
                                 int64 query_id, int stmt_location, int stmt_len,
                                 char* normalized_query, int normalized_len) {
  if (state == nullptr || source_text == nullptr || normalized_query == nullptr) {
    return;
  }

  for (PschNormalizedQueryEntry* entry = state->head; entry != nullptr; entry = entry->next) {
    if (!ExactStatementMatch(entry, source_text, stmt_location, stmt_len)) {
      continue;
    }

    pfree(entry->normalized_query);
    entry->query_id = query_id;
    entry->normalized_query = normalized_query;
    entry->normalized_len = normalized_len;
    return;
  }

  PschNormalizedQueryEntry* entry =
      static_cast<PschNormalizedQueryEntry*>(MemoryContextAlloc(TopMemoryContext, sizeof(*entry)));
  entry->source_text = source_text;
  entry->query_id = query_id;
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

static PschNormalizedQueryEntry* FindBestCursorMatch(PschNormalizedQueryState* state,
                                                     const char* source_text, int64 query_id,
                                                     int cursorpos) {
  PschNormalizedQueryEntry* best = nullptr;

  for (PschNormalizedQueryEntry* entry = state->head; entry != nullptr; entry = entry->next) {
    if (!CursorMatch(entry, source_text, cursorpos)) {
      continue;
    }

    if (query_id != INT64CONST(0) && entry->query_id != query_id) {
      continue;
    }

    if (best == nullptr || entry->stmt_location > best->stmt_location) {
      best = entry;
    }
  }

  return best;
}

static PschNormalizedQueryEntry* FindUniqueMatch(PschNormalizedQueryState* state,
                                                 const char* source_text, int64 query_id) {
  PschNormalizedQueryEntry* match = nullptr;

  for (PschNormalizedQueryEntry* entry = state->head; entry != nullptr; entry = entry->next) {
    if (entry->source_text != source_text) {
      continue;
    }

    if (query_id != INT64CONST(0) && entry->query_id != query_id) {
      continue;
    }

    if (match != nullptr) {
      return nullptr;
    }
    match = entry;
  }

  return match;
}

bool PschCopyNormalizedQueryForLog(PschNormalizedQueryState* state, char* dst, size_t dst_size,
                                   uint16* out_len, const char* source_text, int64 query_id,
                                   int cursorpos) {
  if (state == nullptr || source_text == nullptr) {
    return false;
  }

  PschNormalizedQueryEntry* entry = nullptr;
  if (cursorpos > 0) {
    entry = FindBestCursorMatch(state, source_text, query_id, cursorpos);
    if (entry == nullptr) {
      entry = FindBestCursorMatch(state, source_text, INT64CONST(0), cursorpos);
    }
  }

  if (entry == nullptr && query_id != INT64CONST(0)) {
    entry = FindUniqueMatch(state, source_text, query_id);
  }

  if (entry == nullptr) {
    entry = FindUniqueMatch(state, source_text, INT64CONST(0));
  }

  return CopyEntryText(entry, dst, dst_size, out_len);
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

void PschForgetNormalizedQueriesForSource(PschNormalizedQueryState* state,
                                          const char* source_text) {
  if (state == nullptr || source_text == nullptr) {
    return;
  }

  PschNormalizedQueryEntry* prev = nullptr;
  PschNormalizedQueryEntry* entry = state->head;
  while (entry != nullptr) {
    PschNormalizedQueryEntry* next = entry->next;
    if (entry->source_text == source_text) {
      RemoveEntry(state, prev, entry);
    } else {
      prev = entry;
    }
    entry = next;
  }
}
