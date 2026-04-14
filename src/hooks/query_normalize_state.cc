// Statement-scoped normalized query registry.
//
// Uses PostgreSQL's dynahash (HTAB) for O(1) lookup, insert, and delete.

#include <cstddef>
#include <cstring>
#include <string_view>

#include "absl/hash/hash.h"

extern "C" {
#include "postgres.h"

#include "utils/hsearch.h"
#include "utils/memutils.h"
}

#include "hooks/query_normalize_state.h"

// HTAB key: 16 bytes, compared bytewise via HASH_BLOBS.
// statement_hash already incorporates source_text content, stmt_location,
// and stmt_len via absl::HashOf, so including location/len in the key is
// redundant for uniqueness but makes accidental collisions essentially
// impossible (would require matching all three fields independently).
struct PschNormalizeHashKey {
  size_t statement_hash;
  int stmt_location;
  int stmt_len;
};

// Entry stored in HTAB.  The key must be the first member so that
// hash_search can cast between the key pointer and entry pointer.
struct PschNormalizeHashEntry {
  PschNormalizeHashKey key;
  char* normalized_query;
  int normalized_len;
};

struct PschStatementKeyHash {
  size_t operator()(const PschStatementKey& key) const {
    return absl::HashOf(
        std::string_view(key.source_text != nullptr ? key.source_text : "", key.source_text_len),
        key.stmt_location, key.stmt_len);
  }
};

PschStatementKey PschMakeStatementKey(const char* source_text, int stmt_location, int stmt_len) {
  PschStatementKey key = {source_text, source_text != nullptr ? strlen(source_text) : 0, 0,
                          stmt_location, stmt_len};
  key.statement_hash = (source_text != nullptr) ? PschStatementKeyHash{}(key) : 0;
  return key;
}

static PschNormalizeHashKey MakeHashKey(const PschStatementKey& key) {
  return {key.statement_hash, key.stmt_location, key.stmt_len};
}

// Create the HTAB lazily on first use.
static HTAB* EnsureHtab(PschNormalizedQueryState* state) {
  if (state->htab == nullptr) {
    HASHCTL info;
    MemSet(&info, 0, sizeof(info));
    info.keysize = sizeof(PschNormalizeHashKey);
    info.entrysize = sizeof(PschNormalizeHashEntry);
    info.hcxt = TopMemoryContext;
    state->htab = hash_create("pg_stat_ch normalized queries", 64, &info,
                              HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
  }
  return state->htab;
}

void PschRememberNormalizedQuery(PschNormalizedQueryState* state, const PschStatementKey& key,
                                 char* normalized_query, int normalized_len) {
  if (state == nullptr || key.source_text == nullptr || normalized_query == nullptr) {
    return;
  }

  HTAB* htab = EnsureHtab(state);
  PschNormalizeHashKey hkey = MakeHashKey(key);
  bool found;

  auto* entry = static_cast<PschNormalizeHashEntry*>(hash_search(htab, &hkey, HASH_ENTER, &found));
  if (found) {
    // Update existing entry — free the old normalized text.
    pfree(entry->normalized_query);
  }
  entry->normalized_query = normalized_query;
  entry->normalized_len = normalized_len;
}

bool PschCopyNormalizedQueryForStatement(PschNormalizedQueryState* state, char* dst,
                                         size_t dst_size, uint16* out_len,
                                         const PschStatementKey& key, bool consume) {
  if (state == nullptr || state->htab == nullptr || key.source_text == nullptr) {
    return false;
  }

  PschNormalizeHashKey hkey = MakeHashKey(key);
  auto* entry = static_cast<PschNormalizeHashEntry*>(
      hash_search(state->htab, &hkey, consume ? HASH_REMOVE : HASH_FIND, nullptr));

  if (entry == nullptr) {
    return false;
  }

  size_t len = Min(static_cast<size_t>(entry->normalized_len), dst_size - 1);
  memcpy(dst, entry->normalized_query, len);
  dst[len] = '\0';
  *out_len = static_cast<uint16>(len);

  if (consume) {
    pfree(entry->normalized_query);
  }
  return true;
}

void PschForgetNormalizedQueryForStatement(PschNormalizedQueryState* state,
                                           const PschStatementKey& key) {
  if (state == nullptr || state->htab == nullptr || key.source_text == nullptr) {
    return;
  }

  PschNormalizeHashKey hkey = MakeHashKey(key);
  auto* entry =
      static_cast<PschNormalizeHashEntry*>(hash_search(state->htab, &hkey, HASH_REMOVE, nullptr));

  if (entry != nullptr) {
    pfree(entry->normalized_query);
  }
}
