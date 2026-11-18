// Builds the relations blob reported per query: the tables/views/matviews
// the executor touched, deduplicated, with partition and inheritance
// children collapsed into their parent. Layout:
// [truncated:u8][count:u8]{[ns_len:u8][ns][name_len:u8][name][relkind:u8]}*count.
#ifndef PG_STAT_CH_SRC_HOOKS_RELATIONS_COLLECT_H_
#define PG_STAT_CH_SRC_HOOKS_RELATIONS_COLLECT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

void CollectRelations(StringInfo buf, List* rt, List* append_rel_infos);

#ifdef __cplusplus
}
#endif

#endif  // PG_STAT_CH_SRC_HOOKS_RELATIONS_COLLECT_H_
