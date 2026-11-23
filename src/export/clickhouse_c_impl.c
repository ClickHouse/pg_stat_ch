#define CHC_IMPLEMENTATION
#define CHC_NO_ASYNC
// Inbound blocks are INSERT header, ProfileEvents & Log, bounded by ClickHouse event count
#define CHC_MAX_NUM_ROWS (1ULL << 20)
#include "clickhouse-client.h"
#include "clickhouse-compression.h"
#include "clickhouse-openssl.h"
#include "clickhouse-posix-io.h"
#include "clickhouse.h"
