// Recognized keys for the pg_stat_ch.extra_attributes GUC.
//
// Each key binds the GUC name (how operators configure it, e.g.
// `pg_stat_ch.extra_attributes = 'instance_ubid:abc;...'`) to both:
//   - its Arrow column name in the IPC payload, and
//   - the OTel Resource attribute name emitted by the OTel exporter.
//
// Adding a new identification field: add one `Key` constant below, then:
//   - wire up the Arrow column in arrow_batch.cc (schema + builder + Append)
//   - the OTel exporter picks it up automatically by iterating kAll.

#ifndef PG_STAT_CH_SRC_EXPORT_EXTRA_ATTR_KEYS_H_
#define PG_STAT_CH_SRC_EXPORT_EXTRA_ATTR_KEYS_H_

#include <array>
#include <string_view>

namespace psch::extra_attr_keys {

struct Key {
  // Key name as it appears in the pg_stat_ch.extra_attributes GUC and
  // the Arrow IPC column schema.
  std::string_view guc_key;
  // OTel Resource attribute name. Empty string = Arrow-only (not exposed
  // on the OTel Resource).
  std::string_view resource_attr;
};

inline constexpr Key kInstanceUbid{"instance_ubid", "ubi.postgres_resource_ubid"};
inline constexpr Key kServerUbid{"server_ubid", "ubi.postgres_server_ubid"};
inline constexpr Key kServerRole{"server_role", "ubi.postgres_server_role"};
inline constexpr Key kRegion{"region", "cloud.region"};
inline constexpr Key kCell{"cell", "cell"};
inline constexpr Key kHostId{"host_id", "host.id"};
inline constexpr Key kPodName{"pod_name", "k8s.pod.name"};

inline constexpr std::array<Key, 7> kAll = {
    kInstanceUbid, kServerUbid, kServerRole, kRegion, kCell, kHostId, kPodName,
};

}  // namespace psch::extra_attr_keys

#endif  // PG_STAT_CH_SRC_EXPORT_EXTRA_ATTR_KEYS_H_
