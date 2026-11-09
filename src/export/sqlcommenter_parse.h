#ifndef PG_STAT_CH_SRC_EXPORT_SQLCOMMENTER_PARSE_H_
#define PG_STAT_CH_SRC_EXPORT_SQLCOMMENTER_PARSE_H_

#include <cstddef>
#include <string>
#include <string_view>

// Return the sqlcommenter labels of a stored query text as a JSON object
// string, such as {"controller":"users"}, or "{}" when there are none.
//
// Only the /* ... */ comment that ends the text counts: only trailing
// whitespace may follow it, and no "--" may appear on its line before it or
// inside it. Any malformed pair (missing '=', quote or comma, or a trailing
// comma) yields no labels. A well-formed pair is dropped unless its
// URL-decoded key and value match [A-Za-z0-9_\-./:,]+ and fit 65 and 1024
// bytes. Text of max_stored_len bytes or more may have been cut, so it yields
// no labels.
std::string QueryLabelsJson(std::string_view query_text, size_t max_stored_len);

#endif  // PG_STAT_CH_SRC_EXPORT_SQLCOMMENTER_PARSE_H_
