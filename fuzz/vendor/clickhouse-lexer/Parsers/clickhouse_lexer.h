// Copied, with only this header added, from https://github.com/ClickHouse/clickhouse-lexer, upstream/clickhouse_lexer.h,
// which mirrors src/Parsers/clickhouse_lexer.h of https://github.com/ClickHouse/ClickHouse at the
// commit named in ../COMMIT. Copyright ClickHouse, Inc., Apache License 2.0, see ../LICENSE.

#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

extern size_t clickhouse_lexer_size;
void clickhouse_lexer_create(void * ptr, const char * begin, const char * end, size_t max_query_size);
unsigned char clickhouse_lexer_next_token(void * ptr, const char ** out_token_begin, const char ** out_token_end);
int clickhouse_lexer_token_is_significant(unsigned char token);
int clickhouse_lexer_token_is_error(unsigned char token);
int clickhouse_lexer_token_is_end(unsigned char token);

#ifdef __cplusplus
}
#endif
