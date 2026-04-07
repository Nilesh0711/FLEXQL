#ifndef FLEXQL_CACHE_H
#define FLEXQL_CACHE_H

#include <stddef.h>

#include "concurrency/lock.h"

typedef struct {
  char* sql;
  char** column_names;
  size_t column_count;
  char*** rows;
  size_t row_count;
  unsigned long tick;
  int used;
} flexql_cached_query;

typedef struct {
  flexql_cached_query* entries;
  size_t cap;
  unsigned long clock_tick;
  flexql_mutex mu;
  int mu_ready;
} flexql_cache;

void cache_init(flexql_cache* cache, size_t cap);
void cache_free(flexql_cache* cache);
void cache_invalidate_all(flexql_cache* cache);
const flexql_cached_query* cache_get(flexql_cache* cache, const char* sql);
int cache_put(flexql_cache* cache, const char* sql, const char** cols, size_t col_count,
              char*** rows, size_t row_count);

#endif
