#include "cache/cache.h"

#include <stdlib.h>
#include <string.h>

#include "utils/str_utils.h"

static void free_entry(flexql_cached_query* e) {
  size_t i;
  size_t j;
  if (e == NULL) {
    return;
  }
  free(e->sql);
  for (i = 0; i < e->column_count; ++i) {
    free(e->column_names[i]);
  }
  for (i = 0; i < e->row_count; ++i) {
    for (j = 0; j < e->column_count; ++j) {
      free(e->rows[i][j]);
    }
    free(e->rows[i]);
  }
  free(e->column_names);
  free(e->rows);
  memset(e, 0, sizeof(*e));
}

void cache_init(flexql_cache* cache, size_t cap) {
  memset(cache, 0, sizeof(*cache));
  cache->entries = (flexql_cached_query*)calloc(cap, sizeof(flexql_cached_query));
  if (flexql_mutex_init(&cache->mu) == 0) {
    cache->mu_ready = 1;
  }
  if (cache->entries != NULL) {
    cache->cap = cap;
  }
}

void cache_free(flexql_cache* cache) {
  size_t i;
  if (cache->mu_ready) {
    flexql_mutex_lock(&cache->mu);
  }
  for (i = 0; i < cache->cap; ++i) {
    free_entry(&cache->entries[i]);
  }
  free(cache->entries);
  if (cache->mu_ready) {
    flexql_mutex_unlock(&cache->mu);
    flexql_mutex_destroy(&cache->mu);
  }
  memset(cache, 0, sizeof(*cache));
}

void cache_invalidate_all(flexql_cache* cache) {
  size_t i;
  if (cache->mu_ready) {
    flexql_mutex_lock(&cache->mu);
  }
  for (i = 0; i < cache->cap; ++i) {
    free_entry(&cache->entries[i]);
  }
  cache->clock_tick = 0;
  if (cache->mu_ready) {
    flexql_mutex_unlock(&cache->mu);
  }
}

const flexql_cached_query* cache_get(flexql_cache* cache, const char* sql) {
  size_t i;
  const flexql_cached_query* hit = NULL;
  if (cache->mu_ready) {
    flexql_mutex_lock(&cache->mu);
  }
  for (i = 0; i < cache->cap; ++i) {
    if (cache->entries[i].used && strcmp(cache->entries[i].sql, sql) == 0) {
      cache->clock_tick += 1;
      cache->entries[i].tick = cache->clock_tick;
      hit = &cache->entries[i];
      break;
    }
  }
  if (cache->mu_ready) {
    flexql_mutex_unlock(&cache->mu);
  }
  return hit;
}

static int deep_copy_rows(char**** dst_rows, char*** src_rows, size_t row_count, size_t col_count) {
  size_t i;
  size_t j;
  char*** rows = (char***)calloc(row_count, sizeof(char**));
  if (rows == NULL && row_count != 0) {
    return 0;
  }
  for (i = 0; i < row_count; ++i) {
    rows[i] = (char**)calloc(col_count, sizeof(char*));
    if (rows[i] == NULL && col_count != 0) {
      return 0;
    }
    for (j = 0; j < col_count; ++j) {
      rows[i][j] = fx_strdup(src_rows[i][j]);
      if (rows[i][j] == NULL) {
        return 0;
      }
    }
  }
  *dst_rows = rows;
  return 1;
}

int cache_put(flexql_cache* cache, const char* sql, const char** cols, size_t col_count,
              char*** rows, size_t row_count) {
  size_t i;
  size_t victim = 0;
  unsigned long min_tick = (unsigned long)-1;
  flexql_cached_query* e;

  if (cache->cap == 0) {
    return 1;
  }

  if (cache->mu_ready) {
    flexql_mutex_lock(&cache->mu);
  }

  for (i = 0; i < cache->cap; ++i) {
    if (!cache->entries[i].used) {
      victim = i;
      min_tick = 0;
      break;
    }
    if (cache->entries[i].tick < min_tick) {
      min_tick = cache->entries[i].tick;
      victim = i;
    }
  }

  e = &cache->entries[victim];
  free_entry(e);

  e->sql = fx_strdup(sql);
  if (e->sql == NULL) {
    if (cache->mu_ready) {
      flexql_mutex_unlock(&cache->mu);
    }
    return 0;
  }

  e->column_names = (char**)calloc(col_count, sizeof(char*));
  if (e->column_names == NULL && col_count != 0) {
    if (cache->mu_ready) {
      flexql_mutex_unlock(&cache->mu);
    }
    return 0;
  }
  for (i = 0; i < col_count; ++i) {
    e->column_names[i] = fx_strdup(cols[i]);
    if (e->column_names[i] == NULL) {
      if (cache->mu_ready) {
        flexql_mutex_unlock(&cache->mu);
      }
      return 0;
    }
  }

  if (!deep_copy_rows(&e->rows, rows, row_count, col_count)) {
    if (cache->mu_ready) {
      flexql_mutex_unlock(&cache->mu);
    }
    return 0;
  }

  e->row_count = row_count;
  e->column_count = col_count;
  cache->clock_tick += 1;
  e->tick = cache->clock_tick;
  e->used = 1;
  if (cache->mu_ready) {
    flexql_mutex_unlock(&cache->mu);
  }
  return 1;
}
