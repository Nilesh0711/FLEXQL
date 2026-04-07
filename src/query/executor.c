#define _POSIX_C_SOURCE 200809L

#include "query/executor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "expiration/expiration.h"
#include "index/index.h"
#include "parser/parser.h"
#include "utils/str_utils.h"

typedef struct {
  const flexql_table* t1;
  const flexql_row* r1;
  const flexql_table* t2;
  const flexql_row* r2;
} eval_ctx;

typedef struct {
  int source_table;
  int col_idx;
} output_ref;

typedef struct {
  int source_table;
  int col_idx;
} join_ref;

static void set_err(char** err, const char* msg) {
  if (err != NULL) {
    free(*err);
    *err = fx_strdup(msg);
  }
}

static int compare_values(const char* a, const char* b, const char* op) {
  long double an;
  long double bn;
  int anum = fx_parse_number(a, &an);
  int bnum = fx_parse_number(b, &bn);

  if (anum && bnum) {
    if (strcmp(op, "=") == 0) return an == bn;
    if (strcmp(op, "!=") == 0) return an != bn;
    if (strcmp(op, ">") == 0) return an > bn;
    if (strcmp(op, "<") == 0) return an < bn;
    if (strcmp(op, ">=") == 0) return an >= bn;
    if (strcmp(op, "<=") == 0) return an <= bn;
    return 0;
  }

  if (strcmp(op, "=") == 0) return strcmp(a, b) == 0;
  if (strcmp(op, "!=") == 0) return strcmp(a, b) != 0;
  if (strcmp(op, ">") == 0) return strcmp(a, b) > 0;
  if (strcmp(op, "<") == 0) return strcmp(a, b) < 0;
  if (strcmp(op, ">=") == 0) return strcmp(a, b) >= 0;
  if (strcmp(op, "<=") == 0) return strcmp(a, b) <= 0;
  return 0;
}

static int split_ref(const char* ref, char** out_table, char** out_col) {
  const char* dot = strchr(ref, '.');
  if (dot == NULL) {
    *out_table = NULL;
    *out_col = fx_strdup(ref);
    return *out_col != NULL;
  }
  *out_table = fx_strndup(ref, (size_t)(dot - ref));
  *out_col = fx_strdup(dot + 1);
  if (*out_table == NULL || *out_col == NULL) {
    free(*out_table);
    free(*out_col);
    *out_table = NULL;
    *out_col = NULL;
    return 0;
  }
  return 1;
}

static int resolve_ref_schema(const flexql_table* t1, const flexql_table* t2, const char* ref,
                              int* ok) {
  char* t = NULL;
  char* c = NULL;
  int idx = -1;

  *ok = 0;
  if (!split_ref(ref, &t, &c)) {
    return -1;
  }

  if (t == NULL) {
    idx = table_find_column(t1, c);
    if (idx >= 0) {
      *ok = 1;
      free(c);
      return idx;
    }
    if (t2 != NULL) {
      idx = table_find_column(t2, c);
      if (idx >= 0) {
        *ok = 1;
        free(c);
        return idx;
      }
    }
  } else {
    if (fx_ieq(t, t1->name)) {
      idx = table_find_column(t1, c);
      if (idx >= 0) {
        *ok = 1;
      }
    } else if (t2 != NULL && fx_ieq(t, t2->name)) {
      idx = table_find_column(t2, c);
      if (idx >= 0) {
        *ok = 1;
      }
    }
  }

  free(t);
  free(c);
  return idx;
}

static int resolve_join_ref(const flexql_table* t1, const flexql_table* t2, const char* ref,
                            join_ref* out, int* ok) {
  char* t = NULL;
  char* c = NULL;
  int idx = -1;

  *ok = 0;
  if (!split_ref(ref, &t, &c)) {
    return 0;
  }

  if (t == NULL) {
    idx = table_find_column(t1, c);
    if (idx >= 0) {
      out->source_table = 1;
      out->col_idx = idx;
      *ok = 1;
      free(c);
      return 1;
    }
    if (t2 != NULL) {
      idx = table_find_column(t2, c);
      if (idx >= 0) {
        out->source_table = 2;
        out->col_idx = idx;
        *ok = 1;
        free(c);
        return 1;
      }
    }
  } else if (fx_ieq(t, t1->name)) {
    idx = table_find_column(t1, c);
    if (idx >= 0) {
      out->source_table = 1;
      out->col_idx = idx;
      *ok = 1;
    }
  } else if (t2 != NULL && fx_ieq(t, t2->name)) {
    idx = table_find_column(t2, c);
    if (idx >= 0) {
      out->source_table = 2;
      out->col_idx = idx;
      *ok = 1;
    }
  }

  free(t);
  free(c);
  return *ok;
}

static const char* resolve_ref_value(const eval_ctx* ctx, const char* ref, int* ok) {
  char* t = NULL;
  char* c = NULL;
  int idx;
  *ok = 0;

  if (!split_ref(ref, &t, &c)) {
    return NULL;
  }

  if (t == NULL) {
    idx = table_find_column(ctx->t1, c);
    if (idx >= 0) {
      *ok = 1;
      free(c);
      return ctx->r1->values[idx];
    }
    if (ctx->t2 != NULL) {
      idx = table_find_column(ctx->t2, c);
      if (idx >= 0) {
        *ok = 1;
        free(c);
        return ctx->r2->values[idx];
      }
    }
    free(c);
    return NULL;
  }

  if (fx_ieq(t, ctx->t1->name)) {
    idx = table_find_column(ctx->t1, c);
    free(t);
    free(c);
    if (idx >= 0) {
      *ok = 1;
      return ctx->r1->values[idx];
    }
    return NULL;
  }

  if (ctx->t2 != NULL && fx_ieq(t, ctx->t2->name)) {
    idx = table_find_column(ctx->t2, c);
    free(t);
    free(c);
    if (idx >= 0) {
      *ok = 1;
      return ctx->r2->values[idx];
    }
    return NULL;
  }

  free(t);
  free(c);
  return NULL;
}

static int eval_condition(const eval_ctx* ctx, const flexql_condition* cond, char** err) {
  int ok_left = 0;
  int ok_right = 0;
  const char* lv = resolve_ref_value(ctx, cond->left, &ok_left);
  const char* rv;

  if (!ok_left) {
    set_err(err, "unknown column");
    return 0;
  }

  if (cond->right_is_literal) {
    rv = cond->right;
  } else {
    rv = resolve_ref_value(ctx, cond->right, &ok_right);
    if (!ok_right) {
      set_err(err, "unknown column");
      return 0;
    }
  }

  return compare_values(lv, rv, cond->op);
}

static int rows_append(char**** rows, size_t* row_count, size_t* row_cap, char** row) {
  char*** nrows;
  if (*row_count == *row_cap) {
    size_t ncap = *row_cap == 0 ? 32 : *row_cap * 2;
    nrows = (char***)realloc(*rows, ncap * sizeof(char**));
    if (nrows == NULL) {
      return 0;
    }
    *rows = nrows;
    *row_cap = ncap;
  }
  (*rows)[*row_count] = row;
  *row_count += 1;
  return 1;
}

static void rows_free(char*** rows, size_t row_count, size_t col_count) {
  size_t i;
  size_t j;
  for (i = 0; i < row_count; ++i) {
    for (j = 0; j < col_count; ++j) {
      free(rows[i][j]);
    }
    free(rows[i]);
  }
  free(rows);
}

static void merge_sorted_rows(char*** rows, char**** scratch, size_t left, size_t mid, size_t right,
                              size_t order_idx) {
  size_t i = left;
  size_t j = mid;
  size_t k = left;

  while (i < mid && j < right) {
    if (compare_values(rows[i][order_idx], rows[j][order_idx], "<=")) {
      (*scratch)[k++] = rows[i++];
    } else {
      (*scratch)[k++] = rows[j++];
    }
  }
  while (i < mid) {
    (*scratch)[k++] = rows[i++];
  }
  while (j < right) {
    (*scratch)[k++] = rows[j++];
  }
  for (k = left; k < right; ++k) {
    rows[k] = (*scratch)[k];
  }
}

static int sort_rows_by_column(char*** rows, size_t row_count, size_t order_idx, char** err) {
  size_t width;
  char*** scratch;

  if (row_count < 2) {
    return 1;
  }

  scratch = (char***)calloc(row_count, sizeof(char**));
  if (scratch == NULL) {
    set_err(err, "out of memory");
    return 0;
  }

  for (width = 1; width < row_count; width *= 2) {
    size_t left;
    for (left = 0; left < row_count; left += width * 2) {
      size_t mid = left + width;
      size_t right = left + width * 2;
      if (mid > row_count) {
        mid = row_count;
      }
      if (right > row_count) {
        right = row_count;
      }
      if (mid < right) {
        merge_sorted_rows(rows, &scratch, left, mid, right, order_idx);
      }
    }
  }

  free(scratch);
  return 1;
}

static int resolve_output_columns(const flexql_stmt_select* q, const flexql_table* t1,
                                  const flexql_table* t2, char*** out_cols,
                                  output_ref** out_refs, size_t* out_count, char** err) {
  size_t i;
  size_t count = 0;
  char** cols = NULL;
  output_ref* refs = NULL;

  if (q->select_all) {
    cols = (char**)calloc(t1->column_count + (t2 ? t2->column_count : 0), sizeof(char*));
    refs = (output_ref*)calloc(t1->column_count + (t2 ? t2->column_count : 0),
                               sizeof(output_ref));
    if (cols == NULL || refs == NULL) {
      free(cols);
      free(refs);
      set_err(err, "out of memory");
      return 0;
    }
    for (i = 0; i < t1->column_count; ++i) {
      cols[count++] = fx_strdup(t1->columns[i].name);
      if (cols[count - 1] == NULL) {
        set_err(err, "out of memory");
        goto fail;
      }
      refs[count - 1].source_table = 1;
      refs[count - 1].col_idx = (int)i;
    }
    if (t2 != NULL) {
      for (i = 0; i < t2->column_count; ++i) {
        cols[count++] = fx_strdup(t2->columns[i].name);
        if (cols[count - 1] == NULL) {
          set_err(err, "out of memory");
          goto fail;
        }
        refs[count - 1].source_table = 2;
        refs[count - 1].col_idx = (int)i;
      }
    }
  } else {
    cols = (char**)calloc(q->select_count, sizeof(char*));
    refs = (output_ref*)calloc(q->select_count, sizeof(output_ref));
    if ((cols == NULL || refs == NULL) && q->select_count != 0) {
      free(cols);
      free(refs);
      set_err(err, "out of memory");
      return 0;
    }
    for (i = 0; i < q->select_count; ++i) {
      int ok = 0;
      int idx = resolve_ref_schema(t1, t2, q->select_items[i], &ok);
      char* ref_table = NULL;
      char* ref_col = NULL;
      if (!ok || idx < 0) {
        set_err(err, "unknown column");
        goto fail;
      }
      cols[count++] = fx_strdup(q->select_items[i]);
      if (cols[count - 1] == NULL || !split_ref(q->select_items[i], &ref_table, &ref_col)) {
        free(ref_table);
        free(ref_col);
        set_err(err, "out of memory");
        goto fail;
      }
      if (ref_table == NULL) {
        if (table_find_column(t1, ref_col) == idx) {
          refs[count - 1].source_table = 1;
        } else {
          refs[count - 1].source_table = 2;
        }
      } else if (fx_ieq(ref_table, t1->name)) {
        refs[count - 1].source_table = 1;
      } else {
        refs[count - 1].source_table = 2;
      }
      refs[count - 1].col_idx = idx;
      free(ref_table);
      free(ref_col);
    }
  }

  *out_cols = cols;
  *out_refs = refs;
  *out_count = count;
  return 1;

fail:
  if (cols != NULL) {
    for (i = 0; i < count; ++i) {
      free(cols[i]);
    }
  }
  free(cols);
  free(refs);
  return 0;
}

static int emit_row_result(const eval_ctx* ctx, const output_ref* refs, size_t col_count,
                           int stream_results, char**** rows, size_t* row_count,
                           size_t* row_cap, flexql_callback callback, void* arg, char** cols,
                           int* stop, char** err) {
  size_t j;
  char** out_row = NULL;

  if (stream_results) {
    if (callback == NULL) {
      return 1;
    }
    out_row = (char**)calloc(col_count, sizeof(char*));
    if (out_row == NULL && col_count != 0) {
      set_err(err, "out of memory");
      return 0;
    }
    for (j = 0; j < col_count; ++j) {
      out_row[j] = (refs[j].source_table == 1) ? ctx->r1->values[refs[j].col_idx]
                                               : ctx->r2->values[refs[j].col_idx];
    }
    if (callback(arg, (int)col_count, out_row, cols) != 0) {
      *stop = 1;
    }
    free(out_row);
    return 1;
  }

  out_row = (char**)calloc(col_count, sizeof(char*));
  if (out_row == NULL && col_count != 0) {
    set_err(err, "out of memory");
    return 0;
  }
  for (j = 0; j < col_count; ++j) {
    const char* v = (refs[j].source_table == 1) ? ctx->r1->values[refs[j].col_idx]
                                                : ctx->r2->values[refs[j].col_idx];
    out_row[j] = fx_strdup(v);
    if (out_row[j] == NULL) {
      set_err(err, "out of memory");
      free(out_row);
      return 0;
    }
  }
  if (!rows_append(rows, row_count, row_cap, out_row)) {
    set_err(err, "out of memory");
    free(out_row);
    return 0;
  }
  return 1;
}

static int execute_indexed_join(const flexql_stmt_select* q, const flexql_table* t1,
                                const flexql_table* t2, const join_ref* indexed_ref,
                                const join_ref* scan_ref, const output_ref* refs,
                                size_t col_count, int stream_results, char**** rows,
                                size_t* row_count, size_t* row_cap, flexql_callback callback,
                                void* arg, char** cols, int* stop, char** err) {
  const flexql_table* indexed_table = indexed_ref->source_table == 1 ? t1 : t2;
  const flexql_table* scan_table = scan_ref->source_table == 1 ? t1 : t2;
  flexql_table_scan scan;
  long long now = expiration_now_epoch();

  if (!storage_scan_open(scan_table, &scan, err)) {
    return 0;
  }

  while (!*stop) {
    flexql_row scan_row;
    int scan_rc = storage_scan_next(&scan, &scan_row, err);
    if (scan_rc < 0) {
      storage_scan_close(&scan);
      return 0;
    }
    if (scan_rc == 0) {
      break;
    }
    if (expiration_is_expired(scan_table, &scan_row, now)) {
      storage_row_free(&scan_row, scan_table->column_count);
      continue;
    }

    {
      flexql_row indexed_row;
      int found = 0;
      eval_ctx ctx;
      memset(&indexed_row, 0, sizeof(indexed_row));

      if (!storage_lookup_primary(indexed_table, scan_row.values[scan_ref->col_idx], &indexed_row,
                                  &found, err)) {
        storage_row_free(&scan_row, scan_table->column_count);
        storage_scan_close(&scan);
        return 0;
      }
      if (!found) {
        storage_row_free(&scan_row, scan_table->column_count);
        continue;
      }
      if (expiration_is_expired(indexed_table, &indexed_row, now)) {
        storage_row_free(&indexed_row, indexed_table->column_count);
        storage_row_free(&scan_row, scan_table->column_count);
        continue;
      }

      if (indexed_ref->source_table == 1) {
        ctx.t1 = t1;
        ctx.r1 = &indexed_row;
        ctx.t2 = t2;
        ctx.r2 = &scan_row;
      } else {
        ctx.t1 = t1;
        ctx.r1 = &scan_row;
        ctx.t2 = t2;
        ctx.r2 = &indexed_row;
      }

      if (!eval_condition(&ctx, &q->join_cond, err)) {
        if (err != NULL && *err != NULL) {
          storage_row_free(&indexed_row, indexed_table->column_count);
          storage_row_free(&scan_row, scan_table->column_count);
          storage_scan_close(&scan);
          return 0;
        }
        storage_row_free(&indexed_row, indexed_table->column_count);
        storage_row_free(&scan_row, scan_table->column_count);
        continue;
      }

      if (q->has_where && !eval_condition(&ctx, &q->where_cond, err)) {
        if (err != NULL && *err != NULL) {
          storage_row_free(&indexed_row, indexed_table->column_count);
          storage_row_free(&scan_row, scan_table->column_count);
          storage_scan_close(&scan);
          return 0;
        }
        storage_row_free(&indexed_row, indexed_table->column_count);
        storage_row_free(&scan_row, scan_table->column_count);
        continue;
      }

      if (!emit_row_result(&ctx, refs, col_count, stream_results, rows, row_count, row_cap,
                           callback, arg, cols, stop, err)) {
        storage_row_free(&indexed_row, indexed_table->column_count);
        storage_row_free(&scan_row, scan_table->column_count);
        storage_scan_close(&scan);
        return 0;
      }

      storage_row_free(&indexed_row, indexed_table->column_count);
      storage_row_free(&scan_row, scan_table->column_count);
    }
  }

  storage_scan_close(&scan);
  return 1;
}

static int wal_sync_file(FILE* fp) {
  if (fflush(fp) != 0) {
    return 0;
  }
  return 1;
}

static int wal_append_sql(const flexql_storage* storage, const char* sql) {
  FILE* fp;
  unsigned int len = (unsigned int)strlen(sql);
  if (storage->wal_path == NULL) {
    return 1;
  }
  fp = fopen(storage->wal_path, "ab");
  if (fp == NULL) {
    return 0;
  }
  if (fwrite(&len, sizeof(len), 1, fp) != 1 ||
      (len > 0 && fwrite(sql, 1, len, fp) != len) ||
      !wal_sync_file(fp)) {
    fclose(fp);
    return 0;
  }
  fclose(fp);
  return 1;
}

static int wal_clear(const flexql_storage* storage) {
  FILE* fp;
  if (storage->wal_path == NULL) {
    return 1;
  }
  fp = fopen(storage->wal_path, "wb");
  if (fp == NULL) {
    return 0;
  }
  if (!wal_sync_file(fp)) {
    fclose(fp);
    return 0;
  }
  fclose(fp);
  return 1;
}

static int execute_select(flexql_storage* storage, flexql_cache* cache, const char* sql,
                          const flexql_stmt_select* q, flexql_callback callback, void* arg,
                          char** err) {
  size_t i;
  size_t row_count = 0;
  size_t row_cap = 0;
  char*** rows = NULL;
  char** cols = NULL;
  output_ref* refs = NULL;
  size_t col_count = 0;
  const flexql_table* t1 = storage_find_table_const(storage, q->from_table);
  const flexql_table* t2 = NULL;
  int use_cache = 1;
  int stream_results = 0;
  int stop = 0;

  if (t1 == NULL) {
    set_err(err, "table not found");
    return 0;
  }
  if (q->has_join) {
    t2 = storage_find_table_const(storage, q->join_table);
    if (t2 == NULL) {
      set_err(err, "table not found");
      return 0;
    }
  }

  if (t1->expires_col_idx >= 0 || (t2 != NULL && t2->expires_col_idx >= 0)) {
    use_cache = 0;
  }

  if (use_cache) {
    const flexql_cached_query* hit = cache_get(cache, sql);
    if (hit != NULL) {
      if (callback != NULL) {
        for (i = 0; i < hit->row_count; ++i) {
          if (callback(arg, (int)hit->column_count, hit->rows[i], hit->column_names) != 0) {
            break;
          }
        }
      }
      return 1;
    }
  }

  if (!resolve_output_columns(q, t1, t2, &cols, &refs, &col_count, err)) {
    return 0;
  }

  stream_results = !q->has_order_by && !use_cache;

  if (q->has_order_by) {
    int found = 0;
    for (i = 0; i < col_count; ++i) {
      if (fx_ieq(cols[i], q->order_by)) {
        found = 1;
        break;
      }
    }
    if (!found) {
      set_err(err, "ORDER BY column must appear in SELECT list");
      goto fail;
    }
  }

  if (q->has_where) {
    int ok = 0;
    resolve_ref_schema(t1, t2, q->where_cond.left, &ok);
    if (!ok) {
      set_err(err, "unknown column");
      goto fail;
    }
    if (!q->where_cond.right_is_literal) {
      resolve_ref_schema(t1, t2, q->where_cond.right, &ok);
      if (!ok) {
        set_err(err, "unknown column");
        goto fail;
      }
    }
  }
  if (q->has_join) {
    int ok = 0;
    resolve_ref_schema(t1, t2, q->join_cond.left, &ok);
    if (!ok) {
      set_err(err, "unknown column");
      goto fail;
    }
    resolve_ref_schema(t1, t2, q->join_cond.right, &ok);
    if (!ok) {
      set_err(err, "unknown column");
      goto fail;
    }
  }

  if (!q->has_join) {
    long long now = expiration_now_epoch();
    int used_primary_lookup = 0;

    if (q->has_where && strcmp(q->where_cond.op, "=") == 0 && q->where_cond.right_is_literal) {
      int ok = 0;
      int idx = resolve_ref_schema(t1, NULL, q->where_cond.left, &ok);
      if (ok && idx == 0) {
        flexql_row row;
        int found = 0;
        memset(&row, 0, sizeof(row));
        if (!storage_lookup_primary(t1, q->where_cond.right, &row, &found, err)) {
          goto fail;
        }
        used_primary_lookup = 1;
        if (found) {
          eval_ctx ctx;
          ctx.t1 = t1;
          ctx.r1 = &row;
          ctx.t2 = NULL;
          ctx.r2 = NULL;
          if (!expiration_is_expired(t1, &row, now) &&
              (!q->has_where || eval_condition(&ctx, &q->where_cond, err))) {
            if (err != NULL && *err != NULL) {
              storage_row_free(&row, t1->column_count);
              goto fail;
            }
            if (!emit_row_result(&ctx, refs, col_count, stream_results, &rows, &row_count,
                                 &row_cap, callback, arg, cols, &stop, err)) {
              storage_row_free(&row, t1->column_count);
              goto fail;
            }
          }
          storage_row_free(&row, t1->column_count);
        }
      }
    }

    if (!used_primary_lookup) {
      flexql_table_scan scan;
      flexql_row row;
      if (!storage_scan_open(t1, &scan, err)) {
        goto fail;
      }
      while (!stop) {
        int scan_rc = storage_scan_next(&scan, &row, err);
        eval_ctx ctx;
        if (scan_rc < 0) {
          storage_scan_close(&scan);
          goto fail;
        }
        if (scan_rc == 0) {
          break;
        }
        if (expiration_is_expired(t1, &row, now)) {
          storage_row_free(&row, t1->column_count);
          continue;
        }
        ctx.t1 = t1;
        ctx.r1 = &row;
        ctx.t2 = NULL;
        ctx.r2 = NULL;
        if (q->has_where && !eval_condition(&ctx, &q->where_cond, err)) {
          if (err != NULL && *err != NULL) {
            storage_row_free(&row, t1->column_count);
            storage_scan_close(&scan);
            goto fail;
          }
          storage_row_free(&row, t1->column_count);
          continue;
        }
        if (!emit_row_result(&ctx, refs, col_count, stream_results, &rows, &row_count, &row_cap,
                             callback, arg, cols, &stop, err)) {
          storage_row_free(&row, t1->column_count);
          storage_scan_close(&scan);
          goto fail;
        }
        storage_row_free(&row, t1->column_count);
      }
      storage_scan_close(&scan);
    }
  } else {
    join_ref left_ref;
    join_ref right_ref;
    int ok_left = 0;
    int ok_right = 0;
    int used_indexed_join = 0;

    if (strcmp(q->join_cond.op, "=") == 0 && !q->join_cond.right_is_literal &&
        resolve_join_ref(t1, t2, q->join_cond.left, &left_ref, &ok_left) &&
        resolve_join_ref(t1, t2, q->join_cond.right, &right_ref, &ok_right) && ok_left &&
        ok_right && left_ref.source_table != right_ref.source_table) {
      if (left_ref.col_idx == 0) {
        used_indexed_join = 1;
        if (!execute_indexed_join(q, t1, t2, &left_ref, &right_ref, refs, col_count,
                                  stream_results, &rows, &row_count, &row_cap, callback, arg,
                                  cols, &stop, err)) {
          goto fail;
        }
      } else if (right_ref.col_idx == 0) {
        used_indexed_join = 1;
        if (!execute_indexed_join(q, t1, t2, &right_ref, &left_ref, refs, col_count,
                                  stream_results, &rows, &row_count, &row_cap, callback, arg,
                                  cols, &stop, err)) {
          goto fail;
        }
      }
    }

    if (!used_indexed_join) {
      long long now = expiration_now_epoch();
      flexql_table_scan outer_scan;
      if (!storage_scan_open(t1, &outer_scan, err)) {
        goto fail;
      }
      while (!stop) {
        flexql_row row1;
        int outer_rc = storage_scan_next(&outer_scan, &row1, err);
        if (outer_rc < 0) {
          storage_scan_close(&outer_scan);
          goto fail;
        }
        if (outer_rc == 0) {
          break;
        }
        if (!expiration_is_expired(t1, &row1, now)) {
          flexql_table_scan inner_scan;
          if (!storage_scan_open(t2, &inner_scan, err)) {
            storage_row_free(&row1, t1->column_count);
            storage_scan_close(&outer_scan);
            goto fail;
          }
          while (!stop) {
            flexql_row row2;
            int inner_rc = storage_scan_next(&inner_scan, &row2, err);
            eval_ctx ctx;
            if (inner_rc < 0) {
              storage_scan_close(&inner_scan);
              storage_row_free(&row1, t1->column_count);
              storage_scan_close(&outer_scan);
              goto fail;
            }
            if (inner_rc == 0) {
              break;
            }
            if (expiration_is_expired(t2, &row2, now)) {
              storage_row_free(&row2, t2->column_count);
              continue;
            }
            ctx.t1 = t1;
            ctx.r1 = &row1;
            ctx.t2 = t2;
            ctx.r2 = &row2;
            if (!eval_condition(&ctx, &q->join_cond, err)) {
              if (err != NULL && *err != NULL) {
                storage_row_free(&row2, t2->column_count);
                storage_scan_close(&inner_scan);
                storage_row_free(&row1, t1->column_count);
                storage_scan_close(&outer_scan);
                goto fail;
              }
              storage_row_free(&row2, t2->column_count);
              continue;
            }
            if (q->has_where && !eval_condition(&ctx, &q->where_cond, err)) {
              if (err != NULL && *err != NULL) {
                storage_row_free(&row2, t2->column_count);
                storage_scan_close(&inner_scan);
                storage_row_free(&row1, t1->column_count);
                storage_scan_close(&outer_scan);
                goto fail;
              }
              storage_row_free(&row2, t2->column_count);
              continue;
            }
            if (!emit_row_result(&ctx, refs, col_count, stream_results, &rows, &row_count,
                                 &row_cap, callback, arg, cols, &stop, err)) {
              storage_row_free(&row2, t2->column_count);
              storage_scan_close(&inner_scan);
              storage_row_free(&row1, t1->column_count);
              storage_scan_close(&outer_scan);
              goto fail;
            }
            storage_row_free(&row2, t2->column_count);
          }
          storage_scan_close(&inner_scan);
        }
        storage_row_free(&row1, t1->column_count);
      }
      storage_scan_close(&outer_scan);
    }
  }

  if (q->has_order_by) {
    size_t order_idx = 0;
    for (i = 0; i < col_count; ++i) {
      if (fx_ieq(cols[i], q->order_by)) {
        order_idx = i;
        break;
      }
    }

    if (!sort_rows_by_column(rows, row_count, order_idx, err)) {
      goto fail;
    }
  }

  if (!stream_results && callback != NULL) {
    for (i = 0; i < row_count; ++i) {
      if (callback(arg, (int)col_count, rows[i], cols) != 0) {
        break;
      }
    }
  }

  if (use_cache) {
    cache_put(cache, sql, (const char**)cols, col_count, rows, row_count);
  }

  rows_free(rows, row_count, col_count);
  for (i = 0; i < col_count; ++i) free(cols[i]);
  free(cols);
  free(refs);
  return 1;

fail:
  if (rows != NULL) rows_free(rows, row_count, col_count);
  if (cols != NULL) {
    for (i = 0; i < col_count; ++i) free(cols[i]);
    free(cols);
  }
  free(refs);
  return 0;
}

static int executor_exec_sql_mode(flexql_storage* storage, flexql_cache* cache, const char* sql,
                                  flexql_callback callback, void* arg, char** err,
                                  int use_wal) {
  flexql_statement st;
  int ok = 1;
  int needs_wal = 0;

  if (!parser_parse_sql(sql, &st, err)) {
    return 0;
  }

  needs_wal = (st.type == FLEXQL_STMT_CREATE || st.type == FLEXQL_STMT_INSERT ||
               st.type == FLEXQL_STMT_DELETE || st.type == FLEXQL_STMT_RESET);
  if (use_wal && needs_wal && !wal_append_sql(storage, sql)) {
    parser_free_statement(&st);
    set_err(err, "failed to write WAL");
    return 0;
  }

  switch (st.type) {
    case FLEXQL_STMT_NONE:
      parser_free_statement(&st);
      return 1;
    case FLEXQL_STMT_CREATE:
      if (!storage_create_table(storage, st.as.create_stmt.table_name,
                                st.as.create_stmt.if_not_exists,
                                (const char**)st.as.create_stmt.col_names,
                                (const char**)st.as.create_stmt.col_types,
                                st.as.create_stmt.col_count, err)) {
        ok = 0;
        break;
      }
      cache_invalidate_all(cache);
      break;
    case FLEXQL_STMT_INSERT:
      if (!storage_insert_rows(storage, st.as.insert_stmt.table_name,
                               (const char***)st.as.insert_stmt.rows,
                               st.as.insert_stmt.row_count, st.as.insert_stmt.value_count,
                               err)) {
        ok = 0;
        break;
      }
      cache_invalidate_all(cache);
      break;
    case FLEXQL_STMT_DELETE:
      if (!storage_clear_table_rows(storage, st.as.delete_stmt.table_name, err)) {
        ok = 0;
        break;
      }
      cache_invalidate_all(cache);
      break;
    case FLEXQL_STMT_RESET:
      if (!storage_clear_table_rows(storage, st.as.reset_stmt.table_name, err)) {
        ok = 0;
        break;
      }
      cache_invalidate_all(cache);
      break;
    case FLEXQL_STMT_SELECT:
      if (!execute_select(storage, cache, sql, &st.as.select_stmt, callback, arg, err)) {
        ok = 0;
        break;
      }
      break;
    default:
      set_err(err, "unsupported SQL command");
      ok = 0;
      break;
  }

  parser_free_statement(&st);
  if (use_wal && needs_wal && !wal_clear(storage) && ok) {
    set_err(err, "failed to checkpoint WAL");
    return 0;
  }
  return ok;
}

int executor_exec_sql(flexql_storage* storage, flexql_cache* cache, const char* sql,
                      flexql_callback callback, void* arg, char** err) {
  return executor_exec_sql_mode(storage, cache, sql, callback, arg, err, 1);
}

int executor_recover_wal(flexql_storage* storage, flexql_cache* cache, char** err) {
  FILE* fp;
  long wal_size = 0;
  if (storage->wal_path == NULL) {
    return 1;
  }
  fp = fopen(storage->wal_path, "rb");
  if (fp == NULL) {
    return wal_clear(storage);
  }
  if (fseek(fp, 0, SEEK_END) != 0) {
    fclose(fp);
    set_err(err, "failed to inspect WAL");
    return 0;
  }
  wal_size = ftell(fp);
  if (wal_size < 0) {
    fclose(fp);
    set_err(err, "failed to inspect WAL");
    return 0;
  }
  if (wal_size == 0) {
    fclose(fp);
    return wal_clear(storage);
  }
  if (fseek(fp, 0, SEEK_SET) != 0) {
    fclose(fp);
    set_err(err, "failed to inspect WAL");
    return 0;
  }
  for (;;) {
    unsigned int len = 0;
    char* sql;
    if (fread(&len, sizeof(len), 1, fp) != 1) {
      if (feof(fp)) {
        break;
      }
      fclose(fp);
      set_err(err, "failed to read WAL");
      return 0;
    }
    sql = (char*)malloc((size_t)len + 1);
    if (sql == NULL) {
      fclose(fp);
      set_err(err, "out of memory");
      return 0;
    }
    if (len > 0 && fread(sql, 1, len, fp) != len) {
      free(sql);
      fclose(fp);
      set_err(err, "failed to read WAL");
      return 0;
    }
    sql[len] = '\0';
    if (!executor_exec_sql_mode(storage, cache, sql, NULL, NULL, err, 0)) {
      free(sql);
      fclose(fp);
      return 0;
    }
    free(sql);
  }
  fclose(fp);
  if (!wal_clear(storage)) {
    set_err(err, "failed to checkpoint WAL");
    return 0;
  }
  return 1;
}
