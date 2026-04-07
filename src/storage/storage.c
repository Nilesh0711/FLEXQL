#include "storage/storage.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "index/index.h"
#include "utils/str_utils.h"

#define FLEXQL_DEFAULT_DATA_DIR "data/tables"
#define FLEXQL_DEFAULT_INDEX_DIR "data/indexes"
#define FLEXQL_DEFAULT_WAL_DIR "data/wal"
#define FLEXQL_META_SUFFIX ".meta"
#define FLEXQL_DATA_SUFFIX ".rows"
#define FLEXQL_INDEX_SUFFIX ".idx"
#define FLEXQL_META_MAGIC "FLEXQL_META_V2"

static void set_err(char** err, const char* msg) {
  if (err != NULL) {
    free(*err);
    *err = fx_strdup(msg);
  }
}

static int ensure_table_capacity(flexql_storage* st) {
  if (st->table_count == st->table_cap) {
    size_t ncap = st->table_cap == 0 ? 8 : st->table_cap * 2;
    flexql_table* nt = (flexql_table*)realloc(st->tables, ncap * sizeof(flexql_table));
    if (nt == NULL) {
      return 0;
    }
    st->tables = nt;
    st->table_cap = ncap;
  }
  return 1;
}

static int ensure_column_capacity(flexql_table* table) {
  if (table->column_count == table->column_cap) {
    size_t ncap = table->column_cap == 0 ? 8 : table->column_cap * 2;
    flexql_column* nc = (flexql_column*)realloc(table->columns, ncap * sizeof(flexql_column));
    if (nc == NULL) {
      return 0;
    }
    table->columns = nc;
    table->column_cap = ncap;
  }
  return 1;
}

static int parse_varchar_limit(const char* type) {
  const char* lp = strchr(type, '(');
  const char* rp = strchr(type, ')');
  long double n = 0.0;
  char* num_txt;
  if (lp == NULL || rp == NULL || rp <= lp + 1) {
    return -1;
  }
  num_txt = fx_strndup(lp + 1, (size_t)(rp - (lp + 1)));
  if (num_txt == NULL) {
    return -1;
  }
  if (!fx_parse_number(num_txt, &n)) {
    free(num_txt);
    return -1;
  }
  free(num_txt);
  if (n <= 0.0) {
    return -1;
  }
  return (int)n;
}

static int is_int_value(const char* value) {
  long double n = 0.0;
  long long as_i = 0;
  if (!fx_parse_number(value, &n)) {
    return 0;
  }
  as_i = (long long)n;
  return n == (long double)as_i;
}

static int is_decimal_value(const char* value) {
  long double n = 0.0;
  return fx_parse_number(value, &n);
}

static int is_datetime_value(const char* value) {
  int i;
  long double n = 0.0;
  if (fx_parse_number(value, &n)) {
    return 1;
  }
  if (strlen(value) != 19) {
    return 0;
  }
  for (i = 0; i < 19; ++i) {
    if (i == 4 || i == 7) {
      if (value[i] != '-') return 0;
    } else if (i == 10) {
      if (value[i] != ' ') return 0;
    } else if (i == 13 || i == 16) {
      if (value[i] != ':') return 0;
    } else if (!isdigit((unsigned char)value[i])) {
      return 0;
    }
  }
  return 1;
}

static int validate_value_for_type(const char* type, const char* value) {
  if (fx_ieq(type, "INT")) {
    return is_int_value(value);
  }
  if (fx_ieq(type, "DECIMAL")) {
    return is_decimal_value(value);
  }
  if (fx_ieq(type, "DATETIME")) {
    return is_datetime_value(value);
  }
  if (strncmp(type, "VARCHAR", 7) == 0 || strncmp(type, "varchar", 7) == 0) {
    int lim = parse_varchar_limit(type);
    if (lim < 0) {
      return 1;
    }
    return (int)strlen(value) <= lim;
  }
  return 1;
}

int table_find_column(const flexql_table* table, const char* col_name) {
  size_t i;
  for (i = 0; i < table->column_count; ++i) {
    if (fx_ieq(table->columns[i].name, col_name)) {
      return (int)i;
    }
  }
  return -1;
}

static void table_free(flexql_table* table) {
  size_t i;
  for (i = 0; i < table->column_count; ++i) {
    free(table->columns[i].name);
    free(table->columns[i].type);
  }
  free(table->columns);
  free(table->name);
  free(table->meta_path);
  free(table->data_path);
  free(table->index_path);
  memset(table, 0, sizeof(*table));
  table->expires_col_idx = -1;
}

static int ensure_dir_recursive(const char* path) {
  char* copy;
  char* p;
  struct stat st;
  copy = fx_strdup(path);
  if (copy == NULL) {
    return 0;
  }
  for (p = copy + 1; *p != '\0'; ++p) {
    if (*p == '/') {
      *p = '\0';
      if (mkdir(copy, 0777) != 0 && errno != EEXIST) {
        free(copy);
        return 0;
      }
      *p = '/';
    }
  }
  if (mkdir(copy, 0777) != 0 && errno != EEXIST) {
    free(copy);
    return 0;
  }
  if (stat(copy, &st) != 0 || !S_ISDIR(st.st_mode)) {
    free(copy);
    return 0;
  }
  free(copy);
  return 1;
}

static char* build_path(const char* dir, const char* table_name, const char* suffix) {
  size_t dir_len = strlen(dir);
  size_t name_len = strlen(table_name);
  size_t suf_len = strlen(suffix);
  char* path = (char*)malloc(dir_len + name_len + suf_len + 2);
  if (path == NULL) {
    return NULL;
  }
  memcpy(path, dir, dir_len);
  path[dir_len] = '/';
  memcpy(path + dir_len + 1, table_name, name_len);
  memcpy(path + dir_len + 1 + name_len, suffix, suf_len + 1);
  return path;
}

static int file_exists(const char* path) {
  struct stat st;
  return path != NULL && stat(path, &st) == 0;
}

static char* build_index_path(const flexql_storage* st, const char* table_name) {
  const char* dir = st->index_dir != NULL ? st->index_dir : st->data_dir;
  return build_path(dir, table_name, FLEXQL_INDEX_SUFFIX);
}

static char* resolve_index_path(const flexql_storage* st, const char* table_name, int migrate) {
  char* preferred = build_index_path(st, table_name);
  char* legacy = build_path(st->data_dir, table_name, FLEXQL_INDEX_SUFFIX);
  if (preferred == NULL || legacy == NULL) {
    free(preferred);
    free(legacy);
    return NULL;
  }

  if (strcmp(preferred, legacy) == 0) {
    free(legacy);
    return preferred;
  }
  if (file_exists(preferred)) {
    free(legacy);
    return preferred;
  }
  if (file_exists(legacy)) {
    if (migrate && rename(legacy, preferred) == 0) {
      free(legacy);
      return preferred;
    }
    free(preferred);
    return legacy;
  }

  free(legacy);
  return preferred;
}

static int write_meta(const flexql_table* table, char** err) {
  size_t i;
  FILE* fp = fopen(table->meta_path, "wb");
  if (fp == NULL) {
    set_err(err, "failed to persist table");
    return 0;
  }
  if (fprintf(fp, "%s\n", FLEXQL_META_MAGIC) < 0 ||
      fprintf(fp, "TABLE\t%s\t%zu\t%zu\t%d\n", table->name, table->column_count, table->row_count,
              table->expires_col_idx) < 0) {
    fclose(fp);
    set_err(err, "failed to persist table");
    return 0;
  }
  for (i = 0; i < table->column_count; ++i) {
    if (fprintf(fp, "COL\t%s\t%s\n", table->columns[i].name, table->columns[i].type) < 0) {
      fclose(fp);
      set_err(err, "failed to persist table");
      return 0;
    }
  }
  if (fclose(fp) != 0) {
    set_err(err, "failed to persist table");
    return 0;
  }
  return 1;
}

static int append_row_file(FILE* fp, const char** values, size_t value_count) {
  size_t i;
  uint32_t field_count = (uint32_t)value_count;
  char buf[4096];
  char* p = buf;

  memcpy(p, &field_count, sizeof(field_count));
  p += sizeof(field_count);

  for (i = 0; i < value_count; ++i) {
    uint32_t len = (uint32_t)strlen(values[i]);
    if ((size_t)(p - buf) + sizeof(len) + len > sizeof(buf)) {
      if (fwrite(buf, 1, p - buf, fp) != (size_t)(p - buf)) {
        return 0;
      }
      p = buf;
      if (sizeof(len) + len > sizeof(buf)) {
        if (fwrite(&len, sizeof(len), 1, fp) != 1) {
          return 0;
        }
        if (len > 0 && fwrite(values[i], 1, len, fp) != len) {
          return 0;
        }
        continue;
      }
    }
    memcpy(p, &len, sizeof(len));
    p += sizeof(len);
    if (len > 0) {
      memcpy(p, values[i], len);
      p += len;
    }
  }
  if (p > buf) {
    if (fwrite(buf, 1, p - buf, fp) != (size_t)(p - buf)) {
      return 0;
    }
  }
  return 1;
}

static int read_row_file(FILE* fp, size_t expected_cols, flexql_row* out_row) {
  size_t i;
  uint32_t field_count = 0;
  memset(out_row, 0, sizeof(*out_row));
  out_row->is_reused = 0;
  if (fread(&field_count, sizeof(field_count), 1, fp) != 1) {
    return feof(fp) ? 0 : -1;
  }
  if ((size_t)field_count != expected_cols) {
    return -1;
  }
  out_row->values = (char**)calloc(expected_cols, sizeof(char*));
  if (out_row->values == NULL && expected_cols != 0) {
    return -1;
  }
  for (i = 0; i < expected_cols; ++i) {
    uint32_t len = 0;
    if (fread(&len, sizeof(len), 1, fp) != 1) {
      storage_row_free(out_row, expected_cols);
      return -1;
    }
    out_row->values[i] = (char*)malloc((size_t)len + 1);
    if (out_row->values[i] == NULL) {
      storage_row_free(out_row, expected_cols);
      return -1;
    }
    if (len > 0 && fread(out_row->values[i], 1, len, fp) != len) {
      storage_row_free(out_row, expected_cols);
      return -1;
    }
    out_row->values[i][len] = '\0';
  }
  return 1;
}

void storage_row_free(flexql_row* row, size_t column_count) {
  size_t i;
  if (row == NULL || row->values == NULL) {
    return;
  }
  if (row->is_reused) {
    row->values = NULL;
    return;
  }
  for (i = 0; i < column_count; ++i) {
    free(row->values[i]);
  }
  free(row->values);
  row->values = NULL;
}

static int storage_create_table_internal(flexql_storage* st, const char* table_name,
                                         int if_not_exists, const char** col_names,
                                         const char** col_types, size_t col_count, int persist,
                                         char** err) {
  size_t i;
  flexql_table table;
  if (col_count == 0) {
    set_err(err, "CREATE TABLE must define at least one column");
    return 0;
  }
  if (storage_find_table(st, table_name) != NULL) {
    if (if_not_exists) {
      return 1;
    }
    set_err(err, "table already exists");
    return 0;
  }
  if (!ensure_table_capacity(st)) {
    set_err(err, "out of memory");
    return 0;
  }

  memset(&table, 0, sizeof(table));
  table.expires_col_idx = -1;
  table.name = fx_strdup(table_name);
  table.meta_path = build_path(st->data_dir, table_name, FLEXQL_META_SUFFIX);
  table.data_path = build_path(st->data_dir, table_name, FLEXQL_DATA_SUFFIX);
  table.index_path = resolve_index_path(st, table_name, persist ? 0 : 1);
  if (table.name == NULL || table.meta_path == NULL || table.data_path == NULL ||
      table.index_path == NULL) {
    table_free(&table);
    set_err(err, "out of memory");
    return 0;
  }

  for (i = 0; i < col_count; ++i) {
    if (!ensure_column_capacity(&table)) {
      table_free(&table);
      set_err(err, "out of memory");
      return 0;
    }
    table.columns[table.column_count].name = fx_strdup(col_names[i]);
    table.columns[table.column_count].type = fx_strdup(col_types[i]);
    if (table.columns[table.column_count].name == NULL ||
        table.columns[table.column_count].type == NULL) {
      table_free(&table);
      set_err(err, "out of memory");
      return 0;
    }
    if (fx_ieq(col_names[i], "EXPIRES_AT")) {
      table.expires_col_idx = (int)table.column_count;
    }
    table.column_count += 1;
  }

  if (persist) {
    FILE* data_fp = fopen(table.data_path, "ab");
    if (data_fp == NULL) {
      table_free(&table);
      set_err(err, "failed to persist table");
      return 0;
    }
    fclose(data_fp);
    if (!index_file_create(table.index_path) || !write_meta(&table, err)) {
      table_free(&table);
      return 0;
    }
  }

  st->tables[st->table_count++] = table;
  return 1;
}

static int load_table_from_meta(flexql_storage* st, const char* table_name) {
  FILE* fp;
  char line[1024];
  size_t col_count;
  size_t row_count;
  int expires_idx;
  char** col_names;
  char** col_types;
  size_t i;
  char* meta_path = build_path(st->data_dir, table_name, FLEXQL_META_SUFFIX);
  if (meta_path == NULL) {
    free(meta_path);
    return 0;
  }
  fp = fopen(meta_path, "rb");
  if (fp == NULL) {
    free(meta_path);
    return 0;
  }
  if (fgets(line, sizeof(line), fp) == NULL || strncmp(line, FLEXQL_META_MAGIC, 14) != 0) {
    fclose(fp);
    free(meta_path);
    return 0;
  }
  if (fgets(line, sizeof(line), fp) == NULL ||
      sscanf(line, "TABLE\t%*s\t%zu\t%zu\t%d", &col_count, &row_count, &expires_idx) != 3) {
    fclose(fp);
    free(meta_path);
    return 0;
  }

  col_names = (char**)calloc(col_count, sizeof(char*));
  col_types = (char**)calloc(col_count, sizeof(char*));
  if (col_names == NULL || col_types == NULL) {
    fclose(fp);
    free(meta_path);
    free(col_names);
    free(col_types);
    return 0;
  }

  for (i = 0; i < col_count; ++i) {
    char name[256];
    char type[256];
    if (fgets(line, sizeof(line), fp) == NULL ||
        sscanf(line, "COL\t%255[^\t]\t%255[^\n]", name, type) != 2) {
      fclose(fp);
      free(meta_path);
      while (i > 0) {
        i--;
        free(col_names[i]);
        free(col_types[i]);
      }
      free(col_names);
      free(col_types);
      return 0;
    }
    col_names[i] = fx_strdup(name);
    col_types[i] = fx_strdup(type);
  }
  fclose(fp);

  if (!storage_create_table_internal(st, table_name, 0, (const char**)col_names,
                                     (const char**)col_types, col_count, 0, NULL)) {
    for (i = 0; i < col_count; ++i) {
      free(col_names[i]);
      free(col_types[i]);
    }
    free(col_names);
    free(col_types);
    free(meta_path);
    return 0;
  }

  {
    flexql_table* table = &st->tables[st->table_count - 1];
    table->row_count = row_count;
    table->expires_col_idx = expires_idx;
  }

  for (i = 0; i < col_count; ++i) {
    free(col_names[i]);
    free(col_types[i]);
  }
  free(col_names);
  free(col_types);
  free(meta_path);
  return 1;
}

static void load_existing_tables(flexql_storage* st) {
  DIR* dir = opendir(st->data_dir);
  struct dirent* entry;
  size_t suffix_len = strlen(FLEXQL_META_SUFFIX);
  if (dir == NULL) {
    return;
  }
  while ((entry = readdir(dir)) != NULL) {
    size_t len = strlen(entry->d_name);
    char* table_name;
    if (len <= suffix_len ||
        strcmp(entry->d_name + len - suffix_len, FLEXQL_META_SUFFIX) != 0) {
      continue;
    }
    table_name = fx_strndup(entry->d_name, len - suffix_len);
    if (table_name == NULL) {
      continue;
    }
    load_table_from_meta(st, table_name);
    free(table_name);
  }
  closedir(dir);
}

void storage_init(flexql_storage* st, const char* data_dir, const char* wal_dir) {
  memset(st, 0, sizeof(*st));
  st->data_dir = fx_strdup(data_dir ? data_dir : FLEXQL_DEFAULT_DATA_DIR);
  st->index_dir = fx_strdup(FLEXQL_DEFAULT_INDEX_DIR);
  st->wal_dir = fx_strdup(wal_dir ? wal_dir : FLEXQL_DEFAULT_WAL_DIR);
  st->wal_path = build_path(st->wal_dir ? st->wal_dir : FLEXQL_DEFAULT_WAL_DIR, "flexql", ".wal");
  if (st->data_dir == NULL || st->index_dir == NULL || st->wal_dir == NULL ||
      st->wal_path == NULL) {
    return;
  }
  if (!ensure_dir_recursive(st->data_dir)) {
    free(st->data_dir);
    free(st->index_dir);
    st->data_dir = NULL;
    st->index_dir = NULL;
    return;
  }
  if (!ensure_dir_recursive(st->index_dir)) {
    free(st->data_dir);
    free(st->index_dir);
    free(st->wal_dir);
    free(st->wal_path);
    st->data_dir = NULL;
    st->index_dir = NULL;
    st->wal_dir = NULL;
    st->wal_path = NULL;
    return;
  }
  if (!ensure_dir_recursive(st->wal_dir)) {
    free(st->data_dir);
    free(st->index_dir);
    free(st->wal_dir);
    free(st->wal_path);
    st->data_dir = NULL;
    st->index_dir = NULL;
    st->wal_dir = NULL;
    st->wal_path = NULL;
    return;
  }
  load_existing_tables(st);
}

void storage_free(flexql_storage* st) {
  size_t i;
  for (i = 0; i < st->table_count; ++i) {
    table_free(&st->tables[i]);
  }
  free(st->tables);
  free(st->data_dir);
  free(st->index_dir);
  free(st->wal_dir);
  free(st->wal_path);
  memset(st, 0, sizeof(*st));
}

flexql_table* storage_find_table(flexql_storage* st, const char* table_name) {
  size_t i;
  for (i = 0; i < st->table_count; ++i) {
    if (fx_ieq(st->tables[i].name, table_name)) {
      return &st->tables[i];
    }
  }
  return NULL;
}

const flexql_table* storage_find_table_const(const flexql_storage* st, const char* table_name) {
  size_t i;
  for (i = 0; i < st->table_count; ++i) {
    if (fx_ieq(st->tables[i].name, table_name)) {
      return &st->tables[i];
    }
  }
  return NULL;
}

int storage_create_table(flexql_storage* st, const char* table_name, int if_not_exists,
                         const char** col_names, const char** col_types, size_t col_count,
                         char** err) {
  return storage_create_table_internal(st, table_name, if_not_exists, col_names, col_types,
                                       col_count, 1, err);
}

int storage_insert_rows(flexql_storage* st, const char* table_name, const char*** rows,
                        size_t row_count, size_t value_count, char** err) {
  size_t i;
  flexql_table* table = storage_find_table(st, table_name);
  FILE* fp;
  uint64_t* offsets = NULL;
  const char** keys = NULL;
  if (table == NULL) {
    set_err(err, "table not found");
    return 0;
  }
  if (value_count != table->column_count) {
    set_err(err, "column/value count mismatch in INSERT");
    return 0;
  }
  fp = fopen(table->data_path, "ab");
  if (fp == NULL) {
    set_err(err, "failed to persist table");
    return 0;
  }
  setvbuf(fp, NULL, _IOFBF, 65536);
  if (fseek(fp, 0, SEEK_END) != 0) {
    fclose(fp);
    set_err(err, "failed to persist table");
    return 0;
  }
  long fpos = ftell(fp);
  if (fpos < 0) {
    fclose(fp);
    set_err(err, "failed to persist table");
    return 0;
  }
  uint64_t current_offset = (uint64_t)fpos;

  offsets = (uint64_t*)calloc(row_count, sizeof(uint64_t));
  keys = (const char**)calloc(row_count, sizeof(const char*));
  if ((offsets == NULL || keys == NULL) && row_count != 0) {
    fclose(fp);
    free(offsets);
    free(keys);
    set_err(err, "out of memory");
    return 0;
  }
  for (i = 0; i < row_count; ++i) {
    size_t j;
    uint64_t row_len = sizeof(uint32_t);
    for (j = 0; j < value_count; ++j) {
      if (!validate_value_for_type(table->columns[j].type, rows[i][j])) {
        fclose(fp);
        free(offsets);
        free(keys);
        set_err(err, "type mismatch in INSERT");
        return 0;
      }
      row_len += sizeof(uint32_t) + strlen(rows[i][j]);
    }
    offsets[i] = current_offset;
    if (!append_row_file(fp, rows[i], value_count)) {
      fclose(fp);
      free(offsets);
      free(keys);
      set_err(err, "failed to persist table");
      return 0;
    }
    current_offset += row_len;
    keys[i] = rows[i][0];
    table->row_count += 1;
  }
  if (fclose(fp) != 0 || !index_file_insert_batch(table->index_path, keys, offsets, row_count) ||
      !write_meta(table, err)) {
    free(offsets);
    free(keys);
    return 0;
  }
  free(offsets);
  free(keys);
  return 1;
}

int storage_insert_row(flexql_storage* st, const char* table_name, const char** values,
                       size_t value_count, char** err) {
  return storage_insert_rows(st, table_name, &values, 1, value_count, err);
}

int storage_clear_table_rows(flexql_storage* st, const char* table_name, char** err) {
  FILE* fp;
  flexql_table* table = storage_find_table(st, table_name);
  if (table == NULL) {
    set_err(err, "table not found");
    return 0;
  }
  fp = fopen(table->data_path, "wb");
  if (fp == NULL) {
    set_err(err, "failed to persist table");
    return 0;
  }
  fclose(fp);
  table->row_count = 0;
  if (!index_file_reset(table->index_path) || !write_meta(table, err)) {
    return 0;
  }
  return 1;
}

int storage_scan_open(const flexql_table* table, flexql_table_scan* scan, char** err) {
  memset(scan, 0, sizeof(*scan));
  scan->table = table;
  scan->fp = fopen(table->data_path, "rb");
  if (scan->fp == NULL) {
    set_err(err, "failed to read table");
    return 0;
  }
  scan->read_buf = (char*)malloc(1024 * 1024);
  if (scan->read_buf == NULL) {
    fclose(scan->fp);
    set_err(err, "out of memory");
    return 0;
  }
  return 1;
}

static int scan_read_bytes(flexql_table_scan* scan, void* dst, size_t len) {
    char* target = (char*)dst;
    while (len > 0) {
        if (scan->read_pos >= scan->read_end) {
            size_t n = fread(scan->read_buf, 1, 1024 * 1024, scan->fp);
            if (n == 0) return 0;
            scan->read_pos = 0;
            scan->read_end = n;
        }
        size_t avail = scan->read_end - scan->read_pos;
        size_t copy = len < avail ? len : avail;
        memcpy(target, scan->read_buf + scan->read_pos, copy);
        target += copy;
        scan->read_pos += copy;
        len -= copy;
    }
    return 1;
}

int storage_scan_next(flexql_table_scan* scan, flexql_row* out_row, char** err) {
  uint32_t field_count = 0;
  size_t expected_cols = scan->table->column_count;
  size_t offset = 0;
  size_t i;

  if (!scan_read_bytes(scan, &field_count, sizeof(field_count))) {
    if (feof(scan->fp) && scan->read_pos >= scan->read_end) return 0;
    set_err(err, "failed to read table");
    return -1;
  }
  if ((size_t)field_count != expected_cols) {
    set_err(err, "failed to read table");
    return -1;
  }

  if (scan->values == NULL) {
    scan->values = (char**)calloc(expected_cols, sizeof(char*));
    scan->buf_cap = 4096;
    scan->buf = (char*)malloc(scan->buf_cap);
    if (scan->values == NULL || scan->buf == NULL) {
      set_err(err, "out of memory");
      return -1;
    }
  }

  out_row->values = scan->values;
  out_row->is_reused = 1;

  for (i = 0; i < expected_cols; ++i) {
    uint32_t len = 0;
    if (!scan_read_bytes(scan, &len, sizeof(len))) {
      set_err(err, "failed to read table");
      return -1;
    }
    if (offset + len + 1 > scan->buf_cap) {
      scan->buf_cap = (offset + len + 1) * 2;
      char* nbuf = (char*)realloc(scan->buf, scan->buf_cap);
      if (nbuf == NULL) {
        set_err(err, "out of memory");
        return -1;
      }
      scan->buf = nbuf;
    }
    if (len > 0 && !scan_read_bytes(scan, scan->buf + offset, len)) {
      set_err(err, "failed to read table");
      return -1;
    }
    scan->buf[offset + len] = '\0';
    scan->values[i] = scan->buf + offset;
    offset += len + 1;
  }
  return 1;
}

void storage_scan_close(flexql_table_scan* scan) {
  if (scan->fp != NULL) {
    fclose(scan->fp);
  }
  free(scan->buf);
  free(scan->values);
  free(scan->read_buf);
  memset(scan, 0, sizeof(*scan));
}

int storage_lookup_primary(const flexql_table* table, const char* key, flexql_row* out_row,
                           int* found, char** err) {
  uint64_t offset = 0;
  FILE* fp;
  if (found != NULL) {
    *found = 0;
  }
  if (!index_file_find(table->index_path, key, &offset, found)) {
    set_err(err, "failed to read index");
    return 0;
  }
  if (found == NULL || !*found) {
    return 1;
  }
  fp = fopen(table->data_path, "rb");
  if (fp == NULL) {
    set_err(err, "failed to read table");
    return 0;
  }
  if (fseek(fp, (long)offset, SEEK_SET) != 0 || read_row_file(fp, table->column_count, out_row) <= 0) {
    fclose(fp);
    set_err(err, "failed to read table");
    return 0;
  }
  fclose(fp);
  return 1;
}
