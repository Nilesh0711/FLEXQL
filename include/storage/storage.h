#ifndef FLEXQL_STORAGE_H
#define FLEXQL_STORAGE_H

#include <stddef.h>
#include <stdio.h>

typedef struct {
  char* name;
  char* type;
} flexql_column;

typedef struct {
  char** values;
  int is_reused;
} flexql_row;

typedef struct {
  char* name;
  flexql_column* columns;
  size_t column_count;
  size_t column_cap;
  size_t row_count;
  int expires_col_idx;
  char* meta_path;
  char* data_path;
  char* index_path;
} flexql_table;

typedef struct {
  flexql_table* tables;
  size_t table_count;
  size_t table_cap;
  char* data_dir;
  char* index_dir;
  char* wal_dir;
  char* wal_path;
} flexql_storage;

typedef struct {
  const flexql_table* table;
  FILE* fp;
  char* buf;
  size_t buf_cap;
  char** values;
  char* read_buf;
  size_t read_pos;
  size_t read_end;
} flexql_table_scan;

void storage_init(flexql_storage* st, const char* data_dir, const char* wal_dir);
void storage_free(flexql_storage* st);
flexql_table* storage_find_table(flexql_storage* st, const char* table_name);
const flexql_table* storage_find_table_const(const flexql_storage* st, const char* table_name);
int storage_create_table(flexql_storage* st, const char* table_name, int if_not_exists,
                         const char** col_names, const char** col_types, size_t col_count,
                         char** err);
int storage_insert_row(flexql_storage* st, const char* table_name, const char** values,
                       size_t value_count, char** err);
int storage_insert_rows(flexql_storage* st, const char* table_name, const char*** rows,
                        size_t row_count, size_t value_count, char** err);
int storage_clear_table_rows(flexql_storage* st, const char* table_name, char** err);
int storage_scan_open(const flexql_table* table, flexql_table_scan* scan, char** err);
int storage_scan_next(flexql_table_scan* scan, flexql_row* out_row, char** err);
void storage_scan_close(flexql_table_scan* scan);
void storage_row_free(flexql_row* row, size_t column_count);
int storage_lookup_primary(const flexql_table* table, const char* key, flexql_row* out_row,
                           int* found, char** err);

int table_find_column(const flexql_table* table, const char* col_name);

#endif
