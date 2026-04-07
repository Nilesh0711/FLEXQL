#ifndef FLEXQL_PARSER_H
#define FLEXQL_PARSER_H

#include <stddef.h>

typedef struct {
  char* left;
  char* op;
  char* right;
  int right_quoted;
  int right_is_literal;
} flexql_condition;

typedef enum {
  FLEXQL_STMT_NONE = 0,
  FLEXQL_STMT_CREATE,
  FLEXQL_STMT_INSERT,
  FLEXQL_STMT_SELECT,
  FLEXQL_STMT_DELETE,
  FLEXQL_STMT_RESET
} flexql_stmt_type;

typedef struct {
  int if_not_exists;
  char* table_name;
  char** col_names;
  char** col_types;
  size_t col_count;
} flexql_stmt_create;

typedef struct {
  char* table_name;
  char*** rows;
  size_t row_count;
  size_t value_count;
} flexql_stmt_insert;

typedef struct {
  int select_all;
  char** select_items;
  size_t select_count;
  char* from_table;
  int has_join;
  char* join_table;
  flexql_condition join_cond;
  int has_where;
  flexql_condition where_cond;
  int has_order_by;
  char* order_by;
} flexql_stmt_select;

typedef struct {
  char* table_name;
} flexql_stmt_delete;

typedef struct {
  char* table_name;
} flexql_stmt_reset;

typedef struct {
  flexql_stmt_type type;
  union {
    flexql_stmt_create create_stmt;
    flexql_stmt_insert insert_stmt;
    flexql_stmt_select select_stmt;
    flexql_stmt_delete delete_stmt;
    flexql_stmt_reset reset_stmt;
  } as;
} flexql_statement;

void parser_free_statement(flexql_statement* stmt);
int parser_parse_sql(const char* sql, flexql_statement* stmt, char** err);

#endif
