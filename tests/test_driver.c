#include "common/flexql.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  char*** rows;
  int row_cap;
  int row_count;
  int col_count;
} collector;

static int collect_row(void* arg, int column_count, char** values, char** column_names) {
  int i;
  collector* c = (collector*)arg;
  char** row;
  (void)column_names;
  if (c->row_count == c->row_cap) {
    int ncap = c->row_cap == 0 ? 16 : c->row_cap * 2;
    char*** nrows = (char***)realloc(c->rows, (size_t)ncap * sizeof(char**));
    if (nrows == NULL) {
      return 1;
    }
    c->rows = nrows;
    c->row_cap = ncap;
  }
  row = (char**)calloc((size_t)column_count, sizeof(char*));
  if (row == NULL) {
    return 1;
  }
  c->col_count = column_count;
  for (i = 0; i < column_count; ++i) {
    const char* src = values[i] ? values[i] : "";
    size_t len = strlen(src);
    row[i] = (char*)malloc(len + 1);
    if (row[i] != NULL) {
      memcpy(row[i], src, len + 1);
    }
    if (row[i] == NULL) {
      return 1;
    }
  }
  c->rows[c->row_count] = row;
  c->row_count += 1;
  return 0;
}

static void exec_ok(flexql* db, const char* sql) {
  char* err = NULL;
  int rc = flexql_exec(db, sql, NULL, NULL, &err);
  if (rc != FLEXQL_OK) {
    fprintf(stderr, "SQL failed: %s\nError: %s\n", sql, err ? err : "unknown");
    flexql_free(err);
    exit(1);
  }
  flexql_free(err);
}

static collector query_rows(flexql* db, const char* sql) {
  collector c;
  char* err = NULL;
  int rc;
  memset(&c, 0, sizeof(c));
  rc = flexql_exec(db, sql, collect_row, &c, &err);
  if (rc != FLEXQL_OK) {
    fprintf(stderr, "Query failed: %s\nError: %s\n", sql, err ? err : "unknown");
    flexql_free(err);
    exit(1);
  }
  flexql_free(err);
  return c;
}

static void collector_free(collector* c) {
  int i;
  int j;
  for (i = 0; i < c->row_count; ++i) {
    for (j = 0; j < c->col_count; ++j) {
      free(c->rows[i][j]);
    }
    free(c->rows[i]);
  }
  free(c->rows);
  memset(c, 0, sizeof(*c));
}

int main(void) {
  flexql* db = NULL;
  char* err = NULL;
  int rc;

  assert(flexql_open("127.0.0.1", 9000, &db) == FLEXQL_OK);

  exec_ok(db, "CREATE TABLE IF NOT EXISTS TEST_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);");
  exec_ok(db, "DELETE FROM TEST_USERS;");
  exec_ok(db, "INSERT INTO TEST_USERS VALUES (1, 'Alice', 1200, 1893456000),(2, 'Bob', 450, 1893456000),(3, 'Carol', 2200, 1893456000),(4, 'Dave', 800, 1893456000);");

  {
    collector q = query_rows(db, "SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;");
    assert(q.row_count == 1 && strcmp(q.rows[0][0], "Bob") == 0);
    collector_free(&q);
  }

  {
    collector q = query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000 ORDER BY NAME;");
    assert(q.row_count == 2);
    assert(strcmp(q.rows[0][0], "Alice") == 0);
    assert(strcmp(q.rows[1][0], "Carol") == 0);
    collector_free(&q);
  }

  {
    collector q = query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE >= 1200 ORDER BY ID;");
    assert(q.row_count == 2);
    assert(strcmp(q.rows[0][0], "1") == 0);
    assert(strcmp(q.rows[1][0], "3") == 0);
    collector_free(&q);
  }

  {
    collector q = query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE <= 800 ORDER BY ID;");
    assert(q.row_count == 2);
    assert(strcmp(q.rows[0][0], "2") == 0);
    assert(strcmp(q.rows[1][0], "4") == 0);
    collector_free(&q);
  }

  exec_ok(db, "CREATE TABLE IF NOT EXISTS TEST_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);");
  exec_ok(db, "DELETE FROM TEST_ORDERS;");
  exec_ok(db, "INSERT INTO TEST_ORDERS VALUES (101, 1, 50, 1893456000),(102, 1, 150, 1893456000),(103, 3, 500, 1893456000);");

  {
    collector q = query_rows(db, "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID WHERE TEST_ORDERS.AMOUNT > 100;");
    assert(q.row_count == 2);
    collector_free(&q);
  }

  {
    collector q = query_rows(db, "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID <= TEST_ORDERS.USER_ID WHERE TEST_ORDERS.AMOUNT >= 150;");
    assert(q.row_count > 0);
    collector_free(&q);
  }

  rc = flexql_exec(db, "SELECT UNKNOWN_COLUMN FROM TEST_USERS;", NULL, NULL, &err);
  assert(rc == FLEXQL_ERROR);
  flexql_free(err);

  rc = flexql_exec(db, "SELECT NAME FROM TEST_USERS ORDER BY BALANCE;", NULL, NULL, &err);
  assert(rc == FLEXQL_ERROR);
  flexql_free(err);

  assert(flexql_close(db) == FLEXQL_OK);
  printf("All compatibility tests passed.\n");
  return 0;
}
