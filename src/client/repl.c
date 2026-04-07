#include "common/flexql.h"

#include <stdio.h>
#include <string.h>

static int print_row(void* data, int column_count, char** values, char** column_names) {
  int i;
  (void)data;
  for (i = 0; i < column_count; ++i) {
    printf("%s=%s", column_names[i], values[i] ? values[i] : "NULL");
    if (i + 1 < column_count) {
      printf(" | ");
    }
  }
  printf("\n");
  return 0;
}

int main(void) {
  flexql* db = NULL;
  char line[8192];

  if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK) {
    fprintf(stderr, "failed to open flexql\n");
    return 1;
  }

  printf("FlexQL REPL (type 'exit' to quit)\n");
  while (1) {
    char* err = NULL;
    int rc;
    printf("flexql> ");
    if (fgets(line, sizeof(line), stdin) == NULL) {
      break;
    }
    if (strcmp(line, "exit\n") == 0 || strcmp(line, "quit\n") == 0) {
      break;
    }

    rc = flexql_exec(db, line, print_row, NULL, &err);
    if (rc != FLEXQL_OK) {
      fprintf(stderr, "error: %s\n", err ? err : "unknown");
    }
    flexql_free(err);
  }

  flexql_close(db);
  return 0;
}
