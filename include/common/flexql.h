#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct flexql flexql;

enum {
  FLEXQL_OK = 0,
  FLEXQL_ERROR = 1
};

typedef int (*flexql_callback)(void* data, int columnCount, char** values, char** columnNames);

int flexql_open(const char* host, int port, flexql** db);
int flexql_close(flexql* db);
int flexql_exec(flexql* db, const char* sql, flexql_callback callback, void* arg, char** errmsg);
void flexql_free(void* ptr);

#ifdef __cplusplus
}
#endif

#endif
