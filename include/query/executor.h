#ifndef FLEXQL_EXECUTOR_H
#define FLEXQL_EXECUTOR_H

#include "cache/cache.h"
#include "common/flexql.h"
#include "storage/storage.h"

int executor_exec_sql(flexql_storage* storage, flexql_cache* cache, const char* sql,
                      flexql_callback callback, void* arg, char** err);
int executor_recover_wal(flexql_storage* storage, flexql_cache* cache, char** err);

#endif
