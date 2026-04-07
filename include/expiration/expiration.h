#ifndef FLEXQL_EXPIRATION_H
#define FLEXQL_EXPIRATION_H

#include "storage/storage.h"

int expiration_is_expired(const flexql_table* table, const flexql_row* row, long long now_epoch);
long long expiration_now_epoch(void);

#endif
