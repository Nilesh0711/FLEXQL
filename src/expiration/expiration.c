#include "expiration/expiration.h"

#include <time.h>

#include "utils/str_utils.h"

long long expiration_now_epoch(void) { return (long long)time(NULL); }

int expiration_is_expired(const flexql_table* table, const flexql_row* row, long long now_epoch) {
  long double exp_val;
  if (table == 0 || row == 0) {
    return 0;
  }
  if (table->expires_col_idx < 0 || table->expires_col_idx >= (int)table->column_count) {
    return 0;
  }
  if (!fx_parse_number(row->values[table->expires_col_idx], &exp_val)) {
    return 0;
  }
  return exp_val < (long double)now_epoch;
}
