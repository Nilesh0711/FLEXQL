#ifndef FLEXQL_STR_UTILS_H
#define FLEXQL_STR_UTILS_H

#include <stddef.h>

char* fx_strdup(const char* s);
char* fx_strndup(const char* s, size_t n);
char* fx_trim_inplace(char* s);
int fx_ieq(const char* a, const char* b);
int fx_parse_number(const char* s, long double* out);

#endif
