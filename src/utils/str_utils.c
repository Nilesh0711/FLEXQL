#include "utils/str_utils.h"

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

char* fx_strdup(const char* s) {
  size_t n;
  char* out;
  if (s == NULL) {
    return NULL;
  }
  n = strlen(s);
  out = (char*)malloc(n + 1);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, s, n + 1);
  return out;
}

char* fx_strndup(const char* s, size_t n) {
  char* out;
  if (s == NULL) {
    return NULL;
  }
  out = (char*)malloc(n + 1);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, s, n);
  out[n] = '\0';
  return out;
}

char* fx_trim_inplace(char* s) {
  char* end;
  if (s == NULL) {
    return NULL;
  }
  while (*s != '\0' && isspace((unsigned char)*s)) {
    s++;
  }
  if (*s == '\0') {
    return s;
  }
  end = s + strlen(s) - 1;
  while (end > s && isspace((unsigned char)*end)) {
    *end = '\0';
    end--;
  }
  return s;
}

int fx_ieq(const char* a, const char* b) {
  unsigned char ca;
  unsigned char cb;
  if (a == NULL || b == NULL) {
    return 0;
  }
  while (*a != '\0' && *b != '\0') {
    ca = (unsigned char)tolower((unsigned char)*a);
    cb = (unsigned char)tolower((unsigned char)*b);
    if (ca != cb) {
      return 0;
    }
    a++;
    b++;
  }
  return *a == '\0' && *b == '\0';
}

int fx_parse_number(const char* s, long double* out) {
  char* end;
  if (s == NULL || out == NULL) {
    return 0;
  }
  errno = 0;
  *out = strtold(s, &end);
  if (end == s || errno != 0) {
    return 0;
  }
  while (*end != '\0') {
    if (!isspace((unsigned char)*end)) {
      return 0;
    }
    end++;
  }
  return 1;
}
