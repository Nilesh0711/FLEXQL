#include "common/flexql.h"

#include <stdlib.h>

#include "network/network.h"
#include "query/executor.h"
#include "server/flexql_internal.h"
#include "server/server.h"
#include "utils/str_utils.h"

int flexql_open(const char* host, int port, flexql** db) {
  struct flexql* h;
  char* err = NULL;

  if (db == NULL || host == NULL || !network_validate_endpoint(host, port)) {
    return FLEXQL_ERROR;
  }

  h = (struct flexql*)calloc(1, sizeof(struct flexql));
  if (h == NULL) {
    *db = NULL;
    return FLEXQL_ERROR;
  }

  h->host = fx_strdup(host);
  h->port = port;
  h->sock_fd = -1;
  h->use_local = 0;

  if (h->host == NULL) {
    free(h->host);
    free(h);
    *db = NULL;
    return FLEXQL_ERROR;
  }

  if (!server_client_open(host, port, &h->sock_fd, &err)) {
    free(err);
    free(h->host);
    free(h);
    *db = NULL;
    return FLEXQL_ERROR;
  }

  *db = h;
  return FLEXQL_OK;
}

int flexql_close(flexql* db) {
  struct flexql* h = (struct flexql*)db;
  if (h == NULL) {
    return FLEXQL_ERROR;
  }
  if (!h->use_local && h->sock_fd >= 0) {
    server_client_close(h->sock_fd);
  }
  if (h->use_local) {
    flexql_mutex_destroy(&h->local_mu);
    cache_free(&h->local_cache);
    storage_free(&h->local_storage);
  }
  free(h->host);
  free(h);
  return FLEXQL_OK;
}

int flexql_exec(flexql* db, const char* sql, flexql_callback callback, void* arg, char** errmsg) {
  struct flexql* h = (struct flexql*)db;
  if (errmsg != NULL) {
    *errmsg = NULL;
  }

  if (h == NULL || sql == NULL) {
    if (errmsg != NULL) {
      *errmsg = fx_strdup("invalid database handle or sql");
    }
    return FLEXQL_ERROR;
  }

  if (h->use_local) {
    int ok;
    flexql_mutex_lock(&h->local_mu);
    ok = executor_exec_sql(&h->local_storage, &h->local_cache, sql, callback, arg, errmsg);
    flexql_mutex_unlock(&h->local_mu);
    return ok ? FLEXQL_OK : FLEXQL_ERROR;
  }

  if (h->sock_fd < 0) {
    if (errmsg != NULL) {
      *errmsg = fx_strdup("invalid database handle or sql");
    }
    return FLEXQL_ERROR;
  }

  return server_client_exec(h->sock_fd, sql, callback, arg, errmsg) ? FLEXQL_OK : FLEXQL_ERROR;
}

void flexql_free(void* ptr) { free(ptr); }
