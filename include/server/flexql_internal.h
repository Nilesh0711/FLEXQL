#ifndef FLEXQL_INTERNAL_H
#define FLEXQL_INTERNAL_H

#include "cache/cache.h"
#include "concurrency/lock.h"
#include "storage/storage.h"

struct flexql {
  char* host;
  int port;
  int sock_fd;
  int use_local;
  flexql_mutex local_mu;
  flexql_storage local_storage;
  flexql_cache local_cache;
};

#endif
