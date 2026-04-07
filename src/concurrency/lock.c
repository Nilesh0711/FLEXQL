#include "concurrency/lock.h"

int flexql_mutex_init(flexql_mutex* mu) { return pthread_mutex_init(mu, 0); }
int flexql_mutex_destroy(flexql_mutex* mu) { return pthread_mutex_destroy(mu); }
int flexql_mutex_lock(flexql_mutex* mu) { return pthread_mutex_lock(mu); }
int flexql_mutex_unlock(flexql_mutex* mu) { return pthread_mutex_unlock(mu); }

int flexql_rwlock_init(flexql_rwlock* lock) { return pthread_rwlock_init(lock, 0); }
int flexql_rwlock_destroy(flexql_rwlock* lock) { return pthread_rwlock_destroy(lock); }
int flexql_rwlock_rdlock(flexql_rwlock* lock) { return pthread_rwlock_rdlock(lock); }
int flexql_rwlock_wrlock(flexql_rwlock* lock) { return pthread_rwlock_wrlock(lock); }
int flexql_rwlock_unlock(flexql_rwlock* lock) { return pthread_rwlock_unlock(lock); }
