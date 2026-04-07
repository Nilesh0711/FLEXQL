#ifndef FLEXQL_LOCK_H
#define FLEXQL_LOCK_H

#include <pthread.h>

typedef pthread_mutex_t flexql_mutex;
typedef pthread_rwlock_t flexql_rwlock;

int flexql_mutex_init(flexql_mutex* mu);
int flexql_mutex_destroy(flexql_mutex* mu);
int flexql_mutex_lock(flexql_mutex* mu);
int flexql_mutex_unlock(flexql_mutex* mu);

int flexql_rwlock_init(flexql_rwlock* lock);
int flexql_rwlock_destroy(flexql_rwlock* lock);
int flexql_rwlock_rdlock(flexql_rwlock* lock);
int flexql_rwlock_wrlock(flexql_rwlock* lock);
int flexql_rwlock_unlock(flexql_rwlock* lock);

#endif
