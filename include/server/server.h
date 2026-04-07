#ifndef FLEXQL_SERVER_RUNTIME_H
#define FLEXQL_SERVER_RUNTIME_H

#include "common/flexql.h"

int server_runtime_start(const char* host, int port, char** err);
void server_runtime_stop(void);
int server_client_open(const char* host, int port, int* sock_fd, char** err);
void server_client_close(int sock_fd);
int server_client_exec(int sock_fd, const char* sql, flexql_callback callback, void* arg, char** err);

#endif
