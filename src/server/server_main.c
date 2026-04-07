#include "server/server.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static volatile sig_atomic_t g_stop = 0;

static void handle_signal(int signo) {
  (void)signo;
  g_stop = 1;
}

int main(int argc, char** argv) {
  const char* host = "127.0.0.1";
  int port = 9000;
  char* err = NULL;

  if (argc > 1) {
    port = atoi(argv[1]);
    if (port <= 0) {
      fprintf(stderr, "invalid port\n");
      return 1;
    }
  }

  signal(SIGINT, handle_signal);
  signal(SIGTERM, handle_signal);

  if (!server_runtime_start(host, port, &err)) {
    fprintf(stderr, "failed to start server: %s\n", err ? err : "unknown error");
    free(err);
    return 1;
  }

  printf("FlexQL server listening on %s:%d\n", host, port);
  fflush(stdout);

  while (!g_stop) {
    sleep(1);
  }

  server_runtime_stop();
  return 0;
}
