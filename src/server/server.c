#include "server/server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "cache/cache.h"
#include "concurrency/lock.h"
#include "query/executor.h"
#include "storage/storage.h"
#include "utils/str_utils.h"

typedef struct {
  int fd;
  char buf[8192];
  size_t pos;
  int col_sent;
  uint32_t col_count;
} stream_ctx;

static int tx_flush(stream_ctx* tx) {
  if (tx->pos > 0) {
    const char* p = tx->buf;
    size_t len = tx->pos;
    while (len > 0) {
      ssize_t n = send(tx->fd, p, len, 0);
      if (n <= 0) return 0;
      p += (size_t)n;
      len -= (size_t)n;
    }
    tx->pos = 0;
  }
  return 1;
}

static int tx_write(stream_ctx* tx, const void* data, size_t len) {
  const char* p = (const char*)data;
  while (len > 0) {
    size_t avail = sizeof(tx->buf) - tx->pos;
    if (avail == 0) {
      if (!tx_flush(tx)) return 0;
      avail = sizeof(tx->buf);
    }
    size_t copy = len < avail ? len : avail;
    memcpy(tx->buf + tx->pos, p, copy);
    tx->pos += copy;
    p += copy;
    len -= copy;
  }
  return 1;
}

static int tx_u32(stream_ctx* tx, uint32_t v) {
  uint32_t n = htonl(v);
  return tx_write(tx, &n, sizeof(n));
}

static int tx_string(stream_ctx* tx, const char* s) {
  uint32_t len = s ? (uint32_t)strlen(s) : 0;
  if (!tx_u32(tx, len)) return 0;
  if (len > 0) return tx_write(tx, s, len);
  return 1;
}

typedef struct {
  int initialized;
  int started;
  int listen_fd;
  int port;
  int stop_flag;
  pthread_t accept_thread;
  flexql_mutex state_mu;
  flexql_rwlock db_lock;
  flexql_storage storage;
  flexql_cache cache;
} server_state;

static server_state g_server;

static char* errno_message(const char* prefix) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s: %s", prefix, strerror(errno));
  return fx_strdup(buf);
}

static int send_u32(int fd, uint32_t v);
static int send_string(int fd, const char* s);

static int stream_callback(void* data, int columnCount, char** values, char** columnNames) {
  stream_ctx* ctx = (stream_ctx*)data;
  int i;
  if (!ctx->col_sent) {
    if (!tx_u32(ctx, 0)) return 1;
    if (!tx_u32(ctx, (uint32_t)columnCount)) return 1;
    for (i = 0; i < columnCount; ++i) {
      if (!tx_string(ctx, columnNames[i])) return 1;
    }
    ctx->col_sent = 1;
    ctx->col_count = (uint32_t)columnCount;
  }
  if (!tx_u32(ctx, 1)) return 1;
  for (i = 0; i < columnCount; ++i) {
    if (!tx_string(ctx, values[i] ? values[i] : "")) return 1;
  }
  return 0;
}

static int send_all(int fd, const void* buf, size_t len) {
  const char* p = (const char*)buf;
  while (len > 0) {
    ssize_t n = send(fd, p, len, 0);
    if (n <= 0) {
      return 0;
    }
    p += (size_t)n;
    len -= (size_t)n;
  }
  return 1;
}

static int recv_all(int fd, void* buf, size_t len) {
  char* p = (char*)buf;
  while (len > 0) {
    ssize_t n = recv(fd, p, len, 0);
    if (n <= 0) {
      return 0;
    }
    p += (size_t)n;
    len -= (size_t)n;
  }
  return 1;
}

static int send_u32(int fd, uint32_t v) {
  uint32_t n = htonl(v);
  return send_all(fd, &n, sizeof(n));
}

static int recv_u32(int fd, uint32_t* out) {
  uint32_t n = 0;
  if (!recv_all(fd, &n, sizeof(n))) {
    return 0;
  }
  *out = ntohl(n);
  return 1;
}

static int send_string(int fd, const char* s) {
  uint32_t n = s ? (uint32_t)strlen(s) : 0;
  if (!send_u32(fd, n)) {
    return 0;
  }
  if (n == 0) {
    return 1;
  }
  return send_all(fd, s, n);
}

static char* recv_string(int fd) {
  uint32_t n = 0;
  char* s;
  if (!recv_u32(fd, &n)) {
    return NULL;
  }
  s = (char*)malloc((size_t)n + 1);
  if (s == NULL) {
    return NULL;
  }
  if (n > 0 && !recv_all(fd, s, n)) {
    free(s);
    return NULL;
  }
  s[n] = '\0';
  return s;
}



static int server_send_err(int fd, const char* err) {
  if (!send_u32(fd, 1)) {
    return 0;
  }
  return send_string(fd, err ? err : "unknown error");
}

static int sql_is_read_only(const char* sql) {
  size_t len = 0;
  char verb[16];
  while (*sql != '\0' && *sql <= ' ') {
    sql++;
  }
  while (sql[len] != '\0' && sql[len] > ' ' && len + 1 < sizeof(verb)) {
    verb[len] = sql[len];
    len++;
  }
  verb[len] = '\0';
  return fx_ieq(verb, "SELECT");
}

static void* client_worker(void* arg) {
  int fd = *(int*)arg;
  free(arg);

  for (;;) {
    uint32_t sql_len = 0;
    char* sql = NULL;
    char* err = NULL;
    int ok;
    stream_ctx sctx;

    if (!recv_u32(fd, &sql_len)) {
      break;
    }
    sql = (char*)malloc((size_t)sql_len + 1);
    if (sql == NULL) {
      break;
    }
    if (sql_len > 0 && !recv_all(fd, sql, sql_len)) {
      free(sql);
      break;
    }
    sql[sql_len] = '\0';

    if (sql_is_read_only(sql)) {
      flexql_rwlock_rdlock(&g_server.db_lock);
    } else {
      flexql_rwlock_wrlock(&g_server.db_lock);
    }
    
    sctx.fd = fd;
    sctx.pos = 0;
    sctx.col_sent = 0;
    sctx.col_count = 0;
    ok = executor_exec_sql(&g_server.storage, &g_server.cache, sql, stream_callback, &sctx, &err);
    flexql_rwlock_unlock(&g_server.db_lock);

    if (ok) {
      if (!sctx.col_sent) {
        if (!tx_u32(&sctx, 0)) goto end_req;
        if (!tx_u32(&sctx, 0)) goto end_req;
      }
      if (!tx_u32(&sctx, 0)) goto end_req;
    } else {
      if (!sctx.col_sent) {
        if (!tx_u32(&sctx, 1)) goto end_req;
        if (!tx_string(&sctx, err ? err : "unknown error")) goto end_req;
      } else {
        if (!tx_u32(&sctx, 2)) goto end_req;
        if (!tx_string(&sctx, err ? err : "unknown error")) goto end_req;
      }
    }

  end_req:
    tx_flush(&sctx);

    free(sql);
    free(err);
  }

  close(fd);
  return NULL;
}

static void* accept_loop(void* arg) {
  (void)arg;
  while (!g_server.stop_flag) {
    struct sockaddr_in caddr;
    socklen_t clen = sizeof(caddr);
    int cfd = accept(g_server.listen_fd, (struct sockaddr*)&caddr, &clen);
    if (cfd < 0) {
      continue;
    }

    {
      int opt = 1;
      setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    }

    {
      pthread_t th;
      int* fd_ptr = (int*)malloc(sizeof(int));
      if (fd_ptr == NULL) {
        close(cfd);
        continue;
      }
      *fd_ptr = cfd;
      if (pthread_create(&th, NULL, client_worker, fd_ptr) == 0) {
        pthread_detach(th);
      } else {
        free(fd_ptr);
        close(cfd);
      }
    }
  }
  return NULL;
}

static int ensure_server_started(int port, char** err) {
  struct sockaddr_in addr;

  flexql_mutex_lock(&g_server.state_mu);
  if (g_server.started) {
    flexql_mutex_unlock(&g_server.state_mu);
    return 1;
  }

  g_server.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (g_server.listen_fd < 0) {
    flexql_mutex_unlock(&g_server.state_mu);
    if (err) *err = errno_message("socket create failed");
    return 0;
  }

  {
    int opt = 1;
    setsockopt(g_server.listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
  }

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons((uint16_t)port);

  if (bind(g_server.listen_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0 ||
      listen(g_server.listen_fd, 64) != 0) {
    close(g_server.listen_fd);
    g_server.listen_fd = -1;
    flexql_mutex_unlock(&g_server.state_mu);
    if (err) *err = errno_message("bind/listen failed");
    return 0;
  }

  g_server.port = port;
  g_server.stop_flag = 0;
  if (pthread_create(&g_server.accept_thread, NULL, accept_loop, NULL) != 0) {
    close(g_server.listen_fd);
    g_server.listen_fd = -1;
    flexql_mutex_unlock(&g_server.state_mu);
    if (err) *err = fx_strdup("accept thread create failed");
    return 0;
  }
  pthread_detach(g_server.accept_thread);
  g_server.started = 1;
  flexql_mutex_unlock(&g_server.state_mu);
  return 1;
}

int server_runtime_start(const char* host, int port, char** err) {
  if (host == NULL) {
    if (err) *err = fx_strdup("invalid host");
    return 0;
  }

  if (!g_server.initialized) {
    memset(&g_server, 0, sizeof(g_server));
    g_server.listen_fd = -1;
    storage_init(&g_server.storage, "data/tables", "data/wal");
    cache_init(&g_server.cache, 16);
    if (!executor_recover_wal(&g_server.storage, &g_server.cache, err)) {
      cache_free(&g_server.cache);
      storage_free(&g_server.storage);
      return 0;
    }
    flexql_mutex_init(&g_server.state_mu);
    flexql_rwlock_init(&g_server.db_lock);
    g_server.initialized = 1;
  }

  if (!fx_ieq(host, "127.0.0.1") && !fx_ieq(host, "localhost")) {
    if (err) *err = fx_strdup("only loopback host is supported");
    return 0;
  }

  return ensure_server_started(port, err);
}

void server_runtime_stop(void) {
  if (!g_server.initialized) {
    return;
  }

  flexql_mutex_lock(&g_server.state_mu);
  g_server.stop_flag = 1;
  if (g_server.listen_fd >= 0) {
    close(g_server.listen_fd);
    g_server.listen_fd = -1;
  }
  g_server.started = 0;
  flexql_mutex_unlock(&g_server.state_mu);
}

int server_client_open(const char* host, int port, int* sock_fd, char** err) {
  struct sockaddr_in addr;
  int fd;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);
  if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  }

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    if (err) *err = errno_message("socket create failed");
    return 0;
  }
  {
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
  }
  if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
    *sock_fd = fd;
    return 1;
  }
  close(fd);
  if (err) *err = errno_message("connect failed");
  return 0;
}

void server_client_close(int sock_fd) {
  if (sock_fd >= 0) {
    close(sock_fd);
  }
}

typedef struct {
  int fd;
  char buf[8192];
  size_t pos;
  size_t end;
} rx_buf;

static int rx_read(rx_buf* rx, void* data, size_t len) {
  char* dst = (char*)data;
  while (len > 0) {
    if (rx->pos >= rx->end) {
      ssize_t n = recv(rx->fd, rx->buf, sizeof(rx->buf), 0);
      if (n <= 0) return 0;
      rx->pos = 0;
      rx->end = (size_t)n;
    }
    size_t avail = rx->end - rx->pos;
    size_t copy = len < avail ? len : avail;
    memcpy(dst, rx->buf + rx->pos, copy);
    dst += copy;
    rx->pos += copy;
    len -= copy;
  }
  return 1;
}

static int rx_u32(rx_buf* rx, uint32_t* out) {
  uint32_t n = 0;
  if (!rx_read(rx, &n, sizeof(n))) return 0;
  *out = ntohl(n);
  return 1;
}

static char* rx_string(rx_buf* rx) {
  uint32_t len = 0;
  char* s;
  if (!rx_u32(rx, &len)) return NULL;
  s = (char*)malloc(len + 1);
  if (s == NULL) return NULL;
  if (len > 0 && !rx_read(rx, s, len)) { free(s); return NULL; }
  s[len] = '\0';
  return s;
}

int server_client_exec(int sock_fd, const char* sql, flexql_callback callback, void* arg, char** err) {
  uint32_t status = 0;
  uint32_t col_count = 0;
  uint32_t i;
  uint32_t j;
  char** cols = NULL;

  rx_buf rx;
  rx.fd = sock_fd;
  rx.pos = 0;
  rx.end = 0;

  if (!send_u32(sock_fd, (uint32_t)strlen(sql)) || !send_all(sock_fd, sql, strlen(sql))) {
    if (err) *err = fx_strdup("send failed");
    return 0;
  }

  if (!rx_u32(&rx, &status)) {
    if (err) *err = fx_strdup("recv failed");
    return 0;
  }

  if (status != 0) {
    char* em = rx_string(&rx);
    if (err) {
      *err = em ? em : fx_strdup("server error");
    } else {
      free(em);
    }
    return 0;
  }

  if (!rx_u32(&rx, &col_count)) {
    if (err) *err = fx_strdup("recv failed");
    return 0;
  }

  cols = (char**)calloc(col_count, sizeof(char*));
  if (cols == NULL && col_count != 0) {
    if (err) *err = fx_strdup("out of memory");
    return 0;
  }

  for (i = 0; i < col_count; ++i) {
    cols[i] = rx_string(&rx);
    if (cols[i] == NULL) {
      if (err) *err = fx_strdup("recv failed");
      goto fail;
    }
  }

  for (;;) {
    uint32_t has_row = 0;
    if (!rx_u32(&rx, &has_row)) {
      if (err) *err = fx_strdup("recv failed");
      goto fail;
    }
    if (has_row == 0) {
      break;
    }
    if (has_row == 2) {
      char* em = rx_string(&rx);
      if (err) *err = em ? em : fx_strdup("server error");
      goto fail;
    }

    char** row = (char**)calloc(col_count, sizeof(char*));
    if (row == NULL && col_count != 0) {
      if (err) *err = fx_strdup("out of memory");
      goto fail;
    }
    for (j = 0; j < col_count; ++j) {
      row[j] = rx_string(&rx);
      if (row[j] == NULL) {
        if (err) *err = fx_strdup("recv failed");
        while (j > 0) {
          --j;
          free(row[j]);
        }
        free(row);
        goto fail;
      }
    }

    if (callback != NULL && callback(arg, (int)col_count, row, cols) != 0) {
      for (j = 0; j < col_count; ++j) free(row[j]);
      free(row);
      break;
    }

    for (j = 0; j < col_count; ++j) free(row[j]);
    free(row);
  }

  for (i = 0; i < col_count; ++i) free(cols[i]);
  free(cols);
  return 1;

fail:
  for (i = 0; i < col_count; ++i) free(cols[i]);
  free(cols);
  return 0;
}
