#include "network/network.h"

int network_validate_endpoint(const char* host, int port) {
  if (host == 0 || host[0] == '\0') {
    return 0;
  }
  if (port < 0 || port > 65535) {
    return 0;
  }
  return 1;
}
