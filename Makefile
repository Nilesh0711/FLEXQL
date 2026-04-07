CC := gcc
CXX := g++
CFLAGS := -D_XOPEN_SOURCE=700 -std=c11 -O2 -Wall -Wextra -pedantic
CXXFLAGS := -D_XOPEN_SOURCE=700 -std=c++17 -O2 -Wall -Wextra -pedantic
INCLUDES := -Iinclude -Iinclude/common

BUILD_DIR := build
BIN_DIR := bin

LIB_OBJS := \
	$(BUILD_DIR)/str_utils.o \
	$(BUILD_DIR)/lock.o \
	$(BUILD_DIR)/network.o \
	$(BUILD_DIR)/expiration.o \
	$(BUILD_DIR)/index.o \
	$(BUILD_DIR)/storage.o \
	$(BUILD_DIR)/cache.o \
	$(BUILD_DIR)/parser.o \
	$(BUILD_DIR)/executor.o \
	$(BUILD_DIR)/server_runtime.o \
	$(BUILD_DIR)/flexql_api.o

LIB_A := $(BIN_DIR)/libflexql.a
LIB_SO := $(BIN_DIR)/libflexql.so
REPL_BIN := $(BIN_DIR)/flexql_repl
SERVER_BIN := $(BIN_DIR)/flexql_server
TEST_BIN := $(BIN_DIR)/test_driver
BENCH_BIN := $(BIN_DIR)/benchmark_flexql
JOIN_BENCH_BIN := $(BIN_DIR)/benchmark_flexql_join
AFTER_INSERT_BENCH_BIN := $(BIN_DIR)/benchmark_after_insert

all: $(LIB_A) $(LIB_SO) $(REPL_BIN) $(SERVER_BIN) $(TEST_BIN) $(BENCH_BIN) $(JOIN_BENCH_BIN) $(AFTER_INSERT_BENCH_BIN)

$(BUILD_DIR) $(BIN_DIR):
	mkdir -p $@

$(BUILD_DIR)/str_utils.o: src/utils/str_utils.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/lock.o: src/concurrency/lock.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/network.o: src/network/network.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/expiration.o: src/expiration/expiration.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/index.o: src/index/index.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/storage.o: src/storage/storage.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/cache.o: src/cache/cache.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/parser.o: src/parser/parser.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/executor.o: src/query/executor.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/server_runtime.o: src/server/server.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(BUILD_DIR)/flexql_api.o: src/server/flexql_api.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -fPIC -c $< -o $@

$(LIB_A): $(LIB_OBJS) | $(BIN_DIR)
	ar rcs $@ $^

$(LIB_SO): $(LIB_OBJS) | $(BIN_DIR)
	$(CC) -shared -o $@ $^ -lpthread

$(REPL_BIN): src/client/repl.c $(LIB_A) include/common/flexql.h | $(BIN_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) $< $(LIB_A) -lpthread -o $@

$(SERVER_BIN): src/server/server_main.c $(LIB_A) include/server/server.h | $(BIN_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) $< $(LIB_A) -lpthread -o $@

$(TEST_BIN): tests/test_driver.c $(LIB_A) include/common/flexql.h | $(BIN_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) $< $(LIB_A) -lpthread -o $@

$(BENCH_BIN): tests/benchmark_flexql.cpp $(LIB_A) include/flexql.h | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< $(LIB_A) -lpthread -o $@

$(JOIN_BENCH_BIN): tests/benchmark_flexql_join.cpp $(LIB_A) include/flexql.h | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< $(LIB_A) -lpthread -o $@

$(AFTER_INSERT_BENCH_BIN): tests/benchmark_after_insert.cpp $(LIB_A) include/flexql.h | $(BIN_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< $(LIB_A) -lpthread -o $@

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)

.PHONY: all clean
