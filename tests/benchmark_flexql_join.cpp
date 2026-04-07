#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_ROWS = 100000LL;
static const int DEFAULT_CLIENTS = 4;
static const int INSERT_BATCH_SIZE = 1000;

struct QueryStats {
    long long rows = 0;
};

static void maybe_print_insert_progress(const string &label, long long current_rows,
                                        long long target_rows, int &next_progress_mark) {
    while (target_rows > 0 && next_progress_mark <= 10 &&
           current_rows >= (target_rows * next_progress_mark) / 10) {
        cout << label << " insert progress: " << (next_progress_mark * 10) << "% ("
             << current_rows << "/" << target_rows << " rows)\n";
        next_progress_mark++;
    }
}

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc;
    (void)argv;
    (void)azColName;
    QueryStats *stats = static_cast<QueryStats*>(data);
    if (stats) {
        stats->rows++;
    }
    return 0;
}

static bool open_db(FlexQL **db) {
    return flexql_open("127.0.0.1", 9000, db) == FLEXQL_OK;
}

static bool run_exec(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool load_big_users(FlexQL *db, long long rows) {
    int next_progress_mark = 1;

    if (!run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_USERS")) {
        return false;
    }
    if (!run_exec(db, "DELETE FROM BIG_USERS;", "DELETE BIG_USERS")) {
        return false;
    }

    for (long long inserted = 0; inserted < rows; ) {
        stringstream ss;
        ss << "INSERT INTO BIG_USERS VALUES ";
        int in_batch = 0;
        while (in_batch < INSERT_BATCH_SIZE && inserted < rows) {
            long long id = inserted + 1;
            if (in_batch > 0) {
                ss << ",";
            }
            ss << "(" << id
               << ", 'user" << id << "'"
               << ", 'user" << id << "@mail.com'"
               << ", " << (1000.0 + (id % 10000))
               << ", 1893456000)";
            inserted++;
            in_batch++;
        }
        ss << ";";
        if (!run_exec(db, ss.str(), "INSERT BIG_USERS batch")) {
            return false;
        }

        maybe_print_insert_progress("BIG_USERS", inserted, rows, next_progress_mark);
    }

    return true;
}

static bool load_big_orders(FlexQL *db, long long rows) {
    int next_progress_mark = 1;

    if (!run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS BIG_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_ORDERS")) {
        return false;
    }
    if (!run_exec(db, "DELETE FROM BIG_ORDERS;", "DELETE BIG_ORDERS")) {
        return false;
    }

    for (long long inserted = 0; inserted < rows; ) {
        stringstream ss;
        ss << "INSERT INTO BIG_ORDERS VALUES ";
        int in_batch = 0;
        while (in_batch < INSERT_BATCH_SIZE && inserted < rows) {
            long long id = inserted + 1;
            if (in_batch > 0) {
                ss << ",";
            }
            ss << "(" << id
               << ", " << id
               << ", " << (500.0 + (id % 5000))
               << ", 1893456000)";
            inserted++;
            in_batch++;
        }
        ss << ";";
        if (!run_exec(db, ss.str(), "INSERT BIG_ORDERS batch")) {
            return false;
        }

        maybe_print_insert_progress("BIG_ORDERS", inserted, rows, next_progress_mark);
    }

    return true;
}

static bool run_join_benchmark(long long rows, int client_count) {
    atomic<long long> total_rows{0};
    atomic<bool> failed{false};
    mutex err_mu;
    string first_error;
    vector<thread> workers;
    auto bench_start = high_resolution_clock::now();

    for (int i = 0; i < client_count; ++i) {
        workers.emplace_back([&, i]() {
            FlexQL *db = nullptr;
            if (!open_db(&db)) {
                lock_guard<mutex> lock(err_mu);
                failed = true;
                if (first_error.empty()) {
                    first_error = "client open failed";
                }
                return;
            }

            long long min_amount = 1000 + i * 250;
            stringstream ss;
            ss << "SELECT BIG_USERS.ID, BIG_USERS.NAME, BIG_ORDERS.AMOUNT "
               << "FROM BIG_USERS INNER JOIN BIG_ORDERS ON BIG_USERS.ID = BIG_ORDERS.USER_ID "
               << "WHERE BIG_ORDERS.AMOUNT > " << min_amount << ";";

            QueryStats stats;
            char *errMsg = nullptr;
            if (flexql_exec(db, ss.str().c_str(), count_rows_callback, &stats, &errMsg) != FLEXQL_OK) {
                lock_guard<mutex> lock(err_mu);
                failed = true;
                if (first_error.empty()) {
                    first_error = errMsg ? errMsg : "unknown error";
                }
                if (errMsg) {
                    flexql_free(errMsg);
                }
                flexql_close(db);
                return;
            }

            total_rows += stats.rows;
            flexql_close(db);
        });
    }

    for (auto &worker : workers) {
        worker.join();
    }

    auto bench_end = high_resolution_clock::now();
    if (failed.load()) {
        cout << "[FAIL] INNER JOIN benchmark -> " << first_error << "\n";
        return false;
    }

    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    cout << "[PASS] INNER JOIN benchmark"
         << " | clients=" << client_count
         << " | total_rows=" << total_rows.load()
         << " | elapsed=" << elapsed << " ms\n";
    return true;
}

int main(int argc, char **argv) {
    long long rows = DEFAULT_ROWS;
    int client_count = DEFAULT_CLIENTS;
    FlexQL *db = nullptr;

    if (argc > 1) {
        rows = atoll(argv[1]);
        if (rows <= 0) {
            cout << "Invalid row count.\n";
            return 1;
        }
    }
    if (argc > 2) {
        client_count = atoi(argv[2]);
        if (client_count <= 0) {
            cout << "Invalid client count.\n";
            return 1;
        }
    }

    if (!open_db(&db)) {
        cout << "Cannot open FlexQL. Start ./bin/flexql_server 9000 first.\n";
        return 1;
    }

    cout << "Connected to FlexQL server\n";
    cout << "Preparing join benchmark data for " << rows << " rows\n";
    cout << "Client count: " << client_count << "\n\n";

    if (!load_big_users(db, rows) || !load_big_orders(db, rows)) {
        flexql_close(db);
        return 1;
    }

    if (!run_join_benchmark(rows, client_count)) {
        flexql_close(db);
        return 1;
    }

    flexql_close(db);
    return 0;
}
