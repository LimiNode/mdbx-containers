#include <mdbx_containers/common.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

bool g_count_allocations = false;
std::size_t g_allocation_count = 0;
std::size_t g_fail_ordinal = 0;

void* allocate_bytes(std::size_t size) {
    if (g_count_allocations) {
        ++g_allocation_count;
        if (g_fail_ordinal != 0 && g_allocation_count == g_fail_ordinal) {
            throw std::bad_alloc();
        }
    }
    void* result = std::malloc(size == 0 ? 1 : size);
    if (result == nullptr) {
        throw std::bad_alloc();
    }
    return result;
}

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

struct AllocationResult {
    std::size_t count;
    bool failed;
};

template<class Action>
AllocationResult measure(Action action, std::size_t fail_ordinal = 0) {
    g_allocation_count = 0;
    g_fail_ordinal = fail_ordinal;
    g_count_allocations = true;
    bool failed = false;
    try {
        action();
    } catch (const std::bad_alloc&) {
        failed = true;
    } catch (...) {
        g_count_allocations = false;
        throw;
    }
    g_count_allocations = false;
    return AllocationResult{g_allocation_count, failed};
}

const char kLongContext[] =
    "Diagnostic context deliberately exceeding every supported standard-library "
    "small-string capacity: a successful MDBX operation must not materialize "
    "this literal as an owning string merely to inspect its return code.";

#if defined(_MSC_VER)
__declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
__attribute__((noinline))
#endif
void check_literal(int result) {
    mdbxc::check_mdbx(result, kLongContext);
}

void check_helpers() {
    const std::string existing(kLongContext);
    const AllocationResult literal = measure([]() { check_literal(MDBX_SUCCESS); });
    std::printf("Successful literal diagnostic allocations: %zu\n", literal.count);
    require(!literal.failed && literal.count == 0,
            "successful literal check_mdbx allocated a diagnostic string");
    const AllocationResult owned = measure([&existing]() {
        mdbxc::check_mdbx(MDBX_SUCCESS, existing);
    });
    require(!owned.failed && owned.count == 0,
            "successful existing-string check_mdbx allocated");
    const AllocationResult injected = measure([]() { check_literal(MDBX_SUCCESS); }, 1);
    require(!injected.failed && injected.count == 0,
            "successful diagnostic path reached injected allocation failure");

    const int expected_code = MDBX_NOTFOUND;
    const std::string expected = std::string("MDBXC error: ") + existing +
        ": (" + std::to_string(expected_code) + ") " + mdbx_strerror(expected_code);
    for (int variant = 0; variant != 2; ++variant) {
        bool caught = false;
        try {
            if (variant == 0) {
                check_literal(expected_code);
            } else {
                mdbxc::check_mdbx(expected_code, existing);
            }
        } catch (const mdbxc::MdbxException& error) {
            caught = error.error_code() == expected_code && error.what() == expected;
        }
        require(caught, "diagnostic overload changed error code or text");
    }
}

class Tracker : public mdbxc::TransactionTracker {
public:
    MDBX_txn* active() const { return thread_txn(); }
    bool handles() const { return has_txn_handles(); }
    bool current_handle() const { return current_thread_has_txn_handle(); }
#if MDBXC_SYNC_ENABLED
    unsigned precommit_calls = 0;
    unsigned discard_calls = 0;
    bool reject_precommit = false;

    void on_pre_commit(MDBX_txn*) override {
        ++precommit_calls;
        if (reject_precommit) {
            throw std::runtime_error("intentional precommit refusal");
        }
    }
    void on_discard(MDBX_txn*) noexcept override { ++discard_calls; }
#endif
};

class Environment {
public:
    Environment() {
        mdbxc::check_mdbx(mdbx_env_create(&m_env), "create diagnostic-test environment");
        try {
            mdbxc::check_mdbx(mdbx_env_set_maxdbs(m_env, 4), "set diagnostic-test DBI budget");
            mdbxc::check_mdbx(mdbx_env_open(m_env, "diagnostics.mdbx", MDBX_NOSUBDIR, 0644),
                              "open diagnostic-test environment");
        } catch (...) {
            mdbx_env_close(m_env);
            throw;
        }
    }
    ~Environment() { mdbx_env_close(m_env); }
    MDBX_env* get() const { return m_env; }
private:
    MDBX_env* m_env = nullptr;
};

void put_marker(MDBX_txn* transaction, const char* key) {
    MDBX_dbi database = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(transaction, nullptr, MDBX_DB_DEFAULTS, &database),
                      "open marker database");
    const char value[] = "retained marker";
    MDBX_val raw_key{};
    raw_key.iov_base = const_cast<char*>(key);
    raw_key.iov_len = std::strlen(key);
    MDBX_val raw_value{};
    raw_value.iov_base = const_cast<char*>(value);
    raw_value.iov_len = sizeof(value);
    mdbxc::check_mdbx(mdbx_put(transaction, database, &raw_key, &raw_value, MDBX_UPSERT),
                      "put diagnostic-test marker");
}

bool has_marker(MDBX_env* environment, const char* key) {
    Tracker tracker;
    mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::READ_ONLY);
    MDBX_dbi database = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(transaction.handle(), nullptr, MDBX_DB_DEFAULTS, &database),
                      "open marker read database");
    MDBX_val raw_key{};
    raw_key.iov_base = const_cast<char*>(key);
    raw_key.iov_len = std::strlen(key);
    MDBX_val value{};
    const int result = mdbx_get(transaction.handle(), database, &raw_key, &value);
    require(result == MDBX_SUCCESS || result == MDBX_NOTFOUND, "marker read failed");
    return result == MDBX_SUCCESS;
}

void require_closed(const Tracker& tracker) {
    require(tracker.active() == nullptr && !tracker.handles() && !tracker.current_handle(),
            "closed transaction retained tracker binding or handle reservation");
}

void check_successful_lifecycle(MDBX_env* environment) {
    Tracker tracker;
    {
        mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::WRITABLE);
        put_marker(transaction.handle(), "committed");
        const AllocationResult commit = measure([&transaction]() { transaction.commit(); }, 1);
        require(!commit.failed && commit.count == 0,
                "successful commit performed a C++ allocation in the diagnostic/tracker path");
        require(transaction.handle() == nullptr, "commit retained a consumed handle");
        require_closed(tracker);
    }
    require(has_marker(environment, "committed"), "successful commit did not persist marker");
    {
        mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::WRITABLE);
        put_marker(transaction.handle(), "rolled-back");
        const AllocationResult rollback = measure([&transaction]() { transaction.rollback(); }, 1);
        require(!rollback.failed && rollback.count == 0, "successful rollback allocated diagnostics");
        require(transaction.handle() == nullptr, "rollback retained a consumed handle");
        require_closed(tracker);
    }
    require(!has_marker(environment, "rolled-back"), "rollback persisted marker");
    {
        mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::READ_ONLY);
        MDBX_txn* handle = transaction.handle();
        const AllocationResult reset = measure([&transaction]() { transaction.commit(); }, 1);
        require(!reset.failed && reset.count == 0, "successful read-only reset allocated diagnostics");
        require(transaction.handle() == handle && tracker.active() == nullptr && tracker.handles(),
                "reset did not preserve its unbound reusable handle");
        transaction.begin();
        require(transaction.handle() == handle && tracker.active() == handle,
                "renew failed to bind the original read-only handle");
        const AllocationResult rollback = measure([&transaction]() { transaction.rollback(); }, 1);
        require(!rollback.failed && rollback.count == 0, "read-only rollback/reset allocated diagnostics");
    }
    require_closed(tracker);
    std::printf("Commit/rollback/read-only reset: zero diagnostic-path allocations (sync=%d)\n",
                MDBXC_SYNC_ENABLED);
}

void check_begin_and_renew_failures(MDBX_env* environment) {
#if defined(_MSC_VER) && _ITERATOR_DEBUG_LEVEL > 0
    (void)environment;
    std::puts("Registration allocation sweep NOT RUN: MSVC checked-STL noexcept proxy construction");
#else
    for (int mode_index = 0; mode_index != 2; ++mode_index) {
        const mdbxc::TransactionMode mode = mode_index == 0 ?
            mdbxc::TransactionMode::WRITABLE : mdbxc::TransactionMode::READ_ONLY;
        Tracker tracker;
        const AllocationResult baseline = measure([&tracker, environment, mode]() {
            mdbxc::Transaction transaction(&tracker, environment, mode);
        });
        require(!baseline.failed, "baseline begin unexpectedly failed");
        require_closed(tracker);
        for (std::size_t ordinal = 1; ordinal <= baseline.count; ++ordinal) {
            Tracker fresh_tracker;
            const AllocationResult injected = measure([&fresh_tracker, environment, mode]() {
                mdbxc::Transaction transaction(&fresh_tracker, environment, mode);
            }, ordinal);
            require(injected.failed, "begin allocation ordinal was not exercised");
            require_closed(fresh_tracker);
            mdbxc::Transaction subsequent(&fresh_tracker, environment, mode);
            subsequent.rollback();
        }
        std::printf("Begin tracker allocation failures (mode=%d): %zu\n", mode_index, baseline.count);
    }
    Tracker baseline_tracker;
    mdbxc::Transaction baseline_transaction(&baseline_tracker, environment, mdbxc::TransactionMode::READ_ONLY);
    baseline_transaction.commit();
    const AllocationResult baseline = measure([&baseline_transaction]() { baseline_transaction.begin(); });
    require(!baseline.failed, "baseline renew unexpectedly failed");
    baseline_transaction.commit();
    for (std::size_t ordinal = 1; ordinal <= baseline.count; ++ordinal) {
        Tracker tracker;
        mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::READ_ONLY);
        transaction.commit();
        const AllocationResult injected = measure([&transaction]() { transaction.begin(); }, ordinal);
        require(injected.failed && tracker.active() == nullptr && tracker.handles(),
                "failed renew lost its handle or retained active binding");
        transaction.begin();
        transaction.rollback();
    }
    std::printf("Renew tracker allocation failures: %zu\n", baseline.count);
#endif
}

void check_commit_error_and_thread_mismatch(MDBX_env* environment) {
    Tracker tracker;
    {
        mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::WRITABLE);
        put_marker(transaction.handle(), "injected-commit-error");
        mdbxc::detail::fail_next_transaction_commit_for_test(MDBX_MAP_FULL);
        bool caught = false;
        try {
            transaction.commit();
        } catch (const mdbxc::MdbxException& error) {
            caught = error.error_code() == MDBX_MAP_FULL;
        }
        require(caught && transaction.handle() == nullptr, "terminal commit error retained its handle");
        require_closed(tracker);
    }
    require(!has_marker(environment, "injected-commit-error"), "injected terminal error persisted marker");
    {
        mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::WRITABLE);
        MDBX_txn* handle = transaction.handle();
        int error_code = MDBX_SUCCESS;
        // Deliberately misuse the guard to verify the existing native rejection.
        // Ownership and destruction stay on this thread.
        std::thread foreign([&transaction, &error_code]() {
            try {
                transaction.commit();
            } catch (const mdbxc::MdbxException& error) {
                error_code = error.error_code();
            }
        });
        foreign.join();
        require(error_code == MDBX_THREAD_MISMATCH && transaction.handle() == handle &&
                tracker.active() == handle && tracker.handles(),
                "THREAD_MISMATCH consumed the owner transaction or its registration");
        transaction.rollback();
    }
    require_closed(tracker);
#if MDBXC_SYNC_ENABLED
    {
        mdbxc::Transaction transaction(&tracker, environment, mdbxc::TransactionMode::WRITABLE);
        MDBX_txn* handle = transaction.handle();
        tracker.reject_precommit = true;
        bool rejected = false;
        try {
            transaction.commit();
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        require(rejected && transaction.handle() == handle && tracker.active() == handle,
                "precommit failure changed native transaction ownership");
        transaction.rollback();
    }
    require_closed(tracker);
    require(tracker.precommit_calls != 0 && tracker.discard_calls != 0,
            "sync precommit/discard hooks were not exercised");
#endif
}

void check_manual_connection() {
    mdbxc::Config config;
    config.pathname = "manual-diagnostics.mdbx";
    config.relative_to_exe = false;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection = mdbxc::Connection::create(config);
    connection->begin(mdbxc::TransactionMode::WRITABLE);
    const std::shared_ptr<mdbxc::Transaction> held = connection->current_txn();
    put_marker(held->handle(), "manual-committed");
    const AllocationResult commit = measure([connection]() { connection->commit(); }, 1);
    require(!commit.failed && commit.count == 0 && held->handle() == nullptr &&
            !connection->current_txn(), "manual commit retained state or diagnostic allocation");
    connection->begin(mdbxc::TransactionMode::WRITABLE);
    const AllocationResult rollback = measure([connection]() { connection->rollback(); }, 1);
    require(!rollback.failed && rollback.count == 0 && !connection->current_txn(),
            "manual rollback retained state or diagnostic allocation");
    connection->disconnect();
    connection->connect();
    require(has_marker(connection->env_handle(), "manual-committed"), "manual commit was not durable");
    connection->disconnect();
}

void check_manual_begin_failures() {
#if defined(_MSC_VER) && _ITERATOR_DEBUG_LEVEL > 0
    std::puts("Manual-begin allocation sweep NOT RUN: MSVC checked-STL noexcept proxy construction");
#else
    mdbxc::Config config;
    config.pathname = "manual-begin-failures.mdbx";
    config.relative_to_exe = false;
    config.no_subdir = true;
    std::size_t allocation_count = 0;
    {
        const std::shared_ptr<mdbxc::Connection> connection = mdbxc::Connection::create(config);
        const AllocationResult baseline = measure([connection]() {
            connection->begin(mdbxc::TransactionMode::WRITABLE);
        });
        require(!baseline.failed, "manual-begin allocation baseline failed");
        allocation_count = baseline.count;
        connection->rollback();
        connection->disconnect();
    }
    for (std::size_t ordinal = 1; ordinal <= allocation_count; ++ordinal) {
        const std::shared_ptr<mdbxc::Connection> connection = mdbxc::Connection::create(config);
        const AllocationResult injected = measure([connection]() {
            connection->begin(mdbxc::TransactionMode::WRITABLE);
        }, ordinal);
        require(injected.failed && !connection->current_txn(),
                "manual-begin failure retained an exposed transaction");
        try {
            connection->disconnect();
        } catch (const mdbxc::MdbxException& error) {
            std::fprintf(stderr, "Manual begin ordinal %zu left tracker reservation: %d\n",
                         ordinal, error.error_code());
            throw;
        }
        connection->connect();
        connection->begin(mdbxc::TransactionMode::WRITABLE);
        connection->rollback();
        connection->disconnect();
    }
    std::printf("Manual-begin allocation failures with clean reconnect: %zu\n", allocation_count);
#endif
}

} // namespace

void* operator new(std::size_t size) { return allocate_bytes(size); }
void* operator new[](std::size_t size) { return allocate_bytes(size); }
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
#if __cplusplus >= 201402L || defined(_MSC_VER)
void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { std::free(pointer); }
#endif

int main() {
    try {
        check_helpers();
        Environment environment;
        check_successful_lifecycle(environment.get());
        check_begin_and_renew_failures(environment.get());
        check_commit_error_and_thread_mismatch(environment.get());
        check_manual_connection();
        check_manual_begin_failures();
        std::puts("PASS diagnostic allocation and transaction lifecycle regression");
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "FAIL: %s\n", error.what());
        return EXIT_FAILURE;
    }
}
