#pragma once
#ifndef MDBX_CONTAINERS_HEADER_DETAIL_COMPOSITE_STORE_TRANSACTION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_DETAIL_COMPOSITE_STORE_TRANSACTION_HPP_INCLUDED

/// \file detail/CompositeStoreTransaction.hpp
/// \brief Shared transaction dispatch for multi-table store operations.

namespace mdbxc {
namespace detail {

    class CompositeStoreTransaction {
    public:
        template<class Fn>
        static void run(Connection& connection,
                        Fn fn,
                        TransactionMode mode,
                        MDBX_txn* txn) {
            if (txn != nullptr) {
                fn(txn);
                return;
            }

            txn = connection.thread_txn();
            if (txn != nullptr) {
                fn(txn);
                return;
            }

            Transaction managed = connection.transaction(mode);
            try {
                fn(managed.handle());
                managed.commit();
            } catch (...) {
                try { managed.rollback(); } catch (...) {}
                throw;
            }
        }
    };

} // namespace detail
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_DETAIL_COMPOSITE_STORE_TRANSACTION_HPP_INCLUDED
