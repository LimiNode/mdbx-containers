#pragma once
#ifndef MDBX_CONTAINERS_HEADER_ANY_VALUE_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_ANY_VALUE_TABLE_HPP_INCLUDED

/// \file AnyValueTable.hpp
/// \brief Table storing values of arbitrary type indexed by key.
/// \details
/// Provides a typed, heterogeneous key-value API where each key stores one
/// serialized value chosen by the caller at the access site.

#include "common.hpp"
#include <limits>
#include <typeinfo>

namespace mdbxc {

    /// \struct AnyValueTypeTag
    /// \ingroup mdbxc_tables
    /// \brief Provides the runtime type tag used by \ref AnyValueTable.
    /// \tparam T Value type.
    /// \details
    /// Specialize this trait for durable, application-defined type tags. The
    /// default tag is implementation-defined and may differ across compilers.
    template<class T>
    struct AnyValueTypeTag {
        /// \brief Returns the type tag for \c T.
        /// \return Implementation-defined default tag.
        static const char* value() noexcept {
            return typeid(T).name();
        }
    };

    /// \class AnyValueTable
    /// \ingroup mdbxc_tables
    /// \brief Table storing values of arbitrary type associated with a key.
    /// \tparam KeyT Type of the key used to access values.
    /// \tparam Options Compile-time table policy. Does not change the database
    ///         storage format.
    /// \details
    /// Provides a key-value table for heterogeneous payloads. The table itself
    /// is templated only on the key type; each read or write names the expected
    /// value type through methods such as \c set<T>(), \c insert<T>(),
    /// \c get<T>(), \c find<T>(), and \c update<T>().
    ///
    /// Each key can hold at most one value. \c set<T>() replaces any existing
    /// value for the key, while \c insert<T>() stores the value only when the key
    /// is absent. \c update<T>() reads a value as \c T, passes it to a functor,
    /// and stores the modified value back under the same key.
    ///
    /// \note \c keys() lists stored keys only; this table does not expose a
    ///       type-erased value enumeration API.
    /// \note Type-tag prefix verification is opt-in through
    ///       \c set_type_tag_check(true). It is disabled by default to preserve
    ///       compatibility with existing raw records.
    template <class KeyT, class Options = DefaultTableOptions>
    class AnyValueTable final : public BaseTable {
    public:
        /// \brief Constructs table using existing connection.
        /// \param conn Shared connection to the environment.
        /// \param name Name of the table.
        /// \param flags Additional MDBX flags.
        AnyValueTable(std::shared_ptr<Connection> conn,
                      std::string name = "any_store",
                      MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(conn), std::move(name), flags | get_mdbx_flags<KeyT>()) {}

        /// \brief Constructs table using configuration.
        /// \param cfg Configuration settings.
        /// \param name Name of the table.
        /// \param flags Additional MDBX flags.
        explicit AnyValueTable(const Config& cfg,
                               std::string name = "any_store",
                               MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(cfg), std::move(name), flags | get_mdbx_flags<KeyT>()) {}

        /// \brief Destructor.
        ~AnyValueTable() override = default;

        // --- Write ---

        /// \brief Set value for key, replacing existing value.
        /// \tparam T Type of value.
        /// \param key Key to update.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        /// \note Replaces the existing value for \p key even if it was written
        ///       through another \c T at the call site.
        template <class T>
        void set(const KeyT& key, const T& value, MDBX_txn* txn = nullptr) {
            with_transaction([this, &key, &value](MDBX_txn* t){
                put_typed(key, value, true, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Set value using external transaction.
        template <class T>
        void set(const KeyT& key, const T& value, const Transaction& txn) {
            set(key, value, txn.handle());
        }

        /// \brief Insert value if key does not exist.
        /// \tparam T Type of value.
        /// \param key Key to insert.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        /// \return \c true if inserted, \c false if key already exists.
        /// \note Existence is checked by key only; stored value type is not part
        ///       of uniqueness.
        template <class T>
        bool insert(const KeyT& key, const T& value, MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, &key, &value, &res](MDBX_txn* t){
                res = put_typed(key, value, false, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Insert value using external transaction.
        template <class T>
        bool insert(const KeyT& key, const T& value, const Transaction& txn) {
            return insert<T>(key, value, txn.handle());
        }

        /// \brief Update value using functor.
        /// \tparam T Expected type of stored value.
        /// \tparam Fn Functor accepting reference to value.
        /// \param key Key to modify.
        /// \param fn Function applied to the value.
        /// \param create_if_missing Create default value if key is absent.
        /// \param txn Optional transaction handle.
        /// \note When \p create_if_missing is true, a default-constructed \c T
        ///       is passed to \p fn and then stored.
        template <class T, class Fn>
        void update(const KeyT& key, Fn&& fn, bool create_if_missing = false, MDBX_txn* txn = nullptr) {
            with_transaction([this, &key, &fn, create_if_missing](MDBX_txn* t){
                T tmp{};
                bool exists = get_typed(key, tmp, t);
                if (!exists) {
                    if (!create_if_missing) {
                        throw std::out_of_range("Key not found");
                    }
                }
                fn(tmp);
                put_typed(key, tmp, true, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Update using external transaction.
        template <class T, class Fn>
        void update(const KeyT& key, Fn&& fn, bool create_if_missing, const Transaction& txn) {
            update<T, Fn>(key, std::forward<Fn>(fn), create_if_missing, txn.handle());
        }

        // --- Read ---

        /// \brief Retrieve stored value or throw if missing.
        /// \param key Key to look up.
        /// \param txn Optional transaction handle.
        /// \throws std::out_of_range if key not found.
        /// \throws std::bad_cast if type tag check fails.
        template <class T>
        T get(const KeyT& key, MDBX_txn* txn = nullptr) const {
            T out{};
            bool found = false;
            with_transaction([this, &key, &out, &found](MDBX_txn* t){
                found = get_typed(key, out, t);
            }, TransactionMode::READ_ONLY, txn);
            if (!found) {
                throw std::out_of_range("Key not found");
            }
            return out;
        }

        /// \brief Retrieve using external transaction.
        template <class T>
        T get(const KeyT& key, const Transaction& txn) const {
            return get<T>(key, txn.handle());
        }

        /// \brief Find value by key.
        /// \tparam T Expected value type.
        /// \param key Key to search for.
        /// \param txn Optional transaction handle.
        /// \return Optional with value or std::nullopt.
        /// \note If type-tag verification reports a mismatch, the value is
        ///       treated as missing.
#       if __cplusplus >= 201703L
        template <class T>
        std::optional<T> find(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::optional<T> result;
            with_transaction([this, &key, &result](MDBX_txn* t){
                T tmp{};
                try {
                    if (get_typed(key, tmp, t)) {
                        result = std::move(tmp);
                    }
                } catch (const std::bad_cast&) {
                    // type mismatch -> treat as not found
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Find using external transaction.
        template <class T>
        std::optional<T> find(const KeyT& key, const Transaction& txn) const {
            return find<T>(key, txn.handle());
        }

        /// \brief Get value or default if missing.
        /// \param key Key to look up.
        /// \param default_value Value returned when key not found.
        /// \param txn Optional transaction handle.
        /// \return Stored value or \p default_value when the key is absent.
        template <class T>
        T get_or(const KeyT& key, T default_value, MDBX_txn* txn = nullptr) const {
            if (auto val = find<T>(key, txn)) {
                return *std::move(val);
            }
            return default_value;
        }

        /// \brief Get value or default using external transaction.
        /// \param key Key to look up.
        /// \param default_value Value returned when key not found.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or \p default_value when the key is absent.
        template <class T>
        T get_or(const KeyT& key, T default_value, const Transaction& txn) const {
            return get_or<T>(key, std::move(default_value), txn.handle());
        }
#       else
        /// \brief Find value by key returning C++11-compatible result.
        /// \tparam T Expected value type.
        /// \param key Key to search for.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and retrieved value.
        /// \note If type-tag verification reports a mismatch, the value is
        ///       treated as missing.
        template <class T>
        std::pair<bool, T> find_compat(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::pair<bool, T> result{false, T{}};
            with_transaction([this, &key, &result](MDBX_txn* t){
                try {
                    if (get_typed(key, result.second, t)) {
                        result.first = true;
                    }
                } catch (const std::bad_cast&) {
                    // type mismatch -> treat as not found
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Find value by key using external transaction (C++11 mode).
        /// \tparam T Expected value type.
        /// \param key Key to search for.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and retrieved value.
        template <class T>
        std::pair<bool, T> find_compat(const KeyT& key, const Transaction& txn) const {
            return find_compat<T>(key, txn.handle());
        }

        /// \brief Get value or default if missing (C++11 mode).
        /// \tparam T Expected value type.
        /// \param key Key to look up.
        /// \param default_value Value returned when key not found.
        /// \param txn Optional transaction handle.
        /// \return Stored value or \p default_value when the key is absent.
        template <class T>
        T get_or(const KeyT& key, T default_value, MDBX_txn* txn = nullptr) const {
            auto res = find_compat<T>(key, txn);
            if (res.first) {
                return std::move(res.second);
            }
            return default_value;
        }

        /// \brief Get value or default using external transaction (C++11 mode).
        /// \tparam T Expected value type.
        /// \param key Key to look up.
        /// \param default_value Value returned when key not found.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or \p default_value when the key is absent.
        template <class T>
        T get_or(const KeyT& key, T default_value, const Transaction& txn) const {
            return get_or<T>(key, std::move(default_value), txn.handle());
        }
#       endif

        // --- Meta ---

        /// \brief Check if key exists.
        /// \tparam KT Key type (defaults to \p KeyT).
        /// \param key Key to check.
        /// \param txn Optional transaction handle.
        /// \return \c true if key is present, otherwise \c false.
        template <class KT = KeyT>
        bool contains(const KT& key, MDBX_txn* txn = nullptr) const {
            bool res = false;
            with_transaction([this, &key, &res](MDBX_txn* t) {
                res = db_contains(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Check key existence using external transaction.
        /// \param key Key to check.
        /// \param txn Active transaction wrapper.
        /// \return \c true if key is present, otherwise \c false.
        bool contains(const KeyT& key, const Transaction& txn) const {
            return contains(key, txn.handle());
        }

        /// \brief Erase key from table.
        /// \param key Key to remove.
        /// \param txn Optional transaction handle.
        /// \return \c true if key was removed, otherwise \c false.
        bool erase(const KeyT& key, MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, &key, &res](MDBX_txn* t) {
                res = db_erase(key, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Erase using external transaction.
        /// \param key Key to remove.
        /// \param txn Active transaction wrapper.
        /// \return \c true if key was removed, otherwise \c false.
        bool erase(const KeyT& key, const Transaction& txn) {
            return erase(key, txn.handle());
        }

        /// \brief List all keys stored in table.
        /// \param txn Optional transaction handle.
        /// \return Vector containing every key stored in the table.
        /// \note Key order follows MDBX key ordering, not insertion order.
        std::vector<KeyT> keys(MDBX_txn* txn = nullptr) const {
            std::vector<KeyT> out;
            with_transaction([this, &out](MDBX_txn* t) {
                db_list_keys(out, t);
            }, TransactionMode::READ_ONLY, txn);
            return out;
        }

        /// \brief List keys using external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Vector containing every key stored in the table.
        std::vector<KeyT> keys(const Transaction& txn) const {
            return keys(txn.handle());
        }

        /// \brief Enable or disable type-tag checking.
        /// \param enabled Enables check when set to true.
        /// \note When enabled, newly written values are stored with a type-tag
        ///       prefix and reads validate that prefix before deserialization.
        ///       Existing raw records behave as type mismatches while this flag
        ///       is enabled.
        void set_type_tag_check(bool enabled) noexcept { m_check_type_tag = enabled; }

    private:
        bool m_check_type_tag = false; ///< Flag enabling type-tag verification.

        template <class T>
        bool put_typed(const KeyT& key, const T& value, bool upsert, MDBX_txn* txn) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            SerializeScratch sc_tagged_value;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val raw_val = serialize_value(value, sc_value);
            MDBX_val db_val = wrap_with_type_tag<T>(raw_val, sc_tagged_value);
            MDBX_put_flags_t flags = upsert ? MDBX_UPSERT : MDBX_NOOVERWRITE;
            int rc = mdbx_put(txn, m_dbi, &db_key, &db_val, flags);
            if (!upsert && rc == MDBX_KEYEXIST) {
                return false;
            }
            check_mdbx(rc, upsert ? "Failed to set value" : "Failed to insert value");
            return true;
        }

        template <class T>
        bool get_typed(const KeyT& key, T& out, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val{};
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to retrieve value");
            MDBX_val checked = unwrap_and_check_type_tag<T>(db_val);
            out = deserialize_value<T>(checked);
            return true;
        }

        bool db_contains(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to check key presence");
            return false;
        }

        bool db_erase(const KeyT& key, MDBX_txn* txn) {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            int rc = mdbx_del(txn, m_dbi, &db_key, nullptr);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to erase key");
            return false;
        }

        void db_list_keys(std::vector<KeyT>& out, MDBX_txn* txn) const {
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor), "Failed to open MDBX cursor");
            try {
                MDBX_val db_key, db_val;
                int rc = MDBX_SUCCESS;
                while ((rc = mdbx_cursor_get(cursor, &db_key, &db_val, MDBX_NEXT)) == MDBX_SUCCESS) {
                    out.emplace_back(deserialize_key<KeyT>(db_key));
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "Failed to list AnyValueTable keys");
                }
                mdbx_cursor_close(cursor);
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
        }

        template <class T>
        MDBX_val wrap_with_type_tag(const MDBX_val& raw, SerializeScratch& sc) const {
            if (!m_check_type_tag) return raw;

            const std::string tag = type_tag<T>();
            if (tag.size() > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error("AnyValueTable type tag is too large");
            }

            sc.bytes.clear();
            sc.bytes.reserve(type_tag_header_size() + tag.size() + raw.iov_len);
            append_magic(sc.bytes);
            append_le32(sc.bytes, static_cast<std::uint32_t>(tag.size()));
            sc.bytes.insert(sc.bytes.end(), tag.begin(), tag.end());
            if (raw.iov_len) {
                const std::uint8_t* payload = static_cast<const std::uint8_t*>(raw.iov_base);
                sc.bytes.insert(sc.bytes.end(), payload, payload + raw.iov_len);
            }
            return sc.view_bytes();
        }

        template <class T>
        MDBX_val unwrap_and_check_type_tag(const MDBX_val& raw) const {
            if (!m_check_type_tag) return raw;

            if (raw.iov_len < type_tag_header_size() || !raw.iov_base) {
                throw std::bad_cast();
            }

            const std::uint8_t* data = static_cast<const std::uint8_t*>(raw.iov_base);
            if (std::memcmp(data, type_tag_magic(), type_tag_magic_size()) != 0) {
                throw std::bad_cast();
            }

            const std::uint32_t tag_size = read_le32(data + type_tag_magic_size());
            const std::size_t payload_offset = type_tag_header_size() + tag_size;
            if (tag_size > raw.iov_len - type_tag_header_size()) {
                throw std::bad_cast();
            }

            const std::string expected = type_tag<T>();
            if (expected.size() != tag_size ||
                std::memcmp(data + type_tag_header_size(), expected.data(), tag_size) != 0) {
                throw std::bad_cast();
            }

            MDBX_val payload;
            payload.iov_len = raw.iov_len - payload_offset;
            payload.iov_base = payload.iov_len
                ? const_cast<std::uint8_t*>(data + payload_offset)
                : nullptr;
            return payload;
        }

        static const char* type_tag_magic() {
            return "MDBXCAV1";
        }

        static std::size_t type_tag_magic_size() {
            return 8u;
        }

        static std::size_t type_tag_header_size() {
            return type_tag_magic_size() + 4u;
        }

        static void append_magic(std::vector<std::uint8_t>& out) {
            const char* magic = type_tag_magic();
            out.insert(out.end(), magic, magic + type_tag_magic_size());
        }

        static void append_le32(std::vector<std::uint8_t>& out, std::uint32_t value) {
            out.push_back(static_cast<std::uint8_t>(value & 0xffu));
            out.push_back(static_cast<std::uint8_t>((value >> 8) & 0xffu));
            out.push_back(static_cast<std::uint8_t>((value >> 16) & 0xffu));
            out.push_back(static_cast<std::uint8_t>((value >> 24) & 0xffu));
        }

        static std::uint32_t read_le32(const std::uint8_t* data) {
            return static_cast<std::uint32_t>(data[0]) |
                   (static_cast<std::uint32_t>(data[1]) << 8) |
                   (static_cast<std::uint32_t>(data[2]) << 16) |
                   (static_cast<std::uint32_t>(data[3]) << 24);
        }

        template <class T>
        static std::string type_tag() {
            return std::string(AnyValueTypeTag<T>::value());
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_ANY_VALUE_TABLE_HPP_INCLUDED
