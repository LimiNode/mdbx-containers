#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_LOGICAL_DELIVERY_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_LOGICAL_DELIVERY_STORE_HPP_INCLUDED

/// \file LogicalDeliveryStore.hpp
/// \brief Persistent replay marker store for logical delivery envelopes.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include "../../common/MdbxException.hpp"
#include "../../detail/utils.hpp"
#include "../LogicalChangeFrameCodec.hpp"
#include "../LogicalDeliveryEnvelope.hpp"
#include "../common.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Tracks applied logical delivery envelopes.
    /// \details Key = fixed-size origin node id + origin sequence + frame id
    /// digest. Value = full delivery identity, destination database id, and
    /// canonical encoded logical frame bytes.
    /// The caller writes this marker in the same MDBX transaction that applies
    /// the logical mutations, making duplicate delivery detection atomic with
    /// apply rollback/commit. Reusing the same delivery key with different
    /// frame content fails closed as an identity conflict.
    class LogicalDeliveryStore {
    public:
        explicit LogicalDeliveryStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_logical_delivery")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        /// \brief Opens the store DBI inside the supplied transaction.
        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "LogicalDeliveryStore::open");
            open_checked(txn);
        }

        bool is_open() const { return m_open; }

        MDBX_dbi handle(MDBX_txn* txn) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::handle");
            open_checked(txn);
            return m_dbi;
        }

        void reset_open() { m_open = false; }

        void ensure_open() const {
            if (!m_open) {
                throw std::logic_error("LogicalDeliveryStore is not open");
            }
        }

        /// \brief Returns true when \p envelope already has an applied marker.
        bool contains(MDBX_txn* txn,
                      const LogicalDeliveryEnvelope& envelope,
                      const CodecBounds* bounds = nullptr) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::contains");
            open_const(txn);
            const std::vector<std::uint8_t> key =
                make_key(envelope, bounds);
            MDBX_val k = {
                const_cast<std::uint8_t*>(key.empty() ? nullptr : &key[0]),
                key.size()
            };
            MDBX_val v;
            const int rc = mdbx_get(txn, m_dbi, &k, &v);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "LogicalDeliveryStore read failed");
            assert_marker_value_matches(envelope, v, bounds);
            return true;
        }

        /// \brief Inserts an applied marker.
        /// \return false when the exact delivery key already exists.
        bool try_mark_applied(MDBX_txn* txn,
                              const LogicalDeliveryEnvelope& envelope,
                              const CodecBounds* bounds = nullptr) {
            txn = checked_txn(txn, "LogicalDeliveryStore::try_mark_applied");
            open(txn);
            const std::vector<std::uint8_t> key =
                make_key(envelope, bounds);
            MDBX_val k = {
                const_cast<std::uint8_t*>(key.empty() ? nullptr : &key[0]),
                key.size()
            };
            const std::vector<std::uint8_t> value =
                make_marker_value(envelope, bounds);
            MDBX_val v = {
                const_cast<std::uint8_t*>(
                    value.empty() ? nullptr : &value[0]),
                value.size()
            };
            const int rc = mdbx_put(txn, m_dbi, &k, &v, MDBX_NOOVERWRITE);
            if (rc == MDBX_KEYEXIST) {
                MDBX_val existing;
                check_mdbx(mdbx_get(txn, m_dbi, &k, &existing),
                           "LogicalDeliveryStore duplicate read failed");
                assert_marker_value_matches(envelope, existing, bounds);
                return false;
            }
            check_mdbx(rc, "LogicalDeliveryStore write failed");
            return true;
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void open_const(MDBX_txn* txn) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::open");
            open_checked(txn);
        }

        void open_checked(MDBX_txn* txn) const {
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open LogicalDeliveryStore DBI");
            m_open = true;
        }

        static std::vector<std::uint8_t> make_key(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds) {
            validate_logical_delivery_envelope(envelope, bounds);
            if (envelope.frame_id.size() >
                static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "Logical delivery frame id length exceeds u32");
            }
            const std::vector<std::uint8_t> frame_id_digest =
                make_frame_id_digest(envelope.frame_id);
            std::vector<std::uint8_t> key;
            key.reserve(2u + envelope.origin_node_id.size() + 8u +
                        frame_id_digest.size());
            detail::append_u16_le(key, marker_key_version());
            key.insert(key.end(),
                       envelope.origin_node_id.begin(),
                       envelope.origin_node_id.end());
            detail::append_u64_le(key, envelope.origin_sequence);
            key.insert(key.end(), frame_id_digest.begin(),
                       frame_id_digest.end());
            return key;
        }

        static std::vector<std::uint8_t> make_marker_value(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds) {
            validate_logical_delivery_envelope(envelope, bounds);
            const std::vector<std::uint8_t> frame_bytes =
                LogicalChangeFrameCodec::encode(envelope.frame, bounds);
            if (frame_bytes.size() >
                static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "Logical delivery frame bytes exceed u32");
            }
            if (envelope.frame_id.size() >
                static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "Logical delivery frame id length exceeds u32");
            }
            std::vector<std::uint8_t> out;
            out.reserve(2u + envelope.destination_db_uuid.size() +
                        envelope.origin_node_id.size() + 8u + 4u +
                        envelope.frame_id.size() + 2u + 4u +
                        frame_bytes.size());
            detail::append_u16_le(out, marker_value_version());
            out.insert(out.end(),
                       envelope.destination_db_uuid.begin(),
                       envelope.destination_db_uuid.end());
            out.insert(out.end(),
                       envelope.origin_node_id.begin(),
                       envelope.origin_node_id.end());
            detail::append_u64_le(out, envelope.origin_sequence);
            detail::append_u32_le(
                out, static_cast<std::uint32_t>(envelope.frame_id.size()));
            out.insert(out.end(),
                       envelope.frame_id.begin(),
                       envelope.frame_id.end());
            detail::append_u16_le(
                out, LogicalChangeFrameCodec::codec_version());
            detail::append_u32_le(
                out, static_cast<std::uint32_t>(frame_bytes.size()));
            out.insert(out.end(), frame_bytes.begin(), frame_bytes.end());
            return out;
        }

        static void assert_marker_value_matches(
                const LogicalDeliveryEnvelope& envelope,
                const MDBX_val& value,
                const CodecBounds* bounds) {
            const std::vector<std::uint8_t> expected =
                make_marker_value(envelope, bounds);
            if (value.iov_len != expected.size()) {
                throw std::runtime_error(
                    "LogicalDeliveryStore delivery identity conflict");
            }
            if (expected.empty()) {
                return;
            }
            if (std::memcmp(value.iov_base, &expected[0],
                            expected.size()) != 0) {
                throw std::runtime_error(
                    "LogicalDeliveryStore delivery identity conflict");
            }
        }

        static std::vector<std::uint8_t> make_frame_id_digest(
                const std::string& frame_id) {
            std::uint64_t h0 = UINT64_C(0xcbf29ce484222325);
            std::uint64_t h1 = UINT64_C(0x9ae16a3b2f90404f);
            for (std::size_t i = 0; i < frame_id.size(); ++i) {
                const std::uint8_t b =
                    static_cast<std::uint8_t>(frame_id[i]);
                h0 ^= static_cast<std::uint64_t>(b);
                h0 *= UINT64_C(0x100000001b3);
                h1 ^= static_cast<std::uint64_t>(b) +
                      static_cast<std::uint64_t>(i & 0xffu);
                h1 *= UINT64_C(0x100000001b3);
            }
            std::vector<std::uint8_t> out;
            out.reserve(16u);
            detail::append_u64_le(out, h0);
            detail::append_u64_le(out, h1);
            return out;
        }

        static std::uint16_t marker_key_version() { return 1; }

        static std::uint16_t marker_value_version() { return 1; }

        MDBX_env*   m_env;
        std::string m_dbi_name;
        mutable MDBX_dbi m_dbi;
        mutable bool     m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_LOGICAL_DELIVERY_STORE_HPP_INCLUDED
