#pragma once
#ifndef MDBX_CONTAINERS_HEADER_RAG_RECORDS_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_RAG_RECORDS_HPP_INCLUDED

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace mdbxc {

    struct Document {
        std::uint64_t id = 0u;
        std::string source_uri;
        std::string title;
        std::string source_type;
        std::int64_t created_at_ms = 0;
        std::int64_t indexed_at_ms = 0;
        std::string metadata_json;
    };

    struct Chunk {
        std::uint64_t id = 0u;
        std::uint64_t document_id = 0u;
        std::uint32_t chunk_index = 0u;
        std::uint32_t char_start = 0u;
        std::uint32_t char_end = 0u;
        std::string text;
        std::string metadata_json;
    };

    namespace detail {
        inline void rag_append_u32(std::vector<std::uint8_t>& out, std::uint32_t value) {
            for (unsigned int shift = 0u; shift != 32u; shift += 8u) {
                out.push_back(static_cast<std::uint8_t>(value >> shift));
            }
        }

        inline void rag_append_u64(std::vector<std::uint8_t>& out, std::uint64_t value) {
            for (unsigned int shift = 0u; shift != 64u; shift += 8u) {
                out.push_back(static_cast<std::uint8_t>(value >> shift));
            }
        }

        inline void rag_append_string(std::vector<std::uint8_t>& out, const std::string& value) {
            if (value.size() > (std::numeric_limits<std::uint32_t>::max)()) {
                throw std::length_error("RAG record string exceeds uint32_t length");
            }
            rag_append_u32(out, static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        class RagRecordReader {
        public:
            RagRecordReader(const void* data, std::size_t size)
                : m_bytes(static_cast<const std::uint8_t*>(data)), m_size(size) {
                if (m_bytes == nullptr && m_size != 0u) {
                    throw std::runtime_error("RAG record has null data");
                }
            }

            std::uint32_t u32() {
                require(4u);
                std::uint32_t value = 0u;
                for (unsigned int shift = 0u; shift != 32u; shift += 8u) {
                    value |= static_cast<std::uint32_t>(m_bytes[m_pos++]) << shift;
                }
                return value;
            }

            std::uint64_t u64() {
                require(8u);
                std::uint64_t value = 0u;
                for (unsigned int shift = 0u; shift != 64u; shift += 8u) {
                    value |= static_cast<std::uint64_t>(m_bytes[m_pos++]) << shift;
                }
                return value;
            }

            std::string string() {
                const std::size_t length = u32();
                require(length);
                const char* begin = reinterpret_cast<const char*>(m_bytes + m_pos);
                m_pos += length;
                return std::string(begin, length);
            }

            void finish() const {
                if (m_pos != m_size) {
                    throw std::runtime_error("RAG record has trailing bytes");
                }
            }

        private:
            void require(std::size_t length) {
                if (length > m_size - m_pos) {
                    throw std::runtime_error("RAG record is truncated");
                }
            }

            const std::uint8_t* m_bytes;
            std::size_t m_size;
            std::size_t m_pos = 0u;
        };

        inline std::vector<std::uint8_t> encode_document(const Document& value) {
            std::vector<std::uint8_t> out;
            rag_append_u32(out, 0x4443424du);
            rag_append_u32(out, 1u);
            rag_append_u64(out, value.id);
            rag_append_string(out, value.source_uri);
            rag_append_string(out, value.title);
            rag_append_string(out, value.source_type);
            rag_append_u64(out, static_cast<std::uint64_t>(value.created_at_ms));
            rag_append_u64(out, static_cast<std::uint64_t>(value.indexed_at_ms));
            rag_append_string(out, value.metadata_json);
            return out;
        }

        inline Document decode_document(const std::vector<std::uint8_t>& bytes) {
            RagRecordReader reader(bytes.data(), bytes.size());
            if (reader.u32() != 0x4443424du || reader.u32() != 1u) {
                throw std::runtime_error("Document record format is unsupported");
            }
            Document value;
            value.id = reader.u64();
            value.source_uri = reader.string();
            value.title = reader.string();
            value.source_type = reader.string();
            value.created_at_ms = static_cast<std::int64_t>(reader.u64());
            value.indexed_at_ms = static_cast<std::int64_t>(reader.u64());
            value.metadata_json = reader.string();
            reader.finish();
            return value;
        }

        inline std::vector<std::uint8_t> encode_chunk(const Chunk& value) {
            std::vector<std::uint8_t> out;
            rag_append_u32(out, 0x4b48434du);
            rag_append_u32(out, 1u);
            rag_append_u64(out, value.id);
            rag_append_u64(out, value.document_id);
            rag_append_u32(out, value.chunk_index);
            rag_append_u32(out, value.char_start);
            rag_append_u32(out, value.char_end);
            rag_append_string(out, value.text);
            rag_append_string(out, value.metadata_json);
            return out;
        }

        inline Chunk decode_chunk(const std::vector<std::uint8_t>& bytes) {
            RagRecordReader reader(bytes.data(), bytes.size());
            if (reader.u32() != 0x4b48434du || reader.u32() != 1u) {
                throw std::runtime_error("Chunk record format is unsupported");
            }
            Chunk value;
            value.id = reader.u64();
            value.document_id = reader.u64();
            value.chunk_index = reader.u32();
            value.char_start = reader.u32();
            value.char_end = reader.u32();
            value.text = reader.string();
            value.metadata_json = reader.string();
            reader.finish();
            return value;
        }
    } // namespace detail
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_RAG_RECORDS_HPP_INCLUDED
