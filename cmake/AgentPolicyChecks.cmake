# Keep these checks intentionally lexical and repository-specific. Do not grow
# this script into a C++ or Markdown parser; semantic checks belong in dedicated
# tooling or tests.

if(NOT DEFINED MDBXC_SOURCE_DIR)
    message(FATAL_ERROR "MDBXC_SOURCE_DIR is required")
endif()

get_filename_component(MDBXC_SOURCE_DIR "${MDBXC_SOURCE_DIR}" ABSOLUTE)
set(_mdbxc_policy_errors "")

macro(mdbxc_policy_error text)
    list(APPEND _mdbxc_policy_errors "${text}")
endmacro()

# Project-owned .hpp/.h files must have a matching non-reserved guard. The
# exact historical spelling is intentionally preserved; this gate checks the
# contract structure without renaming established public macros.
file(GLOB_RECURSE _mdbxc_guarded_headers
    LIST_DIRECTORIES FALSE
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/*.h"
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/*.hpp")
foreach(_mdbxc_header IN LISTS _mdbxc_guarded_headers)
    if(_mdbxc_header STREQUAL
            "${MDBXC_SOURCE_DIR}/include/mdbx_containers/detail/xxhash.h")
        # Vendored xxHash keeps its upstream header structure.
        continue()
    endif()
    file(READ "${_mdbxc_header}" _mdbxc_header_content)
    string(REGEX MATCH
        "#ifndef[ \t]+(MDBX_CONTAINERS_HEADER_[A-Za-z0-9_]+_INCLUDED)"
        _mdbxc_guard_match
        "${_mdbxc_header_content}")
    if(NOT _mdbxc_guard_match)
        file(RELATIVE_PATH _mdbxc_relative "${MDBXC_SOURCE_DIR}" "${_mdbxc_header}")
        mdbxc_policy_error("${_mdbxc_relative}: missing project include guard")
        continue()
    endif()

    set(_mdbxc_guard "${CMAKE_MATCH_1}")
    string(FIND "${_mdbxc_header_content}"
        "#define ${_mdbxc_guard}" _mdbxc_define_index)
    string(FIND "${_mdbxc_header_content}"
        "#endif // ${_mdbxc_guard}" _mdbxc_endif_index)
    if(_mdbxc_define_index EQUAL -1 OR _mdbxc_endif_index EQUAL -1)
        file(RELATIVE_PATH _mdbxc_relative "${MDBXC_SOURCE_DIR}" "${_mdbxc_header}")
        mdbxc_policy_error(
            "${_mdbxc_relative}: include guard ${_mdbxc_guard} is not matched")
    endif()
endforeach()

file(GLOB_RECURSE _mdbxc_ipp_files
    LIST_DIRECTORIES FALSE
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/*.ipp")
foreach(_mdbxc_ipp IN LISTS _mdbxc_ipp_files)
    file(READ "${_mdbxc_ipp}" _mdbxc_ipp_content)
    if(_mdbxc_ipp_content MATCHES
            "#ifndef[ \t]+MDBX_CONTAINERS_HEADER_[A-Za-z0-9_]+_INCLUDED")
        file(RELATIVE_PATH _mdbxc_relative "${MDBXC_SOURCE_DIR}" "${_mdbxc_ipp}")
        mdbxc_policy_error("${_mdbxc_relative}: .ipp fragments must remain unguarded")
    endif()
endforeach()

# Detail headers are implementation leaves, not standalone project entry
# points. Their owning aggregate supplies shared project prerequisites. Keep
# this check lexical: reject only upward or project-root includes that reverse
# the ownership direction; local/downward, standard, and external includes are
# outside this gate.
foreach(_mdbxc_internal_leaf IN LISTS _mdbxc_guarded_headers _mdbxc_ipp_files)
    file(RELATIVE_PATH _mdbxc_leaf_relative
        "${MDBXC_SOURCE_DIR}/include/mdbx_containers"
        "${_mdbxc_internal_leaf}")
    string(REPLACE "\\" "/" _mdbxc_leaf_relative
        "${_mdbxc_leaf_relative}")
    if(NOT _mdbxc_leaf_relative MATCHES "(^|/)detail/")
        continue()
    endif()

    file(STRINGS "${_mdbxc_internal_leaf}" _mdbxc_leaf_lines)
    set(_mdbxc_line_number 0)
    foreach(_mdbxc_line IN LISTS _mdbxc_leaf_lines)
        math(EXPR _mdbxc_line_number "${_mdbxc_line_number} + 1")
        string(REGEX REPLACE "[ \t]" "" _mdbxc_compact_line
            "${_mdbxc_line}")
        string(FIND "${_mdbxc_compact_line}"
            "#include\"../" _mdbxc_quote_upward)
        string(FIND "${_mdbxc_compact_line}"
            "#include<../" _mdbxc_angle_upward)
        string(FIND "${_mdbxc_compact_line}"
            "#include\"mdbx_containers/" _mdbxc_quote_root)
        string(FIND "${_mdbxc_compact_line}"
            "#include<mdbx_containers/" _mdbxc_angle_root)
        if(NOT _mdbxc_quote_upward EQUAL -1 OR
                NOT _mdbxc_angle_upward EQUAL -1 OR
                NOT _mdbxc_quote_root EQUAL -1 OR
                NOT _mdbxc_angle_root EQUAL -1)
            file(RELATIVE_PATH _mdbxc_relative
                "${MDBXC_SOURCE_DIR}" "${_mdbxc_internal_leaf}")
            mdbxc_policy_error(
                "${_mdbxc_relative}:${_mdbxc_line_number}: detail leaf must not include upward or project-root headers")
        endif()
    endforeach()
endforeach()

# Remove horizontal whitespace before looking for all default-capture forms:
# [&], [=], [&, name], and [=, &name]. Explicit captures remain allowed.
file(GLOB_RECURSE _mdbxc_cpp_sources
    LIST_DIRECTORIES FALSE
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/*.h"
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/*.hpp"
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/*.ipp"
    "${MDBXC_SOURCE_DIR}/tests/*.cpp"
    "${MDBXC_SOURCE_DIR}/examples/*.cpp"
    "${MDBXC_SOURCE_DIR}/benchmarks/*.cpp")
foreach(_mdbxc_source IN LISTS _mdbxc_cpp_sources)
    if(_mdbxc_source STREQUAL
            "${MDBXC_SOURCE_DIR}/include/mdbx_containers/detail/xxhash.h")
        continue()
    endif()
    file(STRINGS "${_mdbxc_source}" _mdbxc_source_lines)
    set(_mdbxc_line_number 0)
    foreach(_mdbxc_line IN LISTS _mdbxc_source_lines)
        math(EXPR _mdbxc_line_number "${_mdbxc_line_number} + 1")
        string(REGEX REPLACE "[ \t]" "" _mdbxc_compact_line "${_mdbxc_line}")
        string(FIND "${_mdbxc_compact_line}" "[&]" _mdbxc_ref_only)
        string(FIND "${_mdbxc_compact_line}" "[&," _mdbxc_ref_more)
        string(FIND "${_mdbxc_compact_line}" "[=]" _mdbxc_value_only)
        string(FIND "${_mdbxc_compact_line}" "[=," _mdbxc_value_more)
        string(FIND "${_mdbxc_compact_line}"
            "MDBXC_AGENT_POLICY_ALLOW_DEFAULT_CAPTURE" _mdbxc_capture_escape)
        if(_mdbxc_capture_escape EQUAL -1 AND
                (NOT _mdbxc_ref_only EQUAL -1 OR
                NOT _mdbxc_ref_more EQUAL -1 OR
                NOT _mdbxc_value_only EQUAL -1 OR
                NOT _mdbxc_value_more EQUAL -1))
            file(RELATIVE_PATH _mdbxc_relative
                "${MDBXC_SOURCE_DIR}" "${_mdbxc_source}")
            mdbxc_policy_error(
                "${_mdbxc_relative}:${_mdbxc_line_number}: lambda default capture is forbidden")
        endif()
    endforeach()
endforeach()

# Normative documentation pairs are explicit so adding an unrelated English
# guide does not silently make it normative or require a synthetic translation.
set(_mdbxc_document_pairs
    "README.md|README-RU.md"
    "examples/README-sync.md|examples/README-sync-RU.md"
    "guides/sync-architecture.md|guides/sync-architecture-RU.md"
    "guides/sync-audit-followups.md|guides/sync-audit-followups-RU.md"
    "guides/sync-selective-replication-design.md|guides/sync-selective-replication-design-RU.md"
    "guides/sync-table-coverage.md|guides/sync-table-coverage-RU.md"
    "guides/sync-transport-production.md|guides/sync-transport-production-RU.md"
    "guides/sync-v0.1-readiness.md|guides/sync-v0.1-readiness-RU.md")
foreach(_mdbxc_pair IN LISTS _mdbxc_document_pairs)
    string(REPLACE "|" ";" _mdbxc_pair_paths "${_mdbxc_pair}")
    foreach(_mdbxc_pair_path IN LISTS _mdbxc_pair_paths)
        if(NOT EXISTS "${MDBXC_SOURCE_DIR}/${_mdbxc_pair_path}")
            mdbxc_policy_error(
                "${_mdbxc_pair_path}: expected normative documentation counterpart is missing")
        endif()
    endforeach()
endforeach()

# Check local Markdown targets in maintained agent and guide documents. Use the
# tracked guide set in a git checkout so unrelated local drafts do not become
# policy inputs; source archives fall back to the files they contain. External
# URLs and in-document anchors are intentionally outside this filesystem gate.
set(_mdbxc_guide_docs "")
find_program(_mdbxc_git_executable git)
if(_mdbxc_git_executable AND EXISTS "${MDBXC_SOURCE_DIR}/.git")
    execute_process(
        COMMAND "${_mdbxc_git_executable}" -C "${MDBXC_SOURCE_DIR}"
            ls-files -- "guides/*.md"
        OUTPUT_VARIABLE _mdbxc_tracked_guides
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE _mdbxc_git_result)
    if(_mdbxc_git_result EQUAL 0 AND NOT _mdbxc_tracked_guides STREQUAL "")
        string(REPLACE "\n" ";" _mdbxc_tracked_guides
            "${_mdbxc_tracked_guides}")
        foreach(_mdbxc_tracked_guide IN LISTS _mdbxc_tracked_guides)
            list(APPEND _mdbxc_guide_docs
                "${MDBXC_SOURCE_DIR}/${_mdbxc_tracked_guide}")
        endforeach()
    endif()
endif()
if(NOT _mdbxc_guide_docs)
    file(GLOB _mdbxc_guide_docs LIST_DIRECTORIES FALSE
        "${MDBXC_SOURCE_DIR}/guides/*.md")
endif()
# Include the newly introduced scoped document before its first commit.
list(APPEND _mdbxc_guide_docs "${MDBXC_SOURCE_DIR}/guides/AGENTS.md")
list(REMOVE_DUPLICATES _mdbxc_guide_docs)
set(_mdbxc_policy_docs
    "${MDBXC_SOURCE_DIR}/AGENTS.md"
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/AGENTS.md"
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/detail/AGENTS.md"
    "${MDBXC_SOURCE_DIR}/include/mdbx_containers/sync/AGENTS.md"
    "${MDBXC_SOURCE_DIR}/tests/AGENTS.md"
    "${MDBXC_SOURCE_DIR}/.github/AGENTS.md"
    ${_mdbxc_guide_docs})
foreach(_mdbxc_doc IN LISTS _mdbxc_policy_docs)
    file(STRINGS "${_mdbxc_doc}" _mdbxc_doc_lines)
    set(_mdbxc_line_number 0)
    get_filename_component(_mdbxc_doc_dir "${_mdbxc_doc}" DIRECTORY)
    foreach(_mdbxc_line IN LISTS _mdbxc_doc_lines)
        math(EXPR _mdbxc_line_number "${_mdbxc_line_number} + 1")
        set(_mdbxc_link_tail "${_mdbxc_line}")
        while(TRUE)
            string(FIND "${_mdbxc_link_tail}" "](" _mdbxc_link_start)
            if(_mdbxc_link_start EQUAL -1)
                break()
            endif()
            math(EXPR _mdbxc_target_start "${_mdbxc_link_start} + 2")
            string(SUBSTRING "${_mdbxc_link_tail}" ${_mdbxc_target_start} -1
                _mdbxc_after_link_start)
            string(FIND "${_mdbxc_after_link_start}" ")" _mdbxc_link_end)
            if(_mdbxc_link_end EQUAL -1)
                break()
            endif()
            string(SUBSTRING "${_mdbxc_after_link_start}" 0
                ${_mdbxc_link_end} _mdbxc_target)
            string(STRIP "${_mdbxc_target}" _mdbxc_target)
            if(NOT _mdbxc_target MATCHES "^(https?://|mailto:|#)" AND
                    _mdbxc_target MATCHES "\.md(#.*)?$")
                string(REGEX REPLACE "#.*$" ""
                    _mdbxc_target "${_mdbxc_target}")
                if(NOT _mdbxc_target STREQUAL "")
                    get_filename_component(_mdbxc_resolved
                        "${_mdbxc_doc_dir}/${_mdbxc_target}" ABSOLUTE)
                    if(NOT EXISTS "${_mdbxc_resolved}")
                        file(RELATIVE_PATH _mdbxc_relative
                            "${MDBXC_SOURCE_DIR}" "${_mdbxc_doc}")
                        mdbxc_policy_error(
                            "${_mdbxc_relative}:${_mdbxc_line_number}: missing local link target ${_mdbxc_target}")
                    endif()
                endif()
            endif()
            math(EXPR _mdbxc_tail_start "${_mdbxc_link_end} + 1")
            string(SUBSTRING "${_mdbxc_after_link_start}"
                ${_mdbxc_tail_start} -1 _mdbxc_link_tail)
        endwhile()
    endforeach()
endforeach()

list(LENGTH _mdbxc_policy_errors _mdbxc_error_count)
if(_mdbxc_error_count GREATER 0)
    string(REPLACE ";" "\n  - " _mdbxc_error_text "${_mdbxc_policy_errors}")
    message(FATAL_ERROR
        "Agent policy checks found ${_mdbxc_error_count} error(s):\n  - ${_mdbxc_error_text}")
endif()

list(LENGTH _mdbxc_guarded_headers _mdbxc_header_count)
list(LENGTH _mdbxc_cpp_sources _mdbxc_source_count)
list(LENGTH _mdbxc_policy_docs _mdbxc_doc_count)
message(STATUS
    "Agent policy checks passed: ${_mdbxc_header_count} headers, "
    "${_mdbxc_source_count} C++ files, ${_mdbxc_doc_count} documents")
