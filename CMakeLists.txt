cmake_minimum_required(VERSION 3.18)
project(mdbx_containers VERSION 1.0.2 LANGUAGES CXX)

include(CheckCXXCompilerFlag)
include(CheckLinkerFlag)

# ---------------------------------------
# Output directories
# ---------------------------------------
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib CACHE PATH "")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib CACHE PATH "")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin CACHE PATH "")

# ---------------------------------------
# Options (prefixed to avoid clashes when used as a subproject)
# ---------------------------------------
option(MDBXC_BUILD_EXAMPLES     "Build examples"                                  ON)
option(MDBXC_BUILD_TESTS        "Build tests with CTest"                           ON)
option(MDBXC_BUILD_BENCHMARKS   "Build benchmark executables"                      OFF)
option(MDBXC_ENABLE_STRESS_TESTS "Register long stress tests with CTest"           OFF)
option(MDBXC_USE_ASAN "Enable AddressSanitizer for tests/examples (not for libmdbx)" ON)

# Optional transport backends. Off by default to keep the default build free of
# network-stack dependencies. The *_SYNC_EXAMPLE options below preserve the
# older example-oriented entry points and enable the matching backend when used.
option(MDBXC_SIMPLE_WEB_HTTP_TRANSPORT
    "Enable the optional Simple-Web-Server HTTP sync transport backend" OFF)
option(MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT
    "Enable the optional Simple-WebSocket-Server sync transport backend" OFF)
option(MDBXC_KURLYK_HTTP_TRANSPORT
    "Enable the optional Kurlyk/libcurl HTTP sync client transport backend" OFF)
option(MDBXC_HTTP_SYNC_EXAMPLE
    "Build the Simple-Web-Server HTTP sync examples" OFF)
option(MDBXC_WEBSOCKET_SYNC_EXAMPLE
    "Build the Simple-WebSocket-Server sync binding example" OFF)
option(MDBXC_KURLYK_HTTP_SYNC_EXAMPLE
    "Build the Kurlyk/libcurl HTTP sync client example" OFF)
option(MDBXC_KURLYK_HTTP_SYNC_MINGW_CURL_FALLBACK
    "Fetch a ready-made Win64 libcurl package for the MinGW Kurlyk HTTP transport if system libcurl is unavailable"
    ON)
option(MDBXC_WEBSOCKET_SYNC_MINGW_OPENSSL_FALLBACK
    "Fetch a ready-made Win64 OpenSSL Crypto package for the MinGW WebSocket transport if system OpenSSL is unavailable"
    ON)

if(MDBXC_HTTP_SYNC_EXAMPLE AND MDBXC_BUILD_EXAMPLES AND
        NOT MDBXC_SIMPLE_WEB_HTTP_TRANSPORT)
    set(MDBXC_SIMPLE_WEB_HTTP_TRANSPORT ON CACHE BOOL
        "Enable the optional Simple-Web-Server HTTP sync transport backend" FORCE)
    message(STATUS
        "MDBXC_HTTP_SYNC_EXAMPLE enables MDBXC_SIMPLE_WEB_HTTP_TRANSPORT")
endif()
if(MDBXC_WEBSOCKET_SYNC_EXAMPLE AND MDBXC_BUILD_EXAMPLES AND
        NOT MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT)
    set(MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT ON CACHE BOOL
        "Enable the optional Simple-WebSocket-Server sync transport backend"
        FORCE)
    message(STATUS
        "MDBXC_WEBSOCKET_SYNC_EXAMPLE enables "
        "MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT")
endif()
if(MDBXC_KURLYK_HTTP_SYNC_EXAMPLE AND MDBXC_BUILD_EXAMPLES AND
        NOT MDBXC_KURLYK_HTTP_TRANSPORT)
    set(MDBXC_KURLYK_HTTP_TRANSPORT ON CACHE BOOL
        "Enable the optional Kurlyk/libcurl HTTP sync client transport backend"
        FORCE)
    message(STATUS
        "MDBXC_KURLYK_HTTP_SYNC_EXAMPLE enables MDBXC_KURLYK_HTTP_TRANSPORT")
endif()

# Three-mode dependency policy for MDBX: AUTO | SYSTEM | BUNDLED
set(MDBXC_DEPS_MODE "AUTO" CACHE STRING "Dependency mode for MDBX: AUTO|SYSTEM|BUNDLED")
set_property(CACHE MDBXC_DEPS_MODE PROPERTY STRINGS AUTO SYSTEM BUNDLED)

message(STATUS "MDBXC_DEPS_MODE         = ${MDBXC_DEPS_MODE}")
message(STATUS "MDBXC_BUILD_EXAMPLES    = ${MDBXC_BUILD_EXAMPLES}")
message(STATUS "MDBXC_BUILD_TESTS       = ${MDBXC_BUILD_TESTS}")
message(STATUS "MDBXC_BUILD_BENCHMARKS  = ${MDBXC_BUILD_BENCHMARKS}")
message(STATUS "MDBXC_ENABLE_STRESS_TESTS = ${MDBXC_ENABLE_STRESS_TESTS}")
message(STATUS
    "MDBXC_SIMPLE_WEB_HTTP_TRANSPORT = ${MDBXC_SIMPLE_WEB_HTTP_TRANSPORT}")
message(STATUS
    "MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT = "
    "${MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT}")
message(STATUS "MDBXC_KURLYK_HTTP_TRANSPORT = ${MDBXC_KURLYK_HTTP_TRANSPORT}")
message(STATUS "MDBXC_HTTP_SYNC_EXAMPLE = ${MDBXC_HTTP_SYNC_EXAMPLE}")
message(STATUS "MDBXC_WEBSOCKET_SYNC_EXAMPLE = ${MDBXC_WEBSOCKET_SYNC_EXAMPLE}")
message(STATUS "MDBXC_KURLYK_HTTP_SYNC_EXAMPLE = ${MDBXC_KURLYK_HTTP_SYNC_EXAMPLE}")
message(STATUS "MDBXC_KURLYK_HTTP_SYNC_MINGW_CURL_FALLBACK = ${MDBXC_KURLYK_HTTP_SYNC_MINGW_CURL_FALLBACK}")
message(STATUS "MDBXC_WEBSOCKET_SYNC_MINGW_OPENSSL_FALLBACK = ${MDBXC_WEBSOCKET_SYNC_MINGW_OPENSSL_FALLBACK}")
message(STATUS "MDBXC_USE_ASAN          = ${MDBXC_USE_ASAN}")

set(MDBXC_HAS_ASAN OFF CACHE BOOL "Compiler+linker support -fsanitize=address")

if (MDBXC_USE_ASAN AND (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang") AND NOT MSVC)
    # Check whether the compiler supports the flag
    check_cxx_compiler_flag("-fsanitize=address" MDBXC_ASAN_CFLAG_OK)
    # Check whether the linker supports the flag
    check_linker_flag(CXX "-fsanitize=address" MDBXC_ASAN_LDFLAG_OK)

    if (MDBXC_ASAN_CFLAG_OK AND MDBXC_ASAN_LDFLAG_OK)
        set(MDBXC_HAS_ASAN ON CACHE BOOL "" FORCE)
    else()
        message(WARNING "ASan is unavailable for the current toolchain (compiler: ${MDBXC_ASAN_CFLAG_OK}, linker: ${MDBXC_ASAN_LDFLAG_OK}). Disabling MDBXC_USE_ASAN.")
        set(MDBXC_HAS_ASAN OFF CACHE BOOL "" FORCE)
    endif()
endif()

function(mdbxc_enable_asan tgt)
    if(MDBXC_HAS_ASAN AND (CMAKE_BUILD_TYPE MATCHES "Debug|RelWithDebInfo")
       AND (MINGW OR CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang"))
        target_compile_options(${tgt} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(${tgt} PRIVATE -fsanitize=address)
    endif()
endfunction()


# ---------------------------------------
# Include paths for our headers
# ---------------------------------------
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# ---------------------------------------
# MDBX dependency via helper script
# Expects cmake/deps/mdbx.cmake to provide:
#   mdbx_provide(MODE <AUTO|SYSTEM|BUNDLED> OUT_TARGET_STATIC <var> OUT_TARGET_SHARED <var>)
# and to normalize targets:
#   mdbx::mdbx        (shared/normal)
#   mdbx::mdbx-static (static)
# ---------------------------------------
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/cmake/deps)
include(cmake/deps/mdbx.cmake)

mdbx_provide(
    MODE ${MDBXC_DEPS_MODE}
    OUT_TARGET_STATIC MDBX_STATIC_TARGET
    OUT_TARGET_SHARED MDBX_SHARED_TARGET
)

if(NOT TARGET mdbx::mdbx AND NOT TARGET mdbx::mdbx-static)
    message(FATAL_ERROR "MDBX targets not provided by mdbx_provide(). Check cmake/deps/mdbx.cmake.")
endif()

# ---------------------------------------
# Header-only library target
# ---------------------------------------
set(_PKG_TARGET mdbx_containers)
set(_PKG_EXPORT_TARGET mdbx_containers_export)

add_library(${_PKG_TARGET} INTERFACE)
target_include_directories(${_PKG_TARGET} INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(${_PKG_TARGET} INTERFACE cxx_std_11)

if(TARGET mdbx::mdbx-static)
    set(_MDBXC_BUILD_MDBX_TARGET mdbx::mdbx-static)
else()
    set(_MDBXC_BUILD_MDBX_TARGET mdbx::mdbx)
endif()
target_link_libraries(${_PKG_TARGET} INTERFACE
    $<BUILD_INTERFACE:${_MDBXC_BUILD_MDBX_TARGET}>
    $<INSTALL_INTERFACE:mdbx::mdbx>
)

# Namespace alias for consumers
add_library(mdbx_containers::mdbx_containers ALIAS ${_PKG_TARGET})

include(cmake/mdbx_containersTransportBackends.cmake)

# Install a separate export-only target so the build tree can link the bundled
# MDBX target directly, while installed packages depend on find_dependency(MDBX).
add_library(${_PKG_EXPORT_TARGET} INTERFACE)
target_include_directories(${_PKG_EXPORT_TARGET} INTERFACE
    $<INSTALL_INTERFACE:include>
)
target_compile_features(${_PKG_EXPORT_TARGET} INTERFACE cxx_std_11)
target_link_libraries(${_PKG_EXPORT_TARGET} INTERFACE
    $<INSTALL_INTERFACE:$<1:mdbx::mdbx>>
)
set_target_properties(${_PKG_EXPORT_TARGET} PROPERTIES
    EXPORT_NAME mdbx_containers
)

# ---------------------------------------
# Examples (allow interactive-only locally if needed)
# ---------------------------------------
if(MDBXC_BUILD_EXAMPLES)
    file(GLOB EXAMPLES CONFIGURE_DEPENDS examples/*.cpp)
    list(FILTER EXAMPLES EXCLUDE REGEX
        "sync_13_http_simple_web_server\\.cpp$")
    list(FILTER EXAMPLES EXCLUDE REGEX
        "sync_17_websocket_simple_web_server\\.cpp$")
    list(FILTER EXAMPLES EXCLUDE REGEX
        "sync_18_http_node_fleet\\.cpp$")
    list(FILTER EXAMPLES EXCLUDE REGEX
        "sync_19_kurlyk_http_client\\.cpp$")
    foreach(example_file IN LISTS EXAMPLES)
        get_filename_component(example_name ${example_file} NAME_WE)
        add_executable(${example_name} ${example_file})
        set_target_properties(${example_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/examples
        )
        target_compile_features(${example_name} PRIVATE cxx_std_11)

        target_link_libraries(${example_name} PRIVATE ${_PKG_TARGET})
        message(STATUS "Example ${example_name} -> ${_PKG_TARGET}")

        # Enable the optional sync subsystem when its headers are exercised.
        target_compile_definitions(${example_name} PRIVATE MDBXC_SYNC_ENABLED=1)

        # To enable pause-on-Enter in examples only (not in tests), uncomment:
        # target_compile_definitions(${example_name} PRIVATE INTERACTIVE_TEST)
    endforeach()
endif()

# ---------------------------------------
# Optional Simple-Web HTTP transport binding
# ---------------------------------------
# Pulls Simple-Web-Server through FetchContent and exposes the ready
# Simple-Web HTTP binding around HttpSyncServer and HttpSyncPeer. Tests may use
# the backend without building examples.
if(MDBXC_SIMPLE_WEB_HTTP_TRANSPORT)
    mdbx_containers_simple_web_http_transport_provide(
        OUT_TARGET _MDBXC_SIMPLE_WEB_HTTP_TARGET)

    if(MDBXC_HTTP_SYNC_EXAMPLE AND MDBXC_BUILD_EXAMPLES)
        include(cmake/deps/tiny-process-library.cmake)
        tiny_process_library_provide()

        add_executable(sync_13_http_simple_web_server
            examples/sync_13_http_simple_web_server.cpp)
        set_target_properties(sync_13_http_simple_web_server PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/examples
        )
        target_compile_features(sync_13_http_simple_web_server PRIVATE cxx_std_11)
        target_link_libraries(sync_13_http_simple_web_server PRIVATE
            ${_MDBXC_SIMPLE_WEB_HTTP_TARGET})
        message(STATUS
            "HTTP binding example sync_13_http_simple_web_server -> "
            "${_MDBXC_SIMPLE_WEB_HTTP_TARGET}")

        add_executable(sync_18_http_node_fleet
            examples/sync_18_http_node_fleet.cpp)
        set_target_properties(sync_18_http_node_fleet PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/examples
        )
        target_compile_features(sync_18_http_node_fleet PRIVATE cxx_std_11)
        target_link_libraries(sync_18_http_node_fleet PRIVATE
            ${_MDBXC_SIMPLE_WEB_HTTP_TARGET}
            tiny-process-library::tiny-process-library)
        message(STATUS "HTTP node fleet example sync_18_http_node_fleet -> "
            "${_MDBXC_SIMPLE_WEB_HTTP_TARGET};tiny-process-library")
    endif()

    if(MDBXC_BUILD_TESTS)
        include(CTest)
        enable_testing()
        add_executable(test_simple_web_http_transport
            tests/optional/test_simple_web_http_transport.cpp)
        set_target_properties(test_simple_web_http_transport PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/tests
        )
        target_compile_features(test_simple_web_http_transport PRIVATE
            cxx_std_11)
        mdbxc_enable_asan(test_simple_web_http_transport)
        target_link_libraries(test_simple_web_http_transport PRIVATE
            ${_MDBXC_SIMPLE_WEB_HTTP_TARGET})
        add_test(NAME test_simple_web_http_transport
            COMMAND test_simple_web_http_transport)
        set_tests_properties(test_simple_web_http_transport PROPERTIES
            TIMEOUT 30)
        message(STATUS "HTTP binding test "
            "test_simple_web_http_transport -> "
            "${_MDBXC_SIMPLE_WEB_HTTP_TARGET}")
    endif()
endif()

# ---------------------------------------
# Optional Kurlyk HTTP transport client binding
# ---------------------------------------
# Pulls Kurlyk through FetchContent and uses its libcurl-backed HTTP client
# around HttpSyncPeer. Tests may use the client backend without building the
# example, whose server side intentionally reuses the Simple-Web listener.
if(MDBXC_KURLYK_HTTP_TRANSPORT)
    mdbx_containers_kurlyk_http_transport_provide(
        OUT_TARGET _MDBXC_KURLYK_HTTP_TARGET)

    if(MDBXC_KURLYK_HTTP_SYNC_EXAMPLE AND MDBXC_BUILD_EXAMPLES)
        mdbx_containers_simple_web_http_transport_provide(
            OUT_TARGET _MDBXC_SIMPLE_WEB_HTTP_TARGET)

        add_executable(sync_19_kurlyk_http_client
            examples/sync_19_kurlyk_http_client.cpp)
        set_target_properties(sync_19_kurlyk_http_client PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/examples
        )
        target_compile_features(sync_19_kurlyk_http_client PRIVATE cxx_std_17)
        target_link_libraries(sync_19_kurlyk_http_client PRIVATE
            ${_MDBXC_SIMPLE_WEB_HTTP_TARGET}
            ${_MDBXC_KURLYK_HTTP_TARGET})
        get_property(_MDBXC_CURL_RUNTIME_DLLS GLOBAL PROPERTY
            MDBXC_MINGW_CURL_RUNTIME_DLLS)
        foreach(_MDBXC_CURL_RUNTIME_DLL IN LISTS _MDBXC_CURL_RUNTIME_DLLS)
            add_custom_command(TARGET sync_19_kurlyk_http_client
                POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    "${_MDBXC_CURL_RUNTIME_DLL}"
                    "$<TARGET_FILE_DIR:sync_19_kurlyk_http_client>"
                VERBATIM)
        endforeach()
        message(STATUS
            "Kurlyk HTTP client example sync_19_kurlyk_http_client -> "
            "${_MDBXC_SIMPLE_WEB_HTTP_TARGET};${_MDBXC_KURLYK_HTTP_TARGET}")
    endif()

    if(MDBXC_BUILD_TESTS)
        include(CTest)
        enable_testing()
        add_executable(test_kurlyk_http_transport
            tests/optional/test_kurlyk_http_transport.cpp)
        set_target_properties(test_kurlyk_http_transport PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/tests
        )
        target_compile_features(test_kurlyk_http_transport PRIVATE cxx_std_17)
        mdbxc_enable_asan(test_kurlyk_http_transport)
        target_link_libraries(test_kurlyk_http_transport PRIVATE
            ${_MDBXC_KURLYK_HTTP_TARGET})
        get_property(_MDBXC_CURL_RUNTIME_DLLS GLOBAL PROPERTY
            MDBXC_MINGW_CURL_RUNTIME_DLLS)
        foreach(_MDBXC_CURL_RUNTIME_DLL IN LISTS _MDBXC_CURL_RUNTIME_DLLS)
            add_custom_command(TARGET test_kurlyk_http_transport
                POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    "${_MDBXC_CURL_RUNTIME_DLL}"
                    "$<TARGET_FILE_DIR:test_kurlyk_http_transport>"
                VERBATIM)
        endforeach()
        add_test(NAME test_kurlyk_http_transport
            COMMAND test_kurlyk_http_transport)
        set_tests_properties(test_kurlyk_http_transport PROPERTIES
            TIMEOUT 30)
        message(STATUS "Kurlyk HTTP client test "
            "test_kurlyk_http_transport -> ${_MDBXC_KURLYK_HTTP_TARGET}")
    endif()
endif()

# ---------------------------------------
# Optional Simple-WebSocket transport binding
# ---------------------------------------
# Pulls Simple-WebSocket-Server through FetchContent and exposes the concrete
# WebSocket binding. Tests may use the backend without building examples.
if(MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT)
    mdbx_containers_simple_web_websocket_transport_provide(
        OUT_TARGET _MDBXC_SIMPLE_WEB_WEBSOCKET_TARGET)
    get_property(_MDBXC_OPENSSL_RUNTIME_DLLS GLOBAL PROPERTY
        MDBXC_MINGW_OPENSSL_RUNTIME_DLLS)

    if(MDBXC_WEBSOCKET_SYNC_EXAMPLE AND MDBXC_BUILD_EXAMPLES)
        add_executable(sync_17_websocket_simple_web_server
            examples/sync_17_websocket_simple_web_server.cpp)
        set_target_properties(sync_17_websocket_simple_web_server PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/examples
        )
        target_compile_features(sync_17_websocket_simple_web_server PRIVATE
            cxx_std_11)
        target_link_libraries(sync_17_websocket_simple_web_server PRIVATE
            ${_MDBXC_SIMPLE_WEB_WEBSOCKET_TARGET})
        foreach(_MDBXC_OPENSSL_RUNTIME_DLL IN LISTS _MDBXC_OPENSSL_RUNTIME_DLLS)
            add_custom_command(TARGET sync_17_websocket_simple_web_server
                POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    "${_MDBXC_OPENSSL_RUNTIME_DLL}"
                    "$<TARGET_FILE_DIR:sync_17_websocket_simple_web_server>"
                VERBATIM)
        endforeach()
        message(STATUS "WebSocket binding example "
            "sync_17_websocket_simple_web_server -> "
            "${_MDBXC_SIMPLE_WEB_WEBSOCKET_TARGET}")
    endif()

    if(MDBXC_BUILD_TESTS)
        include(CTest)
        enable_testing()
        add_executable(test_simple_websocket_transport
            tests/optional/test_simple_websocket_transport.cpp)
        set_target_properties(test_simple_websocket_transport PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/tests
        )
        target_compile_features(test_simple_websocket_transport PRIVATE cxx_std_11)
        mdbxc_enable_asan(test_simple_websocket_transport)
        target_link_libraries(test_simple_websocket_transport PRIVATE
            ${_MDBXC_SIMPLE_WEB_WEBSOCKET_TARGET})
        foreach(_MDBXC_OPENSSL_RUNTIME_DLL IN LISTS _MDBXC_OPENSSL_RUNTIME_DLLS)
            add_custom_command(TARGET test_simple_websocket_transport
                POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy_if_different
                    "${_MDBXC_OPENSSL_RUNTIME_DLL}"
                    "$<TARGET_FILE_DIR:test_simple_websocket_transport>"
                VERBATIM)
        endforeach()
        add_test(NAME test_simple_websocket_transport
            COMMAND test_simple_websocket_transport)
        set_tests_properties(test_simple_websocket_transport PROPERTIES
            TIMEOUT 30)
        message(STATUS "WebSocket binding test "
            "test_simple_websocket_transport -> "
            "${_MDBXC_SIMPLE_WEB_WEBSOCKET_TARGET}")
    endif()
endif()

# ---------------------------------------
# Tests (non-interactive; registered with CTest)
# ---------------------------------------
if(MDBXC_BUILD_TESTS)
    include(CTest)
    enable_testing()
    file(GLOB TESTS CONFIGURE_DEPENDS tests/*.cpp)
    set(MDBXC_STRESS_TESTS
        test_sync_stress
    )
    set(MDBXC_SYNC_DISABLED_TESTS
        header_sync_disabled_test
    )
    set(MDBXC_STANDALONE_PUBLIC_HEADERS
        mdbx_containers/common.hpp
        mdbx_containers/AnyValueTable.hpp
        mdbx_containers/Hash.hpp
        mdbx_containers/HashedKeyValueStore.hpp
        mdbx_containers/KeyMultiValueTable.hpp
        mdbx_containers/KeyOrderedMultiValueTable.hpp
        mdbx_containers/KeyTable.hpp
        mdbx_containers/KeyValueTable.hpp
        mdbx_containers/SequenceTable.hpp
        mdbx_containers/ValueTable.hpp
    )
    set(MDBXC_SYNC_STANDALONE_HEADERS
        mdbx_containers/sync/codec_flags.hpp
        mdbx_containers/sync/LogicalSchema.hpp
        mdbx_containers/sync/ChangeOp.hpp
        mdbx_containers/sync/ChangeBatch.hpp
        mdbx_containers/sync/ChangeBatchCodec.hpp
        mdbx_containers/sync/SyncApplyObserver.hpp
        mdbx_containers/sync/SyncNodeSession.hpp
        mdbx_containers/sync/SyncWorkerGuard.hpp
        mdbx_containers/sync/stores/SchemaRegistryStore.hpp
        mdbx_containers/sync/TransportMessageCodec.hpp
    )

    function(mdbxc_add_test_target test_name test_file)
        add_executable(${test_name} ${test_file})
        set_target_properties(${test_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/tests
        )
        target_compile_features(${test_name} PRIVATE cxx_std_11)

        mdbxc_enable_asan(${test_name})

        target_link_libraries(${test_name} PRIVATE ${_PKG_TARGET})
        message(STATUS "Test ${test_name} -> ${_PKG_TARGET}")

        # Enable the optional sync subsystem when its headers are exercised.
        list(FIND MDBXC_SYNC_DISABLED_TESTS
             "${test_name}"
             _mdbxc_sync_disabled_test_index)
        if(_mdbxc_sync_disabled_test_index EQUAL -1)
            target_compile_definitions(${test_name} PRIVATE MDBXC_SYNC_ENABLED=1)
        endif()

        list(FIND MDBXC_STRESS_TESTS "${test_name}" _mdbxc_stress_test_index)
        set(_mdbxc_is_stress_test FALSE)
        if(NOT _mdbxc_stress_test_index EQUAL -1)
            set(_mdbxc_is_stress_test TRUE)
        endif()

        if(NOT _mdbxc_is_stress_test OR MDBXC_ENABLE_STRESS_TESTS)
            # IMPORTANT: we do NOT define INTERACTIVE_TEST for tests.
            add_test(NAME ${test_name} COMMAND ${test_name})

            if(_mdbxc_is_stress_test)
                set_tests_properties(${test_name} PROPERTIES LABELS stress TIMEOUT 300)
            endif()

            if(MDBXC_HAS_ASAN)
                set_tests_properties(${test_name} PROPERTIES
                    ENVIRONMENT "ASAN_OPTIONS=abort_on_error=1,detect_leaks=0,allocator_may_return_null=1"
                )
            endif()
        endif()
    endfunction()

    foreach(test_file IN LISTS TESTS)
        get_filename_component(test_name ${test_file} NAME_WE)
        mdbxc_add_test_target(${test_name} ${test_file})
    endforeach()

    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR
            ((CMAKE_CXX_COMPILER_ID MATCHES "Clang") AND
             NOT CMAKE_CXX_SIMULATE_ID STREQUAL "MSVC"))
        mdbxc_add_test_target(utils_fast_math_test tests/utils_test.cpp)
        target_compile_options(utils_fast_math_test PRIVATE -ffast-math)
    elseif((CMAKE_CXX_COMPILER_ID MATCHES "Clang") AND
            CMAKE_CXX_SIMULATE_ID STREQUAL "MSVC")
        mdbxc_add_test_target(utils_fast_math_test tests/utils_test.cpp)
        target_compile_options(utils_fast_math_test PRIVATE /fp:fast)
    endif()

    set(_mdbxc_generated_test_dir "${CMAKE_CURRENT_BINARY_DIR}/generated_tests")
    file(MAKE_DIRECTORY "${_mdbxc_generated_test_dir}")
    foreach(header IN LISTS MDBXC_STANDALONE_PUBLIC_HEADERS)
        string(MAKE_C_IDENTIFIER "${header}" header_target_suffix)
        set(test_name "header_standalone_${header_target_suffix}")
        set(test_file "${_mdbxc_generated_test_dir}/${test_name}.cpp")
        file(WRITE "${test_file}" "#include <${header}>\n\nint main() {\n    return 0;\n}\n")
        mdbxc_add_test_target(${test_name} ${test_file})
    endforeach()
    foreach(header IN LISTS MDBXC_SYNC_STANDALONE_HEADERS)
        string(MAKE_C_IDENTIFIER "${header}" header_target_suffix)
        set(test_name "header_standalone_${header_target_suffix}")
        set(test_file "${_mdbxc_generated_test_dir}/${test_name}.cpp")
        file(WRITE "${test_file}" "#include <${header}>\n\nint main() {\n    return 0;\n}\n")
        mdbxc_add_test_target(${test_name} ${test_file})
    endforeach()
endif()

# ---------------------------------------
# Benchmarks (manual targets; not registered as tests)
# ---------------------------------------
if(MDBXC_BUILD_BENCHMARKS)
    file(GLOB BENCHMARKS CONFIGURE_DEPENDS benchmarks/*.cpp)
    foreach(benchmark_file IN LISTS BENCHMARKS)
        get_filename_component(benchmark_name ${benchmark_file} NAME_WE)
        add_executable(${benchmark_name} ${benchmark_file})
        set_target_properties(${benchmark_name} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/benchmarks
        )
        target_compile_features(${benchmark_name} PRIVATE cxx_std_11)
        target_link_libraries(${benchmark_name} PRIVATE ${_PKG_TARGET})
        target_compile_definitions(${benchmark_name} PRIVATE MDBXC_SYNC_ENABLED=1)
        message(STATUS "Benchmark ${benchmark_name} -> ${_PKG_TARGET}")
    endforeach()
endif()

# ---------------------------------------
# Install: headers + target + CMake package
# ---------------------------------------
install(DIRECTORY include/ DESTINATION include)

install(TARGETS ${_PKG_EXPORT_TARGET}
    EXPORT mdbx_containersTargets
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

include(CMakePackageConfigHelpers)

set(_PKG_CONFIG_INSTALL_DIR "lib/cmake/mdbx_containers")
set(_PKG_INCLUDE_INSTALL_DIR "include")

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/mdbx_containersConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/mdbx_containersConfig.cmake
    INSTALL_DESTINATION ${_PKG_CONFIG_INSTALL_DIR}
    PATH_VARS _PKG_INCLUDE_INSTALL_DIR
    NO_CHECK_REQUIRED_COMPONENTS_MACRO
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/mdbx_containersConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(EXPORT mdbx_containersTargets
    FILE mdbx_containersTargets.cmake
    NAMESPACE mdbx_containers::
    DESTINATION ${_PKG_CONFIG_INSTALL_DIR}
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/mdbx_containersConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/mdbx_containersConfigVersion.cmake
    DESTINATION ${_PKG_CONFIG_INSTALL_DIR}
)

install(FILES
    cmake/mdbx_containersTransportBackends.cmake
    DESTINATION ${_PKG_CONFIG_INSTALL_DIR}
)

install(FILES
    cmake/deps/kurlyk.cmake
    cmake/deps/openssl-mingw.cmake
    cmake/deps/simple-web-server.cmake
    DESTINATION ${_PKG_CONFIG_INSTALL_DIR}/deps
)

# ---------------------------------------
# Developer helper: copy headers to build/include
# ---------------------------------------
add_custom_target(copy_headers_to_build ALL
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            ${CMAKE_BINARY_DIR}/include
    COMMENT "Copying headers to build/include"
)
