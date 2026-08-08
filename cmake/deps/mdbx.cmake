# cmake/deps/mdbx.cmake
# Provides libmdbx targets with minimal, robust logic:
#   1) use an existing parent-provided target, if available
#   2) SYSTEM/AUTO: try find_package (CONFIG/MODULE, various names)
#   3) BUNDLED/AUTO: try submodule at external/libmdbx (with Windows/MSYS quirks)
#   4) fallback: FetchContent from upstream
#
# Canonical aliases produced:
#   mdbx::mdbx        - "the" MDBX target (shared if system provides; otherwise static alias)
#   mdbx::mdbx-static - static target (if available; else absent)
#
# Public API:
#   mdbx_provide(
#       MODE <AUTO|SYSTEM|BUNDLED>
#       OUT_TARGET_STATIC <var>     # returns 'mdbx::mdbx-static' if exists, else 'mdbx::mdbx'
#       OUT_TARGET_SHARED <var>     # returns 'mdbx::mdbx' (shared if system provided; bundled path -> static alias)
#   )
#
# NOTE: Existing parent-provided targets take precedence over MODE, including
#       BUNDLED.  MODE only controls fallback lookup after parent targets.
#
# NOTE: We intentionally keep bundled builds STATIC (MDBX_BUILD_SHARED_LIBRARY=OFF),
#       to preserve existing CI behavior and Windows/MSYS quirks you had.

include(FetchContent)

set(MDBX_GIT_TAG "v0.13.12" CACHE STRING "libmdbx release tag")

# -------- internals ----------------------------------------------------------

function(_mdbx_resolve_alias out_target target_name)
    set(_resolved_target "${target_name}")

    if(TARGET ${target_name})
        get_target_property(_aliased_target ${target_name} ALIASED_TARGET)
        if(_aliased_target)
            set(_resolved_target "${_aliased_target}")
        endif()
    endif()

    set(${out_target} "${_resolved_target}" PARENT_SCOPE)
endfunction()

function(_mdbx_make_aliases _maybe_shared _maybe_static)
    if(DEFINED _maybe_shared AND NOT "${_maybe_shared}" STREQUAL "")
        _mdbx_resolve_alias(_shared_target "${_maybe_shared}")
        if(TARGET ${_shared_target} AND NOT TARGET mdbx::mdbx)
            add_library(mdbx::mdbx ALIAS ${_shared_target})
            message(STATUS "[mdbx] Aliased '${_maybe_shared}' -> mdbx::mdbx")
        endif()
    endif()
    if(DEFINED _maybe_static AND NOT "${_maybe_static}" STREQUAL "")
        _mdbx_resolve_alias(_static_target "${_maybe_static}")
        if(TARGET ${_static_target} AND NOT TARGET mdbx::mdbx-static)
            add_library(mdbx::mdbx-static ALIAS ${_static_target})
            message(STATUS "[mdbx] Aliased '${_maybe_static}' -> mdbx::mdbx-static")
        endif()
    endif()

    if(NOT TARGET mdbx::mdbx AND TARGET mdbx::mdbx-static)
        _mdbx_resolve_alias(_static_target_for_shared "mdbx::mdbx-static")
        if(TARGET ${_static_target_for_shared})
            add_library(mdbx::mdbx ALIAS ${_static_target_for_shared})
            message(STATUS "[mdbx] Aliased 'mdbx::mdbx-static' -> mdbx::mdbx")
        endif()
    endif()
endfunction()

function(_mdbx_detect_and_alias out_ok)
    set(${out_ok} FALSE PARENT_SCOPE)

    # common upstream target name variants
    set(_cands_shared
        mdbx::mdbx
        mdbx
        libmdbx::mdbx
        libmdbx
    )
    set(_cands_static
        mdbx::mdbx-static
        mdbx-static
        libmdbx::mdbx-static
        libmdbx-static
        mdbx_static
        libmdbx_static
    )

    set(_found_shared "")
    foreach(t ${_cands_shared})
        if(TARGET ${t})
            set(_found_shared ${t})
            break()
        endif()
    endforeach()

    set(_found_static "")
    foreach(t ${_cands_static})
        if(TARGET ${t})
            set(_found_static ${t})
            break()
        endif()
    endforeach()

    if(_found_shared OR _found_static)
        _mdbx_make_aliases("${_found_shared}" "${_found_static}")
        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    endif()
endfunction()

function(_mdbx_try_existing_target out_ok)
    set(${out_ok} FALSE PARENT_SCOPE)

    _mdbx_detect_and_alias(_ok)
    if(_ok)
        message(STATUS "[mdbx] Using existing CMake target")
        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    endif()
endfunction()

function(_mdbx_try_find_package out_ok)
    set(${out_ok} FALSE PARENT_SCOPE)

    # Prefer CONFIG
    find_package(MDBX CONFIG QUIET)
    _mdbx_detect_and_alias(_ok1)
    if(MDBX_FOUND OR _ok1)
        if(MDBX_FOUND)
            message(STATUS "[mdbx] Found via find_package(MDBX CONFIG)")
        endif()
        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    endif()

    # Some distros expose MODULE or different casing
    find_package(MDBX QUIET MODULE)
    _mdbx_detect_and_alias(_ok2)
    if(_ok2)
        message(STATUS "[mdbx] Found via find_package(MDBX MODULE)")
        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    endif()

    # Try lowercase package id on some setups
    find_package(mdbx CONFIG QUIET)
    _mdbx_detect_and_alias(_ok3)
    if(mdbx_FOUND OR _ok3)
        if(mdbx_FOUND)
            message(STATUS "[mdbx] Found via find_package(mdbx CONFIG)")
        endif()
        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    endif()
endfunction()

function(_mdbx_try_submodule out_ok)
    set(${out_ok} FALSE PARENT_SCOPE)

    # Expected location of submodule
    set(_MDBX_SRC "${PROJECT_SOURCE_DIR}/external/libmdbx")
    if(EXISTS "${_MDBX_SRC}/CMakeLists.txt")
        # --- Windows/MSYS quirk: normalize to REALPATH to avoid mixed path issues
        if(WIN32)
            get_filename_component(_MDBX_SRC_REAL "${_MDBX_SRC}" REALPATH)
        else()
            set(_MDBX_SRC_REAL "${_MDBX_SRC}")
        endif()
        
        # Try to ensure git tags are available for version.c generation
        # If submodule is a git repo, fetch tags (best-effort).
        find_program(_GIT git)
        if(_GIT)
            execute_process(
                COMMAND "${_GIT}" rev-parse --is-inside-work-tree
                WORKING_DIRECTORY "${_MDBX_SRC_REAL}"
                OUTPUT_VARIABLE _is_repo
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_QUIET
            )
            if(_is_repo STREQUAL "true")
                execute_process(
                COMMAND "${_GIT}" fetch --tags --force
                WORKING_DIRECTORY "${_MDBX_SRC_REAL}"
                RESULT_VARIABLE _fetch_rc
                OUTPUT_QUIET ERROR_QUIET
                )
                if(NOT _fetch_rc EQUAL 0)
                    message(WARNING "[mdbx] failed to fetch tags for submodule; falling back to VERSION.json if present")
                endif()
            endif()
        endif()

        # Build options BEFORE add_subdirectory (keep static as before)
        set(MDBX_BUILD_SHARED_LIBRARY OFF CACHE BOOL "" FORCE)
        set(MDBX_BUILD_TOOLS          OFF CACHE BOOL "" FORCE)

        # Optional: allow their install() of static lib in SDK bundle scenarios (kept from your logic)
        if(DEFINED IMGUIX_SDK_BUNDLE_DEPS AND IMGUIX_SDK_BUNDLE_DEPS)
            set(MDBX_INSTALL_STATIC ON CACHE BOOL "" FORCE)
        endif()

        # Keep upstream release metadata intact.  Newer libmdbx releases validate
        # the amalgamated source package and require VERSION.json to be present.

        # Unique binary dir to avoid clobbering
        add_subdirectory("${_MDBX_SRC_REAL}" "${CMAKE_BINARY_DIR}/_deps/mdbx-build")

        # Upstream usually exposes mdbx-static (or mdbx)
        if(TARGET mdbx-static)
            _mdbx_make_aliases("" "mdbx-static")
            if(NOT TARGET mdbx::mdbx)
                # canonical shared alias points to static in bundled builds
                add_library(mdbx::mdbx ALIAS mdbx-static)
            endif()
        elseif(TARGET mdbx)
            _mdbx_make_aliases("mdbx" "")
        else()
            message(FATAL_ERROR "libmdbx submodule added but no known target (mdbx / mdbx-static) was created")
        endif()

        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    endif()
endfunction()

function(_mdbx_try_fetchcontent out_ok)
    set(${out_ok} FALSE PARENT_SCOPE)

    # Keep STATIC for bundled path to preserve CI behavior
    set(MDBX_BUILD_SHARED_LIBRARY OFF CACHE BOOL "" FORCE)
    set(MDBX_BUILD_TOOLS          OFF CACHE BOOL "" FORCE)
    if(DEFINED IMGUIX_SDK_BUNDLE_DEPS AND IMGUIX_SDK_BUNDLE_DEPS)
        set(MDBX_INSTALL_STATIC ON CACHE BOOL "" FORCE)
    endif()

    FetchContent_Declare(
        libmdbx
        GIT_REPOSITORY https://github.com/erthink/libmdbx.git
        GIT_TAG        ${MDBX_GIT_TAG}
        GIT_SHALLOW    FALSE
        FIND_PACKAGE_ARGS
    )
    message(STATUS "[mdbx] Fetching libmdbx (${MDBX_GIT_TAG})")
    FetchContent_MakeAvailable(libmdbx)

    if(TARGET mdbx-static)
        _mdbx_make_aliases("" "mdbx-static")
        if(NOT TARGET mdbx::mdbx)
            add_library(mdbx::mdbx ALIAS mdbx-static)
        endif()
        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    elseif(TARGET mdbx)
        _mdbx_make_aliases("mdbx" "")
        set(${out_ok} TRUE PARENT_SCOPE)
        return()
    else()
        message(FATAL_ERROR "Unknown libmdbx target names after FetchContent; check upstream CMake")
    endif()
endfunction()

# -------- public API ---------------------------------------------------------

# mdbx_provide(
#     MODE <AUTO|SYSTEM|BUNDLED>
#     OUT_TARGET_STATIC <var>
#     OUT_TARGET_SHARED <var>
# )
function(mdbx_provide)
    set(_oneValueArgs MODE OUT_TARGET_STATIC OUT_TARGET_SHARED)
    cmake_parse_arguments(MX "" "${_oneValueArgs}" "" ${ARGN})

    if(NOT MX_MODE)
        set(MX_MODE "AUTO")
    endif()
    string(TOUPPER "${MX_MODE}" MX_MODE_UP)
    message(STATUS "[mdbx] Provide mode = ${MX_MODE_UP}")

    set(_ok FALSE)

    _mdbx_try_existing_target(_ok)

    if(NOT _ok AND (MX_MODE_UP STREQUAL "SYSTEM" OR MX_MODE_UP STREQUAL "AUTO"))
        _mdbx_try_find_package(_ok)
    endif()

    if(NOT _ok AND (MX_MODE_UP STREQUAL "BUNDLED" OR MX_MODE_UP STREQUAL "AUTO"))
        _mdbx_try_submodule(_ok)
    endif()

    if(NOT _ok AND (MX_MODE_UP STREQUAL "BUNDLED" OR MX_MODE_UP STREQUAL "AUTO"))
        _mdbx_try_fetchcontent(_ok)
    endif()

    if(NOT _ok)
        if(MX_MODE_UP STREQUAL "SYSTEM")
            message(FATAL_ERROR "[mdbx] SYSTEM mode requested, but no existing target or package found.")
        else()
            message(FATAL_ERROR "[mdbx] Failed to provide MDBX (existing target, find_package, submodule, FetchContent all failed).")
        endif()
    endif()

    # Exposed aliases should exist by now:
    # - mdbx::mdbx always present (shared if system provided; else alias to static)
    # - mdbx::mdbx-static present if upstream built static target
    if(NOT TARGET mdbx::mdbx)
        message(FATAL_ERROR "[mdbx] Internal error: canonical target mdbx::mdbx was not created")
    endif()

    # Return outputs:
    if(MX_OUT_TARGET_SHARED)
        # may be SHARED (system) or an alias to static (bundled)
        set(${MX_OUT_TARGET_SHARED} "mdbx::mdbx" PARENT_SCOPE)
    endif()

    if(MX_OUT_TARGET_STATIC)
        if(TARGET mdbx::mdbx-static)
            set(${MX_OUT_TARGET_STATIC} "mdbx::mdbx-static" PARENT_SCOPE)
        else()
            # No explicit static target known → fall back to the canonical one
            set(${MX_OUT_TARGET_STATIC} "mdbx::mdbx" PARENT_SCOPE)
        endif()
    endif()
endfunction()
