#!/usr/bin/env bash

# This is deliberately failure-only CI evidence. It must not hide the failed test.
set -u

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <build-directory>" >&2
  exit 2
fi

build_dir="$(cd "$1" && pwd)"
output_file="$build_dir/windows-mdbx-diagnostics.log"

capture_command() {
  printf '\n$'
  printf ' %q' "$@"
  printf '\n'
  "$@"
  printf '[exit %d]\n' "$?"
}

{
  echo "Windows MDBX failure diagnostics"
  printf 'RUNNER_OS=%s\n' "${RUNNER_OS:-unset}"
  printf 'RUNNER_ARCH=%s\n' "${RUNNER_ARCH:-unset}"
  printf 'ImageOS=%s\n' "${ImageOS:-unset}"
  printf 'ImageVersion=%s\n' "${ImageVersion:-unset}"
  printf 'MSYSTEM=%s\n' "${MSYSTEM:-unset}"
  capture_command uname -a
  capture_command x86_64-w64-mingw32-gcc --version
  capture_command x86_64-w64-mingw32-g++ --version
  capture_command cmake --version
  capture_command ninja --version
  capture_command pacman -Q
  capture_command git -C external/libmdbx rev-parse HEAD
  capture_command git -C external/libmdbx describe --always --tags --dirty

  if [ -f "$build_dir/CMakeCache.txt" ]; then
    echo
    echo "CMake cache (toolchain and MDBX settings)"
    grep -E '^(CMAKE_(C|CXX)_COMPILER|CMAKE_GENERATOR|MDBX_)' \
      "$build_dir/CMakeCache.txt" || true
  fi

  mapfile -t legacy_examples < <(
    find "$build_dir" -type f -name 'mdbx_legacy_example.exe' -print 2>/dev/null)
  if [ "${#legacy_examples[@]}" -eq 0 ]; then
    echo
    echo "No mdbx_legacy_example.exe was built in this job."
  fi

  for legacy_example in "${legacy_examples[@]}"; do
    echo
    printf 'mdbx_legacy_example=%s\n' "$legacy_example"
    capture_command ldd "$legacy_example"
    runtime_dir="$(mktemp -d "$build_dir/mdbx-env-create.XXXXXX")"
    echo
    printf '$ (cd %q && %q)\n' "$runtime_dir" "$legacy_example"
    (
      cd "$runtime_dir" || exit 1
      "$legacy_example"
    )
    printf '[exit %d]\n' "$?"
    rm -rf "$runtime_dir"
  done
} > "$output_file" 2>&1

cat "$output_file"
