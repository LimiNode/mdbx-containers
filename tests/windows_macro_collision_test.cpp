#if defined(_WIN32)
#    include <windows.h>
#endif

// Windows SDK headers may define `small` as a macro. Keep the collision in
// this compile-smoke even on toolchains whose SDK does not currently expose it.
#ifdef small
#    undef small
#endif
#define small mdbxc_test_small_macro
#include <mdbx_containers.hpp>
#undef small

int main() {
    mdbxc::SerializeScratch scratch;
    const unsigned char value[] = {1u, 2u, 3u, 4u};
    const MDBX_val view = scratch.view_small_copy(value, sizeof(value));
    return view.iov_len == sizeof(value) ? 0 : 1;
}
