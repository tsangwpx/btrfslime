from __future__ import annotations

from cffi import FFI

ffibuilder = FFI()

ffibuilder.set_source('btrfslime._xxhash', r"""
#define XXH_INLINE_ALL
#include "xxhash.h"

int32_t calc_key(const char* input, size_t length) {
    return XXH32(input, length, 0);
}

""", include_dirs=['xxhash'])

ffibuilder.cdef(r"""
int32_t calc_key(const char* input, size_t length);
""")

if __name__ == '__main__':
    ffibuilder.compile(verbose=True)
