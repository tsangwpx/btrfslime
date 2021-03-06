from __future__ import annotations

from cffi import FFI

ffi: FFI


class lib:
    FS_IOC_FIEMAP: int
    FIEMAP_MAX_OFFSET: int
    FIEMAP_FLAG_SYNC: int
    FIEMAP_FLAG_XATTR: int
    FIEMAP_FLAGS_COMPAT: int
    FIEMAP_EXTENT_LAST: int
    FIEMAP_EXTENT_UNKNOWN: int
    FIEMAP_EXTENT_DELALLOC: int
    FIEMAP_EXTENT_ENCODED: int
    FIEMAP_EXTENT_DATA_ENCRYPTED: int
    FIEMAP_EXTENT_NOT_ALIGNED: int
    FIEMAP_EXTENT_DATA_INLINE: int
    FIEMAP_EXTENT_DATA_TAIL: int
    FIEMAP_EXTENT_UNWRITTEN: int
    FIEMAP_EXTENT_MERGED: int
    FIEMAP_EXTENT_SHARED: int

    FIDEDUPERANGE: int
    FILE_DEDUPE_RANGE_DIFFERS: int
    FILE_DEDUPE_RANGE_SAME: int

    @staticmethod
    def ioctl(fd: int, request: int, *args: ffi.CData) -> int: ...
