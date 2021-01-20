from __future__ import annotations

from cffi import FFI

ffibuilder = FFI()

ffibuilder.set_source('btrfslime._fs', r"""
#include <linux/fs.h>
#include <linux/fiemap.h>
""")

ffibuilder.cdef(r"""
// APIs used by fiemap
#define FS_IOC_FIEMAP ...
#define FIEMAP_MAX_OFFSET               ...
#define FIEMAP_FLAG_SYNC                ...
#define FIEMAP_FLAG_XATTR               ...
#define FIEMAP_FLAGS_COMPAT             ...
#define FIEMAP_EXTENT_LAST              ...
#define FIEMAP_EXTENT_UNKNOWN           ...
#define FIEMAP_EXTENT_DELALLOC          ...
#define FIEMAP_EXTENT_ENCODED           ...
#define FIEMAP_EXTENT_DATA_ENCRYPTED    ...
#define FIEMAP_EXTENT_NOT_ALIGNED       ...
#define FIEMAP_EXTENT_DATA_INLINE       ...
#define FIEMAP_EXTENT_DATA_TAIL         ...
#define FIEMAP_EXTENT_UNWRITTEN         ...
#define FIEMAP_EXTENT_MERGED            ...
#define FIEMAP_EXTENT_SHARED            ...

struct fiemap_extent {
    uint64_t fe_logical;
    uint64_t fe_physical;
    uint64_t fe_length;
    uint64_t fe_reserved64[2];
    uint32_t fe_flags;
    uint32_t fe_reserved[3];
};
struct fiemap {
    uint64_t fm_start;
    uint64_t fm_length;
    uint32_t fm_flags;
    uint32_t fm_mapped_extents;
    uint32_t fm_extent_count;
    uint32_t fm_reserved;
    struct fiemap_extent fm_extents[];
};

// APIs used by fideduperange
#define FIDEDUPERANGE ...
#define FILE_DEDUPE_RANGE_DIFFERS       ...
#define FILE_DEDUPE_RANGE_SAME          ...

struct file_dedupe_range {
    uint64_t src_offset;
    uint64_t src_length;
    uint16_t dest_count;
    uint16_t reserved1;
    uint32_t reserved2;
    struct file_dedupe_range_info info[];
};
struct file_dedupe_range_info {
    int64_t dest_fd;
    uint64_t dest_offset;
    uint64_t bytes_deduped;
    int32_t status;
    uint32_t reserved;
};

""")

if __name__ == '__main__':
    ffibuilder.compile(verbose=True)
