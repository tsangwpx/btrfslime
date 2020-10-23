from __future__ import annotations

import enum
import fcntl
from typing import List, AnyStr, Union

from ._fs import ffi, lib

__all__ = [
    'ExtentFlag',
    'Extent',
    'fiemap',
    'print_extents',
]

# Assume page size is 4096
_DEFAULT_COUNT = (4096 - ffi.sizeof('struct fiemap')) // ffi.sizeof('struct fiemap_extent')


class ExtentFlag(enum.IntFlag):
    LAST = lib.FIEMAP_EXTENT_LAST
    UNKNOWN = lib.FIEMAP_EXTENT_UNKNOWN
    DELALLOC = lib.FIEMAP_EXTENT_DELALLOC
    ENCODED = lib.FIEMAP_EXTENT_ENCODED
    ENCRYPTED = lib.FIEMAP_EXTENT_DATA_ENCRYPTED
    NOT_ALIGNED = lib.FIEMAP_EXTENT_NOT_ALIGNED
    DATA_INLINE = lib.FIEMAP_EXTENT_DATA_INLINE
    DATA_TAIL = lib.FIEMAP_EXTENT_DATA_TAIL
    UNWRITTEN = lib.FIEMAP_EXTENT_UNWRITTEN
    MERGED = lib.FIEMAP_EXTENT_MERGED
    SHARED = lib.FIEMAP_EXTENT_SHARED


class Extent:
    __slots__ = ('logical', 'physical', 'length', 'flags')

    logical: int
    physical: int
    length: int
    flags: ExtentFlag

    def __init__(self, logical: int, physical: int, length: int, flags: int):
        self.logical = logical
        self.physical = physical
        self.length = length
        self.flags = ExtentFlag(flags)

    def __eq__(self, other):
        if not isinstance(other, Extent):
            return NotImplemented
        return (
            self.logical == other.logical
            and self.physical == other.physical
            and self.length == other.length
            and self.flags == other.flags
        )

    def __repr__(self):
        return f'<Extent logical={self.logical} physical={self.physical} length={self.length} flags={self.flags}>'

    def __hash__(self):
        return hash((self.logical, self.physical, self.length, self.flags))

    @property
    def logical_stop(self):
        return self.logical + self.length

    @property
    def physical_stop(self):
        return self.physical + self.length

    def physically_equals(self, other: Extent):
        if not isinstance(other, Extent):
            raise TypeError
        return self.physical == other.physical and self.length == other.length

    def logically_equals(self, other: Extent):
        if not isinstance(other, Extent):
            raise TypeError
        return self.logical == other.logical and self.length == other.length

    @staticmethod
    def logically_contiguous(a: Extent, b: Extent):
        return a.logical + a.length == b.logical

    @staticmethod
    def physically_contiguous(a: Extent, b: Extent):
        return a.physical + a.length == b.physical


def _fiemap_flags(sync=False, xattr=False):
    flags = 0

    if sync:
        flags |= lib.FIEMAP_FLAG_SYNC

    if xattr:
        flags |= lib.FIEMAP_FLAG_XATTR

    return flags


def _fiemap_new(
    start=0,
    length=lib.FIEMAP_MAX_OFFSET,
    flags=0,
    extent_count: int = None,
    extents: int = None,
):
    """
    Helper function to create struct fiemap*

    :param start:
    :param length:
    :param flags:
    :param extent_count:
    :return:
    """

    if extent_count is None:
        extent_count = _DEFAULT_COUNT

    if extents is None:
        extents = extent_count

    return ffi.new('struct fiemap*', {
        'fm_start': start,
        'fm_length': length,
        'fm_flags': flags,
        'fm_extent_count': extent_count,
        'fm_extents': extents,
    })


def _fiemap(fd: int, *, sync=False) -> List[Extent]:
    flags = _fiemap_flags(sync=sync)
    ptr = _fiemap_new(extent_count=0, extents=_DEFAULT_COUNT, flags=flags)
    fcntl.ioctl(fd, lib.FS_IOC_FIEMAP, ffi.buffer(ptr))

    extent_count = ptr.fm_mapped_extents
    if extent_count > _DEFAULT_COUNT:
        # Allocate a new struct
        ptr = _fiemap_new(extent_count=extent_count, flags=flags)
    else:
        ptr.fm_extent_count = extent_count

    fcntl.ioctl(fd, lib.FS_IOC_FIEMAP, ffi.buffer(ptr))

    if extent_count != ptr.fm_mapped_extents:
        raise IOError('extent_count != mapped_extents')

    extents = [
        Extent(
            e.fe_logical,
            e.fe_physical,
            e.fe_length,
            e.fe_flags,
        ) for e in ptr.fm_extents[0:extent_count]
    ]

    if extents and ExtentFlag.LAST not in extents[-1].flags:
        raise IOError('The last extent does not have the last flag')

    return extents


def fiemap(file: Union[AnyStr, int], *, sync=False, xattr=False) -> List[Extent]:
    with open(file, 'rb', closefd=not isinstance(file, int)) as f:
        fd = f.fileno()
        flags = _fiemap_flags(sync=sync, xattr=xattr)
        ptr = _fiemap_new(extent_count=0, extents=_DEFAULT_COUNT, flags=flags)
        fcntl.ioctl(fd, lib.FS_IOC_FIEMAP, ffi.buffer(ptr))

        extent_count = ptr.fm_mapped_extents
        if extent_count > _DEFAULT_COUNT:
            # Allocate a new struct
            ptr = _fiemap_new(extent_count=extent_count, flags=flags)
        else:
            ptr.fm_extent_count = extent_count

        fcntl.ioctl(fd, lib.FS_IOC_FIEMAP, ffi.buffer(ptr))

        if extent_count != ptr.fm_mapped_extents:
            raise OSError('extent_count != mapped_extents. file is modified?')

        extents = [Extent(e.fe_logical, e.fe_physical, e.fe_length, e.fe_flags) for e in ptr.fm_extents[0:extent_count]]

        if extents and ExtentFlag.LAST not in extents[-1].flags:
            raise IOError('The last extent does not have the last flag')

        return extents


def _print_extent_tuple(blocksize, index, extent_):
    fm_logical = extent_.logical
    fm_physical = extent_.physical
    fm_length = extent_.length

    assert fm_logical % blocksize == 0, (index, fm_logical)
    assert fm_physical % blocksize == 0, (index, fm_physical)
    assert fm_length % blocksize == 0, (index, fm_length)

    return (
        index,
        fm_logical // blocksize,
        (fm_logical + fm_length - 1) // blocksize,
        fm_physical // blocksize,
        (fm_physical + fm_length - 1) // blocksize,
        fm_length // blocksize,
    )


def print_extents(
    extents: List[Extent],
    filesize: int = None,
    *,
    blocksize=4096,
    print=print,
    start=0,
):
    if blocksize <= 0:
        raise ValueError
    numbers = list(_print_extent_tuple(blocksize, i, e) for i, e in enumerate(extents, start))
    widths = [len(str(max(s))) for s in zip(*numbers)]

    fmt = f'{{0:{widths[0]}d}}: {{1:{widths[1]}d}}..{{2:{widths[2]}d}}: {{3:{widths[3]}d}}..{{4:{widths[4]}d}}: {{5:{widths[5]}d}}'

    for offsets, extent in zip(numbers, extents):
        flags = [
            k.lower()
            for k, v in ExtentFlag.__members__.items()
            if (extent.flags & v) == v
        ]

        if filesize is not None and extent.logical < filesize <= extent.logical + extent.length:
            flags.append('eof')

        print('%s %s' % (
            fmt.format(*offsets),
            ','.join(flags),
        ))


def main():
    from argparse import ArgumentParser
    p = ArgumentParser()
    p.add_argument('--blocksize', type=int, default=4096)
    p.add_argument('file')
    ns = p.parse_args()

    import os

    with open(ns.file, 'rb') as f:
        file_size = os.fstat(f.fileno()).st_size
        extents = list(fiemap(f.fileno()))
        print_extents(
            extents,
            file_size,
        )


if __name__ == '__main__':
    main()
