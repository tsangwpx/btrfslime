from __future__ import annotations

import dataclasses
import fcntl
import os
from contextlib import ExitStack
from dataclasses import dataclass
from typing import AnyStr, List, Tuple, Union

from ._fs import ffi, lib
from .util import check_nonnegative

__all__ = ['fideduperange', 'dedupe_files', 'DedupeError', 'DedupeTask']

SRC_SIZE = ffi.sizeof('struct file_dedupe_range')
DST_SIZE = ffi.sizeof('struct file_dedupe_range_info')
PAGE_SIZE = 4096
DST_COUNT_MAX = (PAGE_SIZE - SRC_SIZE) // DST_SIZE  # The whole file_dedupe_range must be smaller than a page size, stated by the man page


class DedupeError(OSError):
    bytes_deduped: int = 0


def fideduperange(src_fd: int, src_offset: int, src_length: int, *args: int, chunksize: int = None) -> int:
    check_nonnegative('src_fd', src_fd)
    check_nonnegative('src_offset', src_offset)

    dst_count, error = divmod(len(args), 2)

    if dst_count <= 0:
        raise TypeError("dst_fd and dst_offset are required")

    if error:
        raise TypeError("dst_fd and dst_offset must be present in pairs")

    if chunksize is not None and chunksize <= 0:
        raise ValueError(f"'chunksize' must be positive")

    dsts = [(i, s, t) for i, (s, t) in enumerate(zip(args[0::2], args[1::2]))]

    for dst_index, dst_fd, dst_offset in dsts:
        check_nonnegative('dst_fd', dst_fd)
        check_nonnegative('dst_offset', dst_offset)

    ptr = ffi.new('struct file_dedupe_range*', {
        'info': min(dst_count, DST_COUNT_MAX),
    })
    pybuf = ffi.buffer(ptr)

    bytes_deduped = 0
    while bytes_deduped < src_length:
        bytes_deduped_loop = src_length - bytes_deduped

        if chunksize is not None:
            bytes_deduped_loop = min(bytes_deduped_loop, chunksize)

        for dst_group in [dsts[i:i + DST_COUNT_MAX] for i in range(0, dst_count, DST_COUNT_MAX)]:
            ptr.src_offset = src_offset + bytes_deduped
            ptr.src_length = bytes_deduped_loop
            ptr.dest_count = len(dst_group)

            for (dst_index, dst_fd, dst_offset), info in zip(dst_group, ptr.info):
                info.dest_fd = dst_fd
                info.dest_offset = dst_offset + bytes_deduped

            fcntl.ioctl(src_fd, lib.FIDEDUPERANGE, pybuf)

            for (dst_index, dst_fd, dst_offset), info in zip(dst_group, ptr.info):
                if info.status == lib.FILE_DEDUPE_RANGE_SAME:
                    if info.bytes_deduped != bytes_deduped_loop:
                        ex = DedupeError(
                            f"Unexpected number of bytes deduped in dest {dst_index} "
                            f"(offset={info.dest_offset}, bytes_deuped={info.bytes_deduped} != {bytes_deduped_loop})"
                        )
                        ex.bytes_deduped = info.bytes_deduped
                        raise
                    continue
                elif info.status == lib.FILE_DEDUPE_RANGE_DIFFERS:
                    ex = DedupeError(f"Failed to dedupe dest {dst_index} ({info.dest_offset} + {info.bytes_deduped})")
                    ex.bytes_deduped = info.bytes_deduped
                    raise ex
                else:
                    raise AssertionError(f"Unknown status code {info.status}")

        bytes_deduped += bytes_deduped_loop

    return bytes_deduped


@dataclass
class DedupeTask:
    source: Union[AnyStr, os.PathLike, int]
    offset: int = 0
    length: int = None
    targets: List[Tuple[Union[AnyStr, os.PathLike, int], int]] = dataclasses.field(default_factory=list)
    chunksize: int = None

    def add_target(self, target: Union[AnyStr, os.PathLike, int], offset: int = 0):
        self.targets.append((target, offset))

    def dedupe(self):
        with ExitStack() as stack:
            src_fd = stack.enter_context(open(self.source, 'rb', closefd=not isinstance(self.source, int))).fileno()
            src_offset = self.offset
            src_length = self.length
            if src_length is None:
                src_length = os.stat(src_fd).st_size

            args = []
            for target, target_offset in self.targets:
                dst_fd = stack.enter_context(open(target, 'r+b', closefd=not isinstance(target, int))).fileno()
                args.extend((dst_fd, target_offset))

            return fideduperange(src_fd, src_offset, src_length, *args, chunksize=self.chunksize)


def dedupe_files(src: AnyStr, dests: List[AnyStr]):
    task = DedupeTask(src)

    for target in dests:
        task.add_target(target)

    task.dedupe()


def main():
    from argparse import ArgumentParser

    p = ArgumentParser()
    p.add_argument('source')
    p.add_argument('dests', nargs='+')
    ns = p.parse_args()

    dedupe_files(ns.source, ns.dests)


if __name__ == '__main__':
    main()
