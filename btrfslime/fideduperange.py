from __future__ import annotations

import dataclasses
import os
from contextlib import ExitStack
from dataclasses import dataclass
from typing import AnyStr, List, Tuple, Union, Sequence

from ._fs import ffi, lib
from .fs import build_error, ioctl
from .util import check_nonnegative

__all__ = ['fideduperange', 'dedupe_files', 'DedupeError', 'DedupeTask']

_ARG_SIZE = 1024  # Limit by fcntlmodules.c, and the file_dedupe_range struct must not greater than a page
_INFO_LEN_MAX = (_ARG_SIZE - ffi.sizeof('struct file_dedupe_range')) // ffi.sizeof('struct file_dedupe_range_info')

FILE_DEDUPE_RANGE_DIFFERS = lib.FILE_DEDUPE_RANGE_DIFFERS
FILE_DEDUPE_RANGE_SAME = lib.FILE_DEDUPE_RANGE_SAME


class DedupeError(OSError):
    bytes_deduped: int = 0
    index: int = None
    status: int = None

    def __init__(self, *args, bytes_deduped: int = 0, index: int = None, status: int = None):
        super().__init__(*args)
        self.bytes_deduped = bytes_deduped
        self.index = index
        self.status = status


class DedupeHelper:
    """
    Reusable one destination deduplication helper

    length should be <= 16MB because kernel may not support larger size.
    """

    def __init__(self, src_fd: int, src_offset: int, dst_fd: int, dst_offset: int, length: int):
        self.src_fd = src_fd
        self.argp = ffi.new('struct file_dedupe_range*', {
            'dest_count': 1,
            'info': 1,
        })
        self.src_fd = src_fd
        self.src_offset = src_offset
        self.dst_fd = dst_fd
        self.dst_offset = dst_offset
        self.length = length

    @property
    def src_offset(self) -> int:
        return self.argp.src_offset

    @src_offset.setter
    def src_offset(self, offset: int):
        self.argp.src_offset = offset

    @property
    def dst_fd(self) -> int:
        return self.argp.info[0].dest_fd

    @dst_fd.setter
    def dst_fd(self, fd: int):
        self.argp.info[0].dest_fd = fd

    @property
    def dst_offset(self) -> int:
        return self.argp.info[0].dest_offset

    @dst_offset.setter
    def dst_offset(self, offset: int):
        self.argp.info[0].dest_offset = offset

    @property
    def length(self) -> int:
        return self.argp.src_length

    @length.setter
    def length(self, length: int):
        self.argp.src_length = length

    def dedupe(self) -> int:
        """
        :return: the number of bytes deduped
        """
        ret = ioctl(self.src_fd, lib.FIDEDUPERANGE, self.argp)
        assert ret >= 0, ret
        info = self.argp.info[0]

        if info.status == lib.FILE_DEDUPE_RANGE_SAME:
            return info.bytes_deduped

        msg = None
        if info.status == lib.FILE_DEDUPE_RANGE_DIFFERS:
            msg = 'Data being deduplicated are different'

        raise build_error(
            -info.status if info.status < 0 else 0,
            msg,
            exc=DedupeError,
            status=info.status,
            bytes_deduped=info.bytes_deduped,
            index=0,
        )


def fideduperange(
    src_fd: int,
    src_offset: int,
    src_length: int,
    dests: Sequence[Tuple[int, int]],
    *,
    chunksize: int = None,
):
    check_nonnegative('src_fd', src_fd)
    check_nonnegative('src_offset', src_offset)

    if chunksize is None:
        chunksize = 16 * 1024 ** 2  # 16 MB

    check_nonnegative('chunksize', chunksize)

    # Copy the dests
    dests = [tuple(s) for s in dests]

    for dst_index, (dst_fd, dst_offset) in enumerate(dests):
        check_nonnegative('dst_fd', dst_fd)
        check_nonnegative('dst_offset', dst_offset)

    batch_size = min(len(dests), _INFO_LEN_MAX)
    ptr = ffi.new('struct file_dedupe_range*', {
        'info': batch_size,
    })

    # Group dests by batch size
    batch_groups = []
    for dest_start in range(0, len(dests), batch_size):
        # Assign each dest an info
        group = []
        for dst_index, (dst_fd, dst_offset) in enumerate(dests[dest_start:dest_start + batch_size], dest_start):
            group.append((dst_index, dst_fd, dst_offset, ptr.info[dst_index % batch_size]))
        batch_groups.append(group)

    file_dedupe_range_same = lib.FILE_DEDUPE_RANGE_SAME

    left = src_length
    bytes_deduped = 0

    while left > 0:
        # Repeatly dedupe until zero bytes left
        task_size = min(chunksize, left)
        task_deduped = task_size

        ptr.src_offset = src_offset + bytes_deduped
        ptr.src_length = task_size

        for group in batch_groups:
            ptr.dest_count = len(group)
            for dst_index, dst_fd, dst_offset, info in group:
                info.dest_fd = dst_fd
                info.dest_offset = dst_offset + bytes_deduped

            ret = ioctl(src_fd, lib.FIDEDUPERANGE, ptr)
            assert ret == 0, ret

            for dst_index, dst_fd, dst_offset, info in group:
                if info.bytes_deduped < task_deduped:
                    task_deduped = info.bytes_deduped

                if info.status == file_dedupe_range_same:
                    continue

                if dst_index + 1 == len(dests):
                    # increase the total bytes deduped if this is the last dest
                    bytes_deduped += info.bytes_deduped

                raise DedupeError(
                    info.status,
                    f"Error {info.status} caused by dest {dst_index}",
                    bytes_deduped=bytes_deduped,
                    status=info.status,
                    index=dst_index,
                )

        assert task_deduped > 0 and task_deduped == task_size, (task_size, task_deduped)
        assert task_deduped <= left, (task_deduped, left)

        bytes_deduped += task_deduped
        left -= task_deduped
    assert bytes_deduped == src_length and left == 0, (bytes_deduped, left, src_length)

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

            dests = []
            for target, target_offset in self.targets:
                dst_fd = stack.enter_context(open(target, 'r+b', closefd=not isinstance(target, int))).fileno()
                dests.append((dst_fd, target_offset))

            return fideduperange(src_fd, src_offset, src_length, dests, chunksize=self.chunksize)


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
