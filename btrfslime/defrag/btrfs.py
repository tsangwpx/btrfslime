from __future__ import annotations

import os
import subprocess
from typing import AnyStr

from ..util import check_nonnegative

BTRFS_BIN = '/bin/btrfs'


def file_defrag(
    target: AnyStr,
    start: int = None,
    size: int = None,
    extent_size: int = None,
    *,
    flush=False,
    btrfs_bin=BTRFS_BIN,
):
    if isinstance(target, bytes):
        target = os.fsdecode(target)

    defrag_args = [btrfs_bin, 'filesystem', 'defrag']

    if start is not None:
        check_nonnegative('start', start)
        defrag_args.extend(('-s', str(start)))

    if size is not None:
        check_nonnegative('size', size)
        defrag_args.extend(('-l', str(size)))

    if extent_size is not None:
        check_nonnegative('extent_size', extent_size)
        defrag_args.extend(('-t', str(extent_size)))

    if flush:
        defrag_args.append('-f')

    defrag_args.append(os.fspath(target))
    subprocess.check_call(defrag_args)
