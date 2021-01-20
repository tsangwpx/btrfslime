from __future__ import annotations

import asyncio
import bisect
import collections
import contextlib
import dataclasses
import hashlib
import itertools
import logging
import os
import threading
import time
from argparse import ArgumentParser
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from functools import partial, lru_cache
from operator import attrgetter
from sqlite3 import sqlite_version_info
from typing import List, Tuple, Set, ContextManager, AnyStr, Sequence, Optional, Dict, Callable, Awaitable

from sqlalchemy import create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.orm import Session, sessionmaker
from tqdm import tqdm

from .hashing import salted_algo
from .model import Base, File, MissingId
from .. import _xxhash
from ..async_util import pull_tasks
from ..fideduperange import DedupeError, FILE_DEDUPE_RANGE_DIFFERS, DedupeHelper
from ..fiemap import Extent, ExtentFlag, fiemap
from ..scantree import ExecutorTreeScanner, iterate_files
from ..util import chunkize

# This number is limited by SQLITE_LIMIT_VARIABLE_NUMBER
_SAFE_VARIABLE_NUMBER = 950 if sqlite_version_info < (3, 32, 0) else 32000


@dataclass
class _DedupeTuple:
    path_index: int
    path: str
    fd: int
    size: int
    extents: List[Extent] = dataclasses.field(repr=False)
    extent_keys: List[int] = dataclasses.field(repr=False)


def _path_key(s: str) -> int:
    """Hash a str to an int"""
    encoded = s.encode('utf-16-be', 'surrogateescape')
    return _xxhash.lib.calc_key(encoded, len(encoded))


def _extent_distribution_key(extents: List[Extent]):
    """Sort key according to extent distribution"""

    attr_length: Callable[..., int] = attrgetter('length')
    shared_flag = ExtentFlag.SHARED
    shared_extents = [e for e in extents if shared_flag in e.flags]
    shared_size = sum(map(attr_length, shared_extents))
    avg_shared_size = shared_size / len(shared_extents) if shared_extents else 0

    return (
        -shared_size,  # larger is better
        -avg_shared_size,  # larger is better
        len(extents),  # less is better
        -min(map(attr_length, extents)),  # larger least extent is better
    )


def _group_by_hashes(hashes: Sequence[Tuple[bytes, ...]], eps: int, minimum: int = 2) -> Tuple[List[List[int]], Set[int]]:
    """
    Implement a modified DBSCAN to group files into clusters

    In general, we should hash every extents of files and dedupe the same ones.
    However, hashing all files and storing all their hashes may not practical.
    Instead we hash the first few blocks of files and cluster files into groups

    :param hashes: A sequence of 3-tuples of bytes
    :param eps min number of equal hashes to treat two files the same, depend on file size
    :param minimum should be 2
    """
    total = len(hashes)
    assigned: List[int] = [False] * total
    clusters: List[List[int]] = []
    noises = set()

    @lru_cache(maxsize=None)
    def same_file(p, q):
        """Tell whether two files are considered the same"""
        # list comprehension seem faster than sum()
        return len([True for s, t in zip(hashes[p], hashes[q]) if s and s == t]) >= eps

    @lru_cache(maxsize=None)
    def find_neighbors(p):
        """
        Return the indices of neighbor files around a file index
        """
        return [q for q in range(total) if (same_file(p, q) if p <= q else same_file(q, p))]

    for i in range(total):
        if assigned[i]:  # Skip assigned index
            continue

        neighbors = find_neighbors(i)
        if len(neighbors) < minimum:  # Not enough neighbors
            noises.add(i)
            continue

        assigned[i] = True
        group = [i]
        clusters.append(group)

        pending = collections.deque([s for s in neighbors if not assigned[s]])

        while pending:
            j = pending.popleft()
            if assigned[j]:  # Skip assigned index
                continue

            assigned[j] = True
            group.append(j)

            if j in noises:
                noises.remove(j)
                continue

            neighbors = find_neighbors(j)
            if len(neighbors) < minimum:
                continue
            pending.extend([s for s in neighbors if not assigned[s]])

    return clusters, noises


def _compute_hashes(path: AnyStr, blocksize: int, count: int) -> List[bytes]:
    hashes: List[Optional[bytes]] = [None] * count

    with open(path, 'rb', buffering=0) as f:
        size = os.stat(f.fileno()).st_size
        step = max(blocksize, size // blocksize // (count + 1) * blocksize)  # align step to blocksize

        offset = 0
        for i in range(count):
            if i and offset + blocksize > size:
                break

            f.seek(offset)
            h = salted_algo(hashlib.sha1, i)
            h.update(f.read(blocksize))
            hashes[i] = h.digest()
            offset += step

        return hashes


class Runner:
    def __init__(
        self,
        roots: List[str],
        db_file: str,
        skip_scan: bool = False,
        skip_hash: bool = False,
        skip_dedupe: bool = False,
        reset_done: bool = False,
        reset_hash: bool = False,
    ):
        self.logger = logging.getLogger(__name__)
        self.roots = roots
        self.db_file = db_file
        self.skip_scan = skip_scan
        self.skip_hash = skip_hash
        self.skip_dedupe = skip_dedupe
        self.reset_done = reset_done
        self.reset_hash = reset_hash
        self.file_size_min = 4096

        self.__key_conflicts = 0

        # Number of workers
        self.scan_workers = 4
        self.hash_workers = 2
        self.dedupe_workers = 2

        self._init_database()

    def _init_database(self):
        self._engine = create_engine(
            f'sqlite:///{self.db_file}?timeout=60',
            # echo=True,
        )

        @listens_for(self._engine, 'connect')
        def set_sqlite_pragma(connection, connection_record):
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.close()

        Base.metadata.create_all(bind=self._engine)
        self._session_cls = sessionmaker(bind=self._engine)

    @contextlib.contextmanager
    def _scoped_session(self, **kwargs) -> ContextManager[Session]:
        kwargs.setdefault('expire_on_commit', False)  # Disable auto load after flush
        db: Session = self._session_cls(**kwargs)
        try:
            yield db
        finally:
            db.close()

    def _progress_bar(self, **kwargs):
        kwargs.setdefault('mininterval', 0.5)
        kwargs.setdefault('maxinterval', 60)
        kwargs.setdefault('miniters', 1)
        kwargs.setdefault('smoothing', 0.1)

        return tqdm(**kwargs)

    async def _do_scan(self):
        from sqlalchemy import delete, insert

        with ExitStack() as exit_stack:
            db = exit_stack.enter_context(self._scoped_session())

            # Setup missing id table
            db.execute(delete(MissingId))
            db.execute(insert(MissingId).from_select([File.id], db.query(File.id).subquery()))
            db.commit()

            # io-intensive executor
            scan_executor = exit_stack.enter_context(ThreadPoolExecutor(self.scan_workers))
            scanner = exit_stack.enter_context(ExecutorTreeScanner(executor=scan_executor))

            file_count = db.query(File).count()
            pbar = exit_stack.enter_context(self._progress_bar(total=file_count, desc='scanning', unit=''))
            pbar.refresh()

            # General executor to bridge sync and async functions
            executor_run = partial(asyncio.get_running_loop().run_in_executor, None)

            async def produce():
                sentinel = object()
                it = chunkize(iterate_files(self.roots, scanner=scanner), _SAFE_VARIABLE_NUMBER)

                for task_id in itertools.count():
                    batch: List[Tuple[str, os.stat_result]]
                    batch = await executor_run(next, it, sentinel)
                    if batch is sentinel:
                        break
                    yield task_id, batch

            def consume(work) -> Awaitable:
                task_id, batch = work
                return executor_run(self._scan_batch, task_id, batch)

            # use 2 workers due to SQLite
            async for result in pull_tasks(produce(), consume, 2):
                pbar.update(result)

            pbar.total = pbar.n
            pbar.refresh()

            pbar.set_description('finishing')

            db.execute(delete(File).where(File.id.in_(db.query(MissingId.id).subquery())))
            db.execute(delete(MissingId))
            db.commit()

            self.logger.debug('Key conflict rate %d/%d (%.1f%%)', self.__key_conflicts, pbar.total, self.__key_conflicts / pbar.total * 100)

    def _scan_batch(self, task_id: int, stat_list: List[Tuple[str, os.stat_result]]) -> int:
        """
        Add / update rows into the database according to the stat list

        :param task_id: for debug purpose
        :param stat_list: a list of (path, os.stat_result) tuples
        :return: the number of found files == len(stat_list)
        """
        file_size_min = self.file_size_min
        table: Dict[str, Tuple[os.stat_result, int]] = {
            s: (t, _path_key(s)) for s, t in stat_list if t.st_size >= file_size_min
        }
        keys: List[int] = [t for _, t in table.values()]

        existing_ids: List[int] = []
        changes: List[File] = []

        # According to EXPLAIN QUERY PLAN, filter by (path, path_key) tuple did NOT use the path_key index
        # so use the simple IN filtering plus a Python dict to balance time and (disk) space.
        with self._scoped_session() as db:
            files: List[File] = db.query(File).filter(File.path_key.in_(keys)).all()
            db.expunge_all()

            for file in files:
                try:
                    path_stat, _ = table.pop(file.path)
                except KeyError:
                    self.__key_conflicts += 1
                    continue
                else:
                    existing_ids.append(file.id)

                    if file.size == path_stat.st_size and file.mtime == path_stat.st_mtime:
                        # Skip unchanged entry
                        continue

                    file.size = path_stat.st_size
                    file.mtime = path_stat.st_mtime
                    file.hash1 = file.hash2 = file.hash3 = None  # reset hashes
                    file.done = False  # reset done flag
                    changes.append(file)

            for path, (path_stat, path_key) in table.items():
                file = File(
                    path=path,
                    path_key=path_key,
                    size=path_stat.st_size,
                    mtime=path_stat.st_mtime,
                )
                changes.append(file)

            db.bulk_save_objects(changes, preserve_order=False)
            db.query(MissingId).filter(MissingId.id.in_(existing_ids)).delete(synchronize_session=False)
            db.commit()

        return len(stat_list)

    async def _do_hash(self):
        from sqlalchemy import func

        hash_file = partial(_compute_hashes, blocksize=1024 ** 2, count=3)

        with ExitStack() as exit_stack:
            db = exit_stack.enter_context(self._scoped_session())

            executor = exit_stack.enter_context(ThreadPoolExecutor(self.hash_workers))
            executor_run = partial(asyncio.get_running_loop().run_in_executor, executor)

            subquery = (db.query(File.size)
                        .filter(File.size >= self.file_size_min)
                        .group_by(File.size)
                        .having(func.count() >= 2)
                        .subquery())

            total = db.query(File).filter(
                File.size.in_(subquery),
                File.hash1.is_(None),
                # the above condition is enough to find un-hashed entries
                # File.hash1.is_(None) | ((File.size >= _SIZE2M) & File.hash2.is_(None)) | ((File.size >= _SIZE3M) & File.hash3.is_(None)),
            ).count()

            pbar = exit_stack.enter_context(self._progress_bar(total=total, desc='hashing', unit=''))
            pbar.refresh()

            async def produce():
                minimum_id: int = 0

                while True:
                    work = db.query(File).filter(
                        File.id >= minimum_id,
                        File.size.in_(subquery),
                        File.hash1.is_(None),
                    ).order_by(
                        File.id,
                    ).limit(1000).all()
                    db.expunge_all()

                    if not work:
                        break

                    for item in work:
                        yield item

                    # noinspection PyTypeChecker
                    minimum_id = max(map(attrgetter('id'), work)) + 1

            async def consume(file_: File):
                try:
                    file_.hash1, file_.hash2, file_.hash3 = await executor_run(hash_file, file_.path)
                except (FileNotFoundError, PermissionError) as ex:
                    self.logger.error('hash error: %s', ex)
                    file_ = None

                pbar.update()
                return file_

            def save_pending():
                if not pending:
                    return

                db.bulk_save_objects(pending, preserve_order=False)
                db.commit()
                pending.clear()

            pending = collections.deque()
            timeout = time.monotonic() + 30  # every 30 seconds
            threshold = _SAFE_VARIABLE_NUMBER // 4  # Columns: id, hash1, hash2, hash3

            async for file in pull_tasks(produce(), consume, self.hash_workers + 1):
                if file:
                    pending.append(file)

                if len(pending) >= threshold or time.monotonic() >= timeout:
                    save_pending()
                    timeout = time.monotonic() + 30

            save_pending()

    async def _do_dedupe(self):
        from sqlalchemy import func

        with ExitStack() as exit_stack:
            db: Session = exit_stack.enter_context(self._scoped_session())

            executor = exit_stack.enter_context(ThreadPoolExecutor(self.dedupe_workers))
            executor_run = partial(asyncio.get_running_loop().run_in_executor, executor)

            # Find same-size groups which their member files are updated / unprocessed
            counts_and_sizes = (db.query(func.count(), File.size)
                                .filter(File.size >= self.file_size_min, File.hash1.isnot(None))
                                .group_by(File.size)
                                .having((func.count() >= 2) & (func.count() != func.sum(File.done)))
                                .all())

            total_size = sum([count * size for count, size in counts_and_sizes])
            all_sizes = [s for _, s in counts_and_sizes]
            all_sizes.sort(reverse=True)

            pbar = exit_stack.enter_context(self._progress_bar(total=total_size, desc='progress', unit='B', unit_scale=True, unit_divisor=1024))
            pbar.refresh()

            async def produce():
                task_id = 0

                for group_size in all_sizes:
                    files = (db.query(File)
                             .filter(File.size == group_size)
                             .order_by(File.path)  # better ordering
                             .all())
                    db.expunge_all()

                    # block size is 1MB, so require 2 identical hashes for files not less than 3MB
                    same_hash_min = 2 if group_size >= 3 * 1024 ** 2 else 1
                    hashes = [(s.hash1, s.hash2, s.hash3) for s in files]

                    clusters, noises = _group_by_hashes(hashes, same_hash_min)

                    if noises:
                        pending.extend([files[s] for s in noises])
                        pbar.total -= len(noises) * group_size
                        pbar.refresh()

                    for group in clusters:
                        yield task_id, group_size, [files[s] for s in group]
                        task_id += 1

            async def consume(work):
                task_id, group_size, files = work
                try:
                    deduped_bytes = await executor_run(self._dedupe_group, task_id, event, [s.path for s in files])
                except Exception as ex:
                    self.logger.error('task %d error: %s', task_id, ex)

                    # Ignore OSError
                    if not isinstance(ex, OSError):
                        raise

                    # Do not add them to pending
                    deduped_bytes = 0
                    pbar.total -= len(files) * group_size
                    pbar.refresh()
                else:
                    pending.extend(files)
                    pbar.update(len(files) * group_size)

                return deduped_bytes

            def save_pending():
                if not pending:
                    return

                for file in pending:
                    file.done = True

                db.bulk_save_objects(pending, preserve_order=False)
                db.commit()
                pending.clear()

            event = threading.Event()  # just a mutable object (use list is fine)
            pending = collections.deque()
            threshold = 100
            timeout = time.monotonic() + 30
            total_bytes_deduped = 0

            try:
                async for bytes_deduped in pull_tasks(produce(), consume, self.dedupe_workers):
                    total_bytes_deduped += bytes_deduped

                    if len(pending) >= threshold or time.monotonic() >= timeout:
                        save_pending()
                        timeout = time.monotonic() + 30

                save_pending()

                self.logger.info('deduped %.1f MB', total_bytes_deduped / 1024 ** 2)
            finally:
                event.set()

    def _dedupe_open_file(self, task_id, path_index: int, path: str) -> Optional[_DedupeTuple]:
        """
        Open a path for read-write and return a _DedupleTupel if succeed or None if error is silenced.

        :param task_id: task id
        :param path_index: path index
        :param path: path
        """
        attr_logical: Callable[..., int] = attrgetter('logical')

        dt = None
        fd = os.open(path, os.O_RDWR | os.O_NOATIME | os.O_CLOEXEC)

        try:
            extents = fiemap(fd)
            extents.sort(key=attr_logical)

            if not extents:
                self.logger.info('task %d path %d is ignored due to zero extents', task_id, path_index)
                return

            if ExtentFlag.LAST not in extents[-1].flags:
                self.logger.info('task %d path %d is ignored due to lack of LAST flag', task_id, path_index)
                return

            # Make sure the file is continuous
            expect_logical = 0

            # Path with bad extent flags are reported and skipped
            bad_flags = ~(ExtentFlag.LAST | ExtentFlag.SHARED)

            for ext in extents:
                if expect_logical != ext.logical:
                    self.logger.info('task %d path %d is ignored due to discontinuity at %d', task_id, path_index, expect_logical)
                    return

                bad_flags = bad_flags & ext.flags
                if bad_flags:
                    self.logger.info('task %d path %d is ignored due to bad flags %r at %d', task_id, path_index, bad_flags, ext.logical)
                    return

                expect_logical = ext.logical_stop

            last_extent = extents[-1]
            size = os.fstat(fd).st_size
            assert last_extent.logical < size <= last_extent.logical_stop, (size, last_extent)

            extent_keys = list(map(attr_logical, extents))
            dt = _DedupeTuple(
                path_index=path_index,
                path=path,
                fd=fd,
                size=size,
                extents=extents,
                extent_keys=extent_keys,
            )
            return dt
        finally:
            if dt is None and fd >= 0:
                # Something wrong
                os.close(fd)

    def _dedupe_extent(self, task_id: int, event: threading.Event, null_fd: int, extent: Extent, src: _DedupeTuple, dst: _DedupeTuple) -> int:
        """
        Dedupe an Extent and return the number of bytes deduped

        Note that the returned number may be smaller than the extent size
        """

        offset = extent.logical
        seek_end = True

        if seek_end:
            # Adjust the offset by avoiding physical overlapping
            # Find a sub-extent from dst extents contained in the src extent
            # There are at least two possible sub-extent choices:
            # 1. The sub-extent aligned with the start of the source extent. (Safe)
            # 2. Last sub-extent possibly represents the last interrupted deduplication location. (Faster, usually accurate, current implementation)
            subextent = None
            ext_index = bisect.bisect_left(dst.extent_keys, extent.logical)
            for ext in dst.extents[ext_index:]:
                if ext.logical >= ext.logical_stop:  # out of range
                    break
                if extent.physical < ext.physical_stop <= extent.physical_stop:
                    subextent = ext

            if subextent:
                assert extent.logical <= subextent.logical < extent.logical_stop

                # Use the end of the sub-extent as offset
                offset = src.size if ExtentFlag.LAST in subextent.flags else subextent.logical_stop

        # If LAST flag present, the valid data size <= the extent size
        length = src.size - offset if ExtentFlag.LAST in extent.flags else extent.logical_stop - offset
        assert length >= 0

        if not length:
            return 0

        chunksize = chunksize_max = 8 * 1024 ** 2
        chunksize_min = 1 * 1024 ** 2
        helper = DedupeHelper(src.fd, offset, dst.fd, offset, length)
        left = length

        os.posix_fadvise(src.fd, offset, left, os.POSIX_FADV_SEQUENTIAL)
        os.posix_fadvise(dst.fd, offset, left, os.POSIX_FADV_SEQUENTIAL)

        while left > 0:
            if event.is_set():
                break

            advise_size = min(left, chunksize * 2)
            os.posix_fadvise(src.fd, offset, advise_size, os.POSIX_FADV_WILLNEED)
            os.posix_fadvise(dst.fd, offset, advise_size, os.POSIX_FADV_WILLNEED)

            step_size = min(left, chunksize)
            # Read in advance to hopefully cache the contents in memory
            # QUESTION: Would sendfile raise?
            os.sendfile(null_fd, dst.fd, offset, step_size)
            os.sendfile(null_fd, src.fd, offset, step_size)

            try:
                helper.src_offset = helper.dst_offset = offset
                helper.length = step_size
                step_deduped = helper.dedupe()
            except DedupeError as ex:
                if ex.status != FILE_DEDUPE_RANGE_DIFFERS:
                    # Something else
                    raise

                if ex.bytes_deduped is None:
                    step_deduped = chunksize_min  # minimum skip
                else:
                    # round up bytes_deduped to the least positive multiple of chunk_size_min
                    step_deduped = max(chunksize_min, -(-ex.bytes_deduped // chunksize_min) * chunksize_min)

                step_deduped = min(step_deduped, left)  # avoid left < 0 in future
                chunksize = chunksize_min  # reduce chunksize

                self.logger.warning(
                    'task %d src %d dst %d are different at %d',
                    task_id, src.path_index, dst.path_index, offset + (ex.bytes_deduped or 0),
                )
            else:
                chunksize = min(chunksize * 2, chunksize_max)  # speed up if possible

            # Discard what have passed from dst while keeping ones from src
            os.posix_fadvise(dst.fd, offset, step_deduped, os.POSIX_FADV_DONTNEED)

            offset += step_deduped
            left -= step_deduped

        return length - left

    def _dedupe_group(self, task_id: int, event: threading.Event, paths: List[str]):
        """
        Given a list of paths, sort them using their extent distribution
        Choose the good-looking one as the source file

        :param task_id: for logging purpose
        :param event: event is set if interrupted
        :param paths: a list of file names to dedupe
        """

        start_time = time.monotonic()

        deduped_bytes = 0
        perf_timing = 0

        with ExitStack() as exit_stack:
            # store valid paths and their relatives in a list of quadruplets
            dedupe_tuples = []
            sizes = set()

            for path_index, path in enumerate(paths):
                if event.is_set():
                    dedupe_tuples.clear()
                    break

                self.logger.debug('task %d path %d is %s', task_id, path_index, path)

                try:
                    dt = self._dedupe_open_file(task_id, path_index, path)
                except (FileNotFoundError, PermissionError) as ex:
                    self.logger.warning('task %d path %d is inaccessible: %s', task_id, path_index, repr(ex))
                    continue

                if dt is None:
                    # Something wrong happened silently
                    continue

                exit_stack.callback(os.close, dt.fd)
                dedupe_tuples.append(dt)
                sizes.add(dt.size)

            if len(sizes) >= 2:
                # The files got modified somehow
                self.logger.warning('task %d is skipped due to file changes, re-run is needed')
                return

            if len(dedupe_tuples) >= 2:
                # FIXME: What if /dev/null missing?
                null_fd = os.open('/dev/null', os.O_WRONLY | os.O_CLOEXEC)
                exit_stack.callback(os.close, null_fd)

                # Sort paths by extent distribution
                dedupe_tuples.sort(key=lambda s: _extent_distribution_key(s.extents))
                src = dedupe_tuples[0]

                self.logger.debug('task %d choose path %d as src', task_id, src.path_index)

                for extent in src.extents:
                    if event.is_set():
                        break

                    for dst in dedupe_tuples[1:]:
                        if event.is_set():
                            break

                        t0 = time.perf_counter()
                        deduped_bytes += self._dedupe_extent(task_id, event, null_fd, extent, src, dst)
                        perf_timing += time.perf_counter() - t0

        deduped_mb = deduped_bytes / 1024 ** 2
        used_time = time.monotonic() - start_time

        if perf_timing > 0:
            self.logger.debug(
                'task %d deduped %.1fMB (%.2f MB/s) in %.2fs',
                task_id, deduped_mb, deduped_mb / perf_timing, used_time,
            )
        else:
            self.logger.debug('task %d done in %.2fs', task_id, used_time)

        return deduped_bytes

    async def run(self):
        if self.reset_done or self.reset_hash:
            with self._scoped_session() as db:
                if self.reset_done:
                    db.query(File).update({
                        File.done: False,
                    })

                if self.reset_hash:
                    db.query(File).update({
                        File.hash1: None,
                        File.hash2: None,
                        File.hash3: None,
                    })

                db.commit()

        if not self.skip_scan:
            await self._do_scan()

        if not self.skip_hash:
            await self._do_hash()

        if not self.skip_dedupe:
            await self._do_dedupe()


def parser():
    p = ArgumentParser()
    p.add_argument('--verbose', '-v', action='count', default=0)
    p.add_argument(
        '--db-file', default='dedupe.db',
        help='Cache database file for file hashes (default: dedupe.db)',
    )
    p.add_argument(
        '--skip-scan',
        action='store_true', default=False,
        help='Skip file scanning phase',
    )
    p.add_argument(
        '--skip-hash',
        action='store_true', default=False,
        help='Skip file hashing phase',
    )
    p.add_argument(
        '--skip-dedupe',
        action='store_true', default=False,
        help='Skip dedupe phase',
    )
    p.add_argument(
        '--reset-done',
        action='store_true', default=False,
        help='Reset the done flag',
    )
    p.add_argument(
        '--reset-hash',
        action='store_true', default=False,
        help='Reset the computed hash values',
    )
    p.add_argument(
        '--no-purge-missing',
        action='store_false', default=True, dest='purge_missing',
        help='Do not purge missing entries from the database',
    )
    p.add_argument(
        'roots',
        nargs='+',
        help='Files / directories that need dedupe',
    )
    return p


def main():
    p = parser()
    ns = p.parse_args()

    if ns.verbose >= 2:
        log_level = logging.DEBUG
    elif ns.verbose >= 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    logging.basicConfig(level=log_level)

    logger = logging.getLogger('dedupe')
    logger.debug('%r', ns)

    runner = Runner(
        roots=ns.roots,
        db_file=ns.db_file,
        skip_scan=ns.skip_scan,
        skip_hash=ns.skip_hash,
        skip_dedupe=ns.skip_dedupe,
        reset_done=ns.reset_done,
        reset_hash=ns.reset_hash,
    )
    asyncio.run(runner.run())


if __name__ == '__main__':
    main()
