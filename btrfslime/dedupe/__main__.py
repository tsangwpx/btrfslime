from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import hashlib
import itertools
import logging
import operator
import os
import os.path
from contextlib import ExitStack
from functools import partial, lru_cache
from sqlite3 import sqlite_version_info
from typing import List, Tuple, Set, ContextManager, AnyStr, Iterator, Sequence, Optional, Dict

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session, Query
from sqlalchemy.sql import select
from tqdm import tqdm

from .hashing import salted_algo
from .model import Base, File
from ..async_util import executor_run_many
from ..fideduperange import DedupeTask, DedupeError
from ..fiemap import Extent, ExtentFlag, fiemap
from ..scantree import ExecutorTreeScanner, iterate_files
from ..util import chunkize

# This number is limited by SQLITE_LIMIT_VARIABLE_NUMBER
_SAFE_VARIABLE_NUMBER = 950 if sqlite_version_info < (3, 32, 0) else 32000


def path_digest(s: str) -> bytes:
    h = salted_algo(hashlib.sha256)
    h.update(s.encode('utf-16-be'))
    return h.digest()


def key_by_extents(extents: List[Extent]):
    """Sort files according to the distribution of their extents"""
    return (
        len(extents),  # less extents is better
        -sum(1 for e in extents if ExtentFlag.SHARED in e.flags),  # more shared extents are better
        -min(e.length for e in extents),  # larger is better
        -max(e.length for e in extents),  # larger is better
    )


def group_by_hashes(hashes: Sequence[Tuple[bytes, ...]], eps: int, min_neighbors: int = 2) -> List[Set[int]]:
    """
    Implement a modified DBSCAN to group files into clusters

    In general, we should hash every extents of files and dedupe the same ones.
    However, hashing all files and storing all their hashes may not practical.
    Instead we hash the first few blocks of files and cluster files into groups

    :param hashes: A sequence of 3-tuples of bytes
    :param eps min number of equal hashes to treat two files the same, depend on file size
    :param min_neighbors should be 2
    """
    total = len(hashes)
    processed: Set[int] = set()
    clusters: List[Set[int]] = []

    @lru_cache(maxsize=None)
    def same_file(p, q):
        """Tell whether two files are considered the same"""
        # list comprehension seem faster than sum()
        return (p == q) or (eps <= len([
            True for s, t in zip(hashes[p], hashes[q]) if s and s == t
        ]))

    @lru_cache(maxsize=None)
    def find_neighbors(p):
        """
        Return the indices of neighbor files around a file index
        """
        return [q for q in range(total) if (same_file(p, q) if p <= q else same_file(q, p))]

    for i in range(total):
        if i in processed:  # This file has been assigned a group
            continue

        neighbors = find_neighbors(i)
        if len(neighbors) < min_neighbors:  # Not enough neighbors
            continue

        processed.add(i)

        neighbors = neighbors[:]  # copy
        group = {i}
        clusters.append(group)

        # print('Group created: %s' % (i,))

        while neighbors:
            j = neighbors.pop()
            if j in processed:  # This file has been assigned a group
                continue

            processed.add(j)
            group.add(j)
            neighbors2 = find_neighbors(j)
            if len(neighbors2) < min_neighbors:
                continue

            neighbors.extend([i for i in neighbors2 if i not in processed])  # Though the filter is not necessary

        # print('New group: %s' % (sorted(group),))

    return clusters


def fast_digests(path: AnyStr, blocksize: int, count: int) -> List[bytes]:
    hashes: List[Optional[bytes]] = [None] * count

    with open(path, 'rb') as f:
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
        reset_done: bool = False,
        purge_missing: bool = True,
    ):
        self.logger = logging.getLogger(__name__)
        self.roots = roots
        self.db_file = db_file
        self.skip_scan = skip_scan
        self.skip_hash = skip_hash
        self.reset_done = reset_done
        self.purge_missing = purge_missing

        # for heavy io
        self._io_executor = concurrent.futures.ThreadPoolExecutor(4)

        self._init_database()

    def _init_database(self):
        self._engine = create_engine(f'sqlite:///{self.db_file}')

        from sqlalchemy.event import listens_for

        @listens_for(self._engine, 'connect')
        def set_sqlite_pragma(connection, connection_record):
            cursor = connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.close()

        Base.metadata.create_all(bind=self._engine)
        self._session_cls = sessionmaker(bind=self._engine)

    @contextlib.contextmanager
    def _scoped_session(self) -> ContextManager[Session]:
        db = self._session_cls()
        try:
            yield db
        finally:
            db.close()

    async def _scan_files(self):
        missing_ids = set()
        if self.purge_missing:
            # Load all file ids for purging purpose
            with self._scoped_session() as db, tqdm(desc='load database', unit='') as pbar:
                pbar.refresh()

                itemgetter = operator.itemgetter(0)
                result = db.execute(select([File.id]))
                for chunk in iter(lambda: result.fetchmany(10000), []):
                    pbar.update(len(chunk))
                    missing_ids.update(map(itemgetter, chunk))

                pbar.refresh()

        # Update the database according to existing files
        return_ids = len(missing_ids) > 0 and self.purge_missing

        with ExecutorTreeScanner(executor=self._io_executor) as scanner, tqdm(total=len(missing_ids), desc='scan files', unit='') as pbar:
            pbar.refresh()

            def finish_callback(found_ids, task_id, stat_list):
                missing_ids.difference_update(found_ids)
                pbar.update(len(stat_list))

            task_iterator = iter(chunkize(iterate_files(self.roots, scanner=scanner), _SAFE_VARIABLE_NUMBER))
            task_counter = itertools.count()
            # Distribute the database update to multiple threads
            await executor_run_many(
                partial(self._scan_files_inner, return_ids=return_ids),
                task_counter,
                task_iterator,
                workers=2,
                onfinish=finish_callback,
            )

            pbar.total = pbar.n
            pbar.refresh()

        if self.purge_missing:
            # Remove missing files
            with self._scoped_session() as db, tqdm(len(missing_ids), desc='purge missing', unit='') as pbar:
                for chunk_ids in chunkize(missing_ids, _SAFE_VARIABLE_NUMBER):
                    db.query(File).filter(File.id.in_(chunk_ids)).delete(
                        synchronize_session=False,
                    )
                    pbar.update(len(chunk_ids))
                db.commit()
                pbar.refresh()

    def _scan_files_inner(self, task_id: int, stat_list: List[Tuple[str, os.stat_result]], return_ids: bool) -> Set[int]:
        """

        :param task_id:
        :param stat_list:
        :param return_ids: whether return the ids of the existing files in the database
        :return: a set of file ids or an empty set if return_ids is falsy
        """
        table: Dict[str, Tuple[os.stat_result, bytes]] = {s: (t, path_digest(s)) for s, t in stat_list}
        keys: List[bytes] = [t for _, t in table.values()]
        found_ids = set()

        with self._scoped_session() as db:
            updated = []
            created = []

            if return_ids:
                # According to EXPLAIN QUERY PLAN, filter by (path, digest) tuple did NOT use the digest index
                # so use the simple IN filtering plus a Python dict to balance time and (disk) space.
                for file in db.query(File).filter(File.path_key.in_(keys)):  # type: File
                    try:
                        path_stat, _ = table.pop(file.path)
                    except KeyError:
                        self.logger.debug('Path %r is missing, duplicate hash? %r', file.path, file.path_key.hex())
                        continue
                    else:
                        found_ids.add(file.id)

                        if file.size == path_stat.st_size and file.mtime == path_stat.st_mtime:
                            # Skip unchanged entry
                            continue

                        file.size = path_stat.st_size
                        file.mtime = path_stat.st_mtime
                        file.hash1 = file.hash2 = file.hash3 = None  # reset hashes
                        file.done = False  # reset done flag
                        updated.append(file)

            for path, (path_stat, path_key) in table.items():
                file = File(
                    path=path,
                    path_key=path_key,
                    size=path_stat.st_size,
                    mtime=path_stat.st_mtime
                )
                db.add(file)
                created.append(file)

            if not return_ids:
                updated += created
                created = []

            if updated:
                db.bulk_save_objects(updated, preserve_order=False)

            if created:
                db.bulk_save_objects(created, return_defaults=True, preserve_order=False)

            if updated or created:
                db.commit()

            if return_ids and created:
                # Fetch back the file ids
                keys = [s for _, s in table.values()]
                path_ids = db.query(File.path, File.id).filter(File.path_key.in_(keys)).all()
                found_ids.update([t for s, t in path_ids if s in table])

            return found_ids

    async def _hash_files(self):
        hash_file = partial(fast_digests, blocksize=1024 ** 2, count=3)
        executor_run = partial(asyncio.get_running_loop().run_in_executor, None)
        subquery = Query(File.size).group_by(File.size).having(func.count() >= 2).subquery()
        query: Query = Query(File).filter(
            File.size.in_(subquery),
            File.hash1.is_(None),
            # the above condition is enough to find un-hashed entries
            # File.hash1.is_(None) | ((File.size >= _SIZE2M) & File.hash2.is_(None)) | ((File.size >= _SIZE3M) & File.hash3.is_(None)),
        )
        self.logger.debug('_hash_changed_files: SQL=%s', str(query))

        with self._scoped_session() as db:
            total = query.with_session(db).count()

        query = query.limit(1000)

        with tqdm(total=total, desc='hashing', unit='') as pbar:
            pbar.refresh()

            pending = []

            def generator():
                while True:
                    with self._scoped_session() as db_:
                        files_ = query.with_session(db_).all()
                    if files_:
                        yield from files_
                    else:
                        break

            def onfinish(hashes, file):
                file.hash1, file.hash2, file.hash3 = hashes
                file.done = False
                pending.append(file)

            def onerror(exc, file):
                file.hash1 = file.hash2 = file.hash3 = None
                file.done = False
                pending.append(file)

                if isinstance(exc, (FileNotFoundError, PermissionError)):
                    self.logger.info('Failed to hash %s due to %s', file.path, type(exc).__name__)
                    return
                raise

            def update_database(files):
                with self._scoped_session() as db_:
                    db_.bulk_save_objects(files, preserve_order=False)
                    db_.commit()

            task = asyncio.create_task(executor_run_many(
                lambda file: hash_file(file.path),
                generator(),
                onfinish=onfinish,
                onerror=onerror,
                workers=1,
                executor=self._io_executor,
            ))

            fs = {task}

            while fs:
                done, _ = await asyncio.wait(fs, timeout=2)
                fs.difference_update(done)

                for fut in done:
                    await fut

                if pending:
                    pbar.update(len(pending))
                    # send the list to executor and re-create one
                    fs.add(executor_run(update_database, pending))
                    pending = []

            pbar.refresh()

    def _dedupe_file_group(self, task_id: int, files: List[str]):
        """
        Given a list of paths, sort them using their extent distribution
        Choose the good-looking as the source file

        :param task_id a friendly number for logging
        :param files a list of file names to dedupe
        """
        extent_lists = list(self._io_executor.map(fiemap, files))

        # Sort them by extents
        files, extent_lists = zip(*sorted(zip(files, extent_lists), key=lambda s: key_by_extents(s[1])))

        src_path = files[0]
        src_extents: List[Extent] = extent_lists[0]

        dst_paths: List[str] = []
        dst_extents: List[List[Extent]] = []

        for i, extents in enumerate(extent_lists[1:], 1):  # type: (int, List[Extent])
            if all(a == b for a, b in zip(src_extents, extents)):
                # Skip file with the same physical extents
                continue

            dst_paths.append(files[i])
            dst_extents.append(extents)

        if not dst_paths:
            return

        del files, extent_lists

        with ExitStack() as stack:
            src_fd = stack.enter_context(open(src_path, 'rb')).fileno()
            dst_fds = [stack.enter_context(open(s, 'r+b')).fileno() for s in dst_paths]
            size = os.fstat(src_fd).st_size

            for extent in src_extents:
                dedupe_fds = []
                dedupe_paths = []

                for target_fd, target_path, target_extents in zip(dst_fds, dst_paths, dst_extents):
                    if extent in target_extents:
                        continue

                    dedupe_fds.append(target_fd)
                    dedupe_paths.append(target_path)

                if not dedupe_fds:
                    continue

                dedupe_offset = extent.logical
                left = size - dedupe_offset if ExtentFlag.LAST in extent.flags else extent.length

                chunksize = 1024 ** 2

                while left > 0:
                    dedupe_size = min(chunksize, left)
                    task = DedupeTask(src_fd, offset=dedupe_offset, length=dedupe_size)

                    for fd in dedupe_fds:
                        task.add_target(fd, dedupe_offset)

                    try:
                        bytes_deduped = task.dedupe()
                    except DedupeError as ex:
                        bytes_deduped = ex.bytes_deduped
                        self.logger.warning(
                            'task %d failed in offset %d %d: %s => %r',
                            task_id, dedupe_offset, dedupe_size,
                            src_path, dedupe_paths,
                            exc_info=True,
                        )

                    dedupe_offset += chunksize
                    left -= chunksize

    async def _dedupe_files(self):
        # Find same-size groups which their member files are updated / unprocessed
        with self._scoped_session() as session:
            q = (session.query(func.count(), File.size)
                 .filter(File.size > 0, File.hash1.isnot(None))
                 .group_by(File.size)
                 .having((func.count() >= 2) & (func.count() != func.sum(File.done))))
            counts_and_sizes = q.all()

        total = sum([s for s, _ in counts_and_sizes])
        sizes = [s for _, s in counts_and_sizes]

        with tqdm(total=total, desc='progress', unit='') as pbar:
            pbar.refresh()

            def group_iterator() -> Iterator[List[File]]:
                for group_size in sizes:
                    with self._scoped_session() as db:
                        files = db.query(File).filter(File.size == group_size).all()

                    min_hashes = 2 if group_size >= 3 * 1024 ** 2 else 1

                    hashes = [(s.hash1, s.hash2, s.hash3) for s in files]
                    clusters = group_by_hashes(hashes, min_hashes)
                    lonely_group = set(range(len(files))).difference(*clusters)

                    if lonely_group:
                        group_files = [files[s] for s in lonely_group]

                        for file in group_files:
                            file.done = True

                        with self._scoped_session() as db:
                            db.bulk_save_objects(group_files, preserve_order=False)
                            db.commit()

                        pbar.update(len(lonely_group))

                    for group in clusters:
                        group_files = [files[s] for s in group]

                        yield [file.path for file in group_files]

                        for file in group_files:
                            file.done = True

                        with self._scoped_session() as db:
                            db.bulk_save_objects(group_files, preserve_order=False)
                            db.commit()

                        pbar.update(len(group))

            task_counter = itertools.count()

            await executor_run_many(
                self._dedupe_file_group,
                task_counter,
                group_iterator(),
                workers=2,
            )

            pbar.refresh()

    async def run(self):
        if self.reset_done:
            with self._scoped_session() as db:
                db.query(File).update({
                    File.done: False,
                })
                db.commit()

        if not self.skip_scan:
            await self._scan_files()

        if not self.skip_hash:
            await self._hash_files()

        await self._dedupe_files()


def parser():
    from argparse import ArgumentParser
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
        '--reset-done',
        action='store_true', default=False,
        help='Reset the done flag',
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
        reset_done=ns.reset_done,
        purge_missing=ns.purge_missing,
    )
    asyncio.run(runner.run())


if __name__ == '__main__':
    main()
