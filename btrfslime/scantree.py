from __future__ import annotations

import collections
import logging
import os
import stat
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import ExitStack
from typing import Union, AnyStr, Iterable, Tuple, List, Callable, Any, Deque, Optional, Iterator


def iterate_files(roots: Iterable[AnyStr], dirfilter: Callable = None, scanner=None, executor=None) -> Iterator[Tuple[AnyStr, os.stat_result]]:
    """Yield file path"""
    roots = list(roots)

    if not roots:
        raise TypeError('provide at least one root')

    with ExitStack() as stack:
        if scanner is None:
            if executor is None:
                scanner = TreeScanner(dirfilter=dirfilter)
            else:
                scanner = ExecutorTreeScanner(dirfilter=dirfilter)

        scanner = stack.enter_context(scanner)
        is_reg = stat.S_ISREG
        is_dir = stat.S_ISDIR
        path_join = os.path.join

        for path in roots:
            sr = os.stat(path)

            if is_reg(sr.st_mode):
                yield path, sr
            elif is_dir(sr.st_mode):
                scanner.add_path(path)
            else:
                logging.getLogger(__name__).debug('%r is neither a file nor a dir', path)

        for path, stat_results in scanner:
            for name, sr in stat_results:
                if is_reg(sr.st_mode):
                    yield path_join(path, name), sr


def _scandir(path: AnyStr, follow_symlinks: bool, onerror: Optional[Callable]) -> Optional[List[Tuple[AnyStr, os.stat_result]]]:
    try:
        with os.scandir(path) as it:
            results = [(e.name, e.stat(follow_symlinks=follow_symlinks)) for e in it]
    except OSError as ex:
        if onerror is not None:
            onerror(ex, path)
        return None
    else:
        return results


class TreeScanner:
    __slots__ = ('queue', 'dirfilter', 'follow_symlinks', '__dict__')

    def __init__(
        self,
        roots: Union[AnyStr, Iterable[AnyStr]] = None,
        *,
        follow_symlinks: bool = False,
        dirfilter: Callable[[AnyStr], bool] = None,
        onerror: Callable[[OSError, AnyStr], Any] = None,
    ):
        if roots is None:
            roots = ()
        elif isinstance(roots, (str, bytes)):
            roots = (roots,)

        self.queue: Deque[AnyStr] = collections.deque(roots)
        self.follow_symlinks = bool(follow_symlinks)
        self.queue_pop = self.queue.popleft
        self.dirfilter = dirfilter
        self.onerror = onerror
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.queue.clear()
        self.closed = True

    def __iter__(self):
        return self

    def add_path(self, path: AnyStr):
        if self.closed:
            raise ValueError('TreeScanner is closed')
        self.queue.append(path)

    def _next_path(self):
        while True:
            try:
                path = self.queue_pop()
            except IndexError:
                return None

            dirfilter = self.dirfilter
            if dirfilter is None or dirfilter(path):
                return path

    def __next__(self) -> Tuple[AnyStr, List[Tuple[AnyStr, os.stat_result]]]:
        while True:
            path = self._next_path()
            if path is None:
                self.closed = True
                raise StopIteration

            results = _scandir(path, self.follow_symlinks, self.onerror)
            if results is None:
                continue

            isdir = stat.S_ISDIR
            self.queue.extend([os.path.join(path, name) for name, sr in results if isdir(sr.st_mode)])
            return path, results


class ExecutorTreeScanner(TreeScanner):
    def __init__(
        self,
        roots: Union[AnyStr, Iterable[AnyStr]] = None,
        *,
        executor: 'ThreadPoolExecutor' = None,
        workers: int = 4,
        max_tasks: int = 32,
        **kwargs,
    ):
        super().__init__(roots, **kwargs)

        shutdown_executor = False

        if executor is None:
            executor = ThreadPoolExecutor(workers)
            shutdown_executor = True
        if not isinstance(executor, ThreadPoolExecutor):
            raise TypeError

        if max_tasks <= 0:
            raise ValueError

        self.max_tasks = max_tasks
        self._executor = executor
        self._shutdown_executor = shutdown_executor
        self._path_futures: Deque[Tuple[AnyStr, Future]] = collections.deque()

    def __enter__(self):
        if self._shutdown_executor:
            cls = type(self._executor)
            cls.__enter__(self._executor)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            fs = list(self._path_futures)
            self._path_futures.clear()
            for _, fut in fs:
                fut.cancel()
        finally:
            if self._shutdown_executor:
                cls = type(self._executor)
                return cls.__exit__(self._executor, exc_type, exc_val, exc_tb)

    def __next__(self) -> Tuple[AnyStr, List[Tuple[AnyStr, os.stat_result]]]:
        while not self.closed and (self._path_futures or self.queue):
            for _ in range(self.max_tasks - len(self._path_futures)):
                if not self._submit_one():
                    break

            try:
                path, fut = self._path_futures.popleft()
            except IndexError:
                break

            stat_results = fut.result()

            if stat_results is None:
                continue

            isdir = stat.S_ISDIR
            self.queue.extend([os.path.join(path, name) for name, sr in stat_results if isdir(sr.st_mode)])
            self._submit_one()
            return path, stat_results

        self.closed = True
        raise StopIteration

    def _submit_one(self):
        path = self._next_path()
        if path is None:
            return False

        fut = self._executor.submit(_scandir, path, self.follow_symlinks, self.onerror)
        self._path_futures.append((path, fut))
        return True
