from __future__ import annotations

import argparse
import collections
import logging
import operator
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional, List, AnyStr, Iterator

from btrfslime.scantree import iterate_files
from . import btrfs
from .planner import PlannerParams, Planner
from ..fiemap import Extent, fiemap
from ..util import binary_type


@dataclass
class Task:
    id: int
    path: str
    extents: List[Extent] = None
    clusters: List[List[int]] = None
    frequency: int = 0


class Runner:
    def __init__(
        self,
        roots: List[str],
        planner_params: PlannerParams,
        *,
        dry_run: bool = False,
        repetitions: int = 1,
        flush: bool = False,
        disk_speed: int = None,
        logger=None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.roots = list(roots)

        if not isinstance(planner_params, PlannerParams):
            raise TypeError('planner_params')
        self.planner_params = planner_params

        assert isinstance(repetitions, int), type(repetitions)
        if repetitions <= 0:
            raise ValueError('passes <= 0')

        self._dry_run = bool(dry_run)
        self._repetitions = repetitions
        self._flush = flush

        if disk_speed is not None and disk_speed < 1:
            disk_speed = None
        self._disk_speed: Optional[int] = disk_speed

        self._task_count = 0

    def run(self):
        for path, _ in iterate_files(self.roots):
            task = self._create_task(path)

            for index in range(self._repetitions):
                self.logger.info('Pass %d/%d start', index + 1, self._repetitions)
                if self._check_task(task):
                    self._defrag_task(task)

    def _create_task(self, path: AnyStr):
        self._task_count += 1
        return Task(
            id=self._task_count,
            path=path,
        )

    def _check_task(self, task: Task) -> bool:
        """
        Update the task and return true if the task seems fragmented
        """
        try:
            extents = list(fiemap(task.path, sync=task.frequency > 1))
        except OSError:
            self.logger.error('Error#%d %s', task.id, task.path, exc_info=True)
            return False

        if not extents:
            return False

        planner = Planner(self.planner_params, extents)
        clusters = planner.result()

        if not clusters:
            return False

        task.extents = extents
        task.clusters = clusters

        return True

    def _defrag_task(self, task: Task):
        self.logger.info('Defrag#%d %s', task.id, task.path)

        for group_index in range(len(task.clusters)):
            self._defrag_group(task, group_index)

        task.frequency += 1

    def _defrag_group(self, task: Task, group_index):
        """Called by _defrag_task()"""

        group = task.clusters[group_index]
        extents = task.extents

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(
                'Group#%d %d/%d',
                task.id, group_index + 1, len(task.clusters),
            )

        head = extents[group[0]]
        start = head.logical

        tail = extents[group[-1]]
        size = tail.logical_stop - start

        t0 = time.monotonic()
        if not self._dry_run:
            btrfs.file_defrag(
                task.path, start, size,
                extent_size=self.planner_params.extent_size,
                flush=self._flush,
            )
        t1 = time.monotonic()

        if self._disk_speed is not None:
            t = max(size / self._disk_speed - (t1 - t0), 0)
            time.sleep(min(60.0, t))


class ThreadedRunner(Runner):
    def __init__(
        self,
        roots: List[str],
        *,
        workers: int = None,
        max_tasks: int = 32,
        **kwargs,
    ):
        super().__init__(roots, **kwargs)

        if workers is None:
            workers = 4
        self._workers = workers
        self._max_tasks = max_tasks

    def run(self):

        with ThreadPoolExecutor(self._workers) as executor:
            task_futures = collections.deque()
            it: Iterator[AnyStr] = map(operator.itemgetter(0), iterate_files(self.roots, executor=executor))

            def task_enqueue(t: Task):
                f = executor.submit(self._check_task, t)
                task_futures.append((t, f))

            while True:
                for _ in range(self._max_tasks - len(task_futures)):
                    path = next(it, None)
                    if path is None:
                        break
                    task = self._create_task(path)
                    task_enqueue(task)

                if not task_futures:
                    break

                task, fut = task_futures.popleft()

                if not fut.result():
                    continue

                self._defrag_task(task)

                if task.frequency < self._repetitions:
                    task_enqueue(task)


def parser():
    def speed_type(x: str) -> Optional[int]:
        speed = binary_type(x)
        if speed == 0:
            return None
        return speed

    def tolerance_type(x: str):
        f = float(x)
        if not 0 <= f <= 1:
            raise ValueError
        return f

    p = argparse.ArgumentParser()
    p.add_argument(
        '--verbose', '-v',
        action='count', default=0,
    )
    p.add_argument(
        '--dry-run', '-n',
        action='store_true', default=False,
    )
    p.add_argument(
        '--target-size',
        dest='extent_size',
        type=binary_type, default=1024 ** 2 * 128,
        help='Target extent size (default: 128MB)',
    )
    p.add_argument(
        '--acceptable-size',
        dest='small_extent_size',
        type=binary_type, default=1024 ** 2 * 32,
        help='Acceptable extent size (default: 32MB)',
    )
    p.add_argument(
        '--large-extent-size',
        type=binary_type, default=1024 ** 2 * 96,
        help='Ignorable extent size (default: 64MB)',
    )
    p.add_argument(
        '--shared-size',
        type=binary_type, default=1024 ** 2 * 1,
        help='Ignorable shared extent size (default: 1MB)',
    )
    p.add_argument(
        '--tolerance',
        type=tolerance_type, default=0.1,
        help='tolerable fraction in both size and number of extents (default: 0.1)',
    )
    p.add_argument(
        '--dedupe',
        action='store_true', default=False,
        help='use ioctl FIDEDUPERANGE to defrag hopefully',
    )
    p.add_argument(
        '--repetitions',
        type=int, default=1,
        help='defragment multiple times (default: 1)',
    )
    p.add_argument(
        '--flush',
        action='store_true', default=False,
    )
    p.add_argument(
        '--speed',
        default=None, type=speed_type,
        help='Disk IO speed that estimate pause between files (default: 0, no pause)',
    )
    p.add_argument(
        'roots',
        nargs='+', metavar='root',
        help='Files / directories that need defragment',
    )

    return p


def main():
    p = parser()
    ns = p.parse_args()

    debug = ns.verbose >= 3
    if ns.verbose >= 2:
        log_level = logging.DEBUG
    elif ns.verbose >= 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    logging.basicConfig(
        level=log_level,
    )

    logger = logging.getLogger('main')

    planner_params = PlannerParams(
        extent_size=ns.extent_size,
        small_extent_size=ns.small_extent_size,
        large_extent_size=ns.large_extent_size,
        shared_extent_size=ns.shared_size,
        tolerance=ns.tolerance,
    )

    logger.debug(
        'Planner params [%d, %d, %d, %d]',
        planner_params.extent_size // 4096,
        planner_params.small_extent_size // 4096,
        planner_params.large_extent_size // 4096,
        planner_params.shared_extent_size // 4096,
    )

    cls = Runner if debug else ThreadedRunner
    runner = cls(
        ns.roots,
        dry_run=ns.dry_run,
        planner_params=planner_params,
        repetitions=ns.repetitions,
        flush=ns.flush,
        disk_speed=ns.speed,
    )

    runner.run()


if __name__ == '__main__':
    main()
