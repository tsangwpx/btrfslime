from __future__ import annotations

import asyncio
from functools import partial
from typing import Callable, Dict, Iterable

from .util import T, R


async def executor_run_many(
    func: Callable[[T, ...], R],
    *iterables: Iterable[T],
    workers: int = 1,
    onfinish: Callable[[R, T, ...], None] = None,
    onerror: Callable[[Exception, T, ...], None] = None,
    executor=None,
    cancel_if_errors=True,
):
    """
    This implements a producer-consumer pattern with limited workers

    :param func: the consumer function
    :param iterables: the tasks generated by producer
    :param workers: the number of concurrent consumers
    :param onfinish: callback when a task is finished without exceptions
    :param onerror: callback when a task raises an exception
    :param executor: executor to which `func` is submitted
    :param cancel_if_errors: cancel and wait outstanding futures before return
    :return:
    """
    if not iterables:
        raise TypeError('provide at least one iterable')

    it = zip(*iterables)
    fs: Dict[asyncio.Future, T] = {}
    executor_run = partial(asyncio.get_running_loop().run_in_executor, executor)

    fut: asyncio.Future

    try:
        while True:
            for _ in range(workers - len(fs)):
                task = next(it, None)
                if not task:
                    break
                fut = executor_run(func, *task)
                fs[fut] = task

            if not fs:
                return

            done, _ = await asyncio.wait(fs, return_when=asyncio.FIRST_COMPLETED)

            for fut in done:
                task = fs.pop(fut)

                try:
                    result = fut.result()
                except Exception as ex:
                    if onerror is None:
                        raise
                    onerror(ex, *task)
                else:
                    if onfinish is not None:
                        onfinish(result, *task)
    finally:
        if cancel_if_errors:
            for fut in fs:
                fut.cancel()
            if fs:
                await asyncio.wait(fs)
