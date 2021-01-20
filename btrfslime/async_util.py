from __future__ import annotations

import asyncio
from asyncio import ensure_future, Future, FIRST_COMPLETED
from typing import Callable, Awaitable, Union, AsyncIterator

from .util import T, R


async def pull_tasks(ait: AsyncIterator[T], process: Callable[[T], Awaitable[R]], limit: int = 2):
    """
    Pull work units from ait, apply it with handle, yield the result

    :param ait: an async iterator
    :param process: process item from ait and return an awaitable
    :param limit: the concurrency of outstanding "process" tasks
    :return:
    """
    ait = type(ait).__aiter__(ait)
    __anext__ = type(ait).__anext__

    fs = set()
    pull: Union[bool, Future, None] = None

    while True:
        if pull is None and len(fs) < limit:
            pull = ensure_future(__anext__(ait))
            fs.add(pull)

        if not fs:
            break

        done, _ = await asyncio.wait(fs, return_when=FIRST_COMPLETED)

        if pull and pull.done():
            fs.remove(pull)

            try:
                work = await pull
            except StopAsyncIteration:
                pull = False  # ait is exhausted
            else:
                fs.add(ensure_future(process(work)))
                pull = work = None

            continue

        for f in done:
            yield await f
            fs.remove(f)
