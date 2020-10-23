from __future__ import annotations

import itertools
import re
from functools import partial
from typing import Iterable, List, Iterator, Callable, TypeVar, Tuple

T = TypeVar('T')
T_co = TypeVar('T_co', covariant=True)
R = TypeVar('R')
R_co = TypeVar('R_co', covariant=True)


def binary_type(s):
    # used with argparse
    if isinstance(s, int):
        return s
    elif isinstance(s, str):
        m = re.match(r'^(\d+)(?:([kmgtpezy])(?:i?b)?)?$', s.lower(), re.ASCII | re.IGNORECASE)

        if not m:
            raise ValueError(str(s))

        unit = m.group(2) or ''
        factor = {
            '': 1,
            'k': 1024 ** 1,
            'm': 1024 ** 2,
            'g': 1024 ** 3,
            't': 1024 ** 4,
            'p': 1024 ** 5,
            'e': 1024 ** 6,
            'z': 1024 ** 7,
            'y': 1024 ** 8,
        }[unit.lower()]
        return int(m.group(1)) * factor
    else:
        raise TypeError(f'Unknown type {type(s)!r}')


def take(iterable: Iterable[T], n: int) -> List[T]:
    return list(itertools.islice(iterable, n))


def chunkize(iterable: Iterable[T], n: int) -> Iterator[List[T]]:
    return iter(partial(take, iter(iterable), n), [])


def pairwise(iterable: Iterable[T]) -> Iterator[Tuple[T, T]]:
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def group_by_link(iterable: Iterable[T], is_same: Callable[[T, T], bool]) -> Iterator[List[T]]:
    """
    Split iterable into lists of consecutive items

    :param iterable:
    :param is_same: Return True if two consecutive items are in the same list; False otherwise
    :return:
    """

    it = iter(iterable)

    try:
        prev = next(it)
    except StopIteration:
        return

    group: List[T_co] = [prev]
    append = list.append

    for curr in it:
        same = is_same(prev, curr)
        prev = curr
        if same:
            append(group, curr)
        elif group:
            yield group
            group = [curr]

    yield group


def check_nonnegative(name: str, value: int) -> int:
    if not isinstance(value, int):
        raise TypeError(f"'{name}' must be int")
    elif value < 0:
        raise ValueError(f"'{name}' must be non-negative")
    return value
