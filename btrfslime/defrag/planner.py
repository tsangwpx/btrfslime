from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Set, Tuple

from ..fiemap import ExtentFlag, Extent
from ..util import group_by_link, pairwise


def _check_int_4096(name, value):
    if not isinstance(value, int):
        raise TypeError(f"'{name}' expects an int")
    elif value % 4096 != 0:
        raise ValueError(f"'{name}' expects an int multiple of 4096")

    return value


@dataclass
class PlannerParams:
    extent_size: int
    small_extent_size: int
    large_extent_size: int
    shared_extent_size: int
    tolerance: float  # range [0, 1]

    def __post_init__(self):
        self.extent_size = _check_int_4096('extent_size', self.extent_size)
        self.small_extent_size = _check_int_4096('small_extent_size', self.small_extent_size)
        self.large_extent_size = _check_int_4096('large_extent_size', self.large_extent_size)
        self.shared_extent_size = _check_int_4096('shared_extent_size', self.shared_extent_size)

        if self.small_extent_size > self.large_extent_size:
            raise ValueError('large extent size must be not less than small extent size')


class Planner:
    logger = logging.getLogger(__name__)

    def __init__(
        self,
        params: PlannerParams,
        extents: List[Extent],
        filename: str = None,
    ):
        self.params = params
        self.extents = extents
        self.filename = filename

    def _classify_extents(self) -> Tuple[List[int], Set[int]]:
        """
        Classify extents using their lengths and flags

        :return: an ordered list of fragmented extent indices and a set of unused extent indices
        """

        good_flags = ExtentFlag.LAST | ExtentFlag.SHARED
        bad_flags = ~good_flags
        unknown_flags = ~(good_flags | ExtentFlag.DELALLOC | ExtentFlag.ENCODED | ExtentFlag.ENCRYPTED | ExtentFlag.UNWRITTEN)

        fragmented = []
        unused = set()

        small_extent_size = self.params.small_extent_size
        large_extent_size = self.params.large_extent_size
        shared_size = self.params.shared_extent_size

        for index, extent in enumerate(self.extents):
            if extent.flags & bad_flags:
                if extent.flags & unknown_flags:
                    self.logger.debug(
                        'unknown flags %r in %s',
                        extent.flags & unknown_flags, self.filename,
                    )
                # Ignore extent with unknown flags
                continue

            extent_length = extent.length

            if extent_length >= shared_size and ExtentFlag.SHARED in extent.flags:
                # Ignore shared extent with large enough size
                continue

            if extent_length >= large_extent_size:
                # Ignore extent with large enough size
                continue

            if extent_length >= small_extent_size:
                unused.add(index)
            else:
                fragmented.append(index)

        return fragmented, unused

    def _compute_contiguity(self) -> List[bool]:
        """
        Return a list of boolean values indicating whether two nearby extents are logically contiguous
        E(i) are extents and C(i) are contiguities between extents E(i) and E(i+1)
        E0, E1, E2, E3, E4, E5
          C0, C1, C2, C3, C4, C5 = False
        """
        if len(self.extents) <= 0:
            return []

        continuities = [a.logical_stop == b.logical for a, b in pairwise(self.extents)]
        continuities.append(False)

        return continuities

    def result(self) -> List[List[int]]:
        """
        :return: a list of groups of extent indices or empty list if defragmentation is unnecessary
        """
        extents = self.extents
        if len(extents) <= 1:
            # Shortcut
            return []

        fragmented, unused = self._classify_extents()
        if not fragmented or self._looking_good(fragmented):
            return []

        contiguity = self._compute_contiguity()
        extent_size = self.params.extent_size

        while True:
            adjusted = False

            # 1. Group fragmented extents by logical locations.
            # 2. Prepend and append adjacent extents if the group size smaller than extent_size parameter
            for group in group_by_link(fragmented[:], lambda a, b: contiguity[a] and a + 1 == b):
                group_size = sum([extents[i].length for i in group])

                if group_size >= extent_size:
                    continue

                prev_idx = group[0] - 1
                next_idx = group[-1] + 1

                while (
                    group_size < extent_size
                    and prev_idx in unused
                    and contiguity[prev_idx]
                    and extents[prev_idx].length < extent_size  # do not touch the prev extent already in target size
                ):
                    unused.remove(prev_idx)
                    fragmented.append(prev_idx)
                    group_size += extents[prev_idx].length
                    adjusted = True
                    prev_idx -= 1

                while (
                    group_size < extent_size
                    and next_idx in unused
                    and contiguity[next_idx - 1]
                ):
                    unused.remove(next_idx)
                    fragmented.append(next_idx)
                    group_size += extents[next_idx].length
                    adjusted = True
                    next_idx += 1

            if not adjusted:
                break

            fragmented.sort()

        # Only return group with size >= 2
        return [
            g
            for g in group_by_link(fragmented, lambda a, b: contiguity[a] and a + 1 == b)
            if len(g) >= 2
        ]

    def _looking_good(self, fragmented_extents: List[int]) -> bool:
        """
        Tell whether the degree of fragmentation is acceptable
        """
        extents = self.extents
        size = sum([e.length for e in extents])
        if size <= self.params.extent_size:
            # Dont skip small file
            return False
        if fragmented_extents:
            if len(fragmented_extents) > len(extents) * self.params.tolerance:
                return False
            fragmented_lengths = [extents[i].length for i in fragmented_extents]
            if min(fragmented_lengths) <= min(self.params.small_extent_size * 2, self.params.extent_size):
                # The minimum extent length is less than extent_size or double of small_extent_size
                return False
            if sum(fragmented_lengths) >= size * self.params.tolerance:
                # 10% of the file size is fragmented
                return False
        return True
