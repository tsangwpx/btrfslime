from __future__ import annotations

from cffi import FFI

ffi: FFI


class lib:

    @staticmethod
    def calc_key(input: bytes, length: int) -> int: ...
