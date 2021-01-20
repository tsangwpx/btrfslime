from __future__ import annotations

import os

from ._fs import lib, ffi


def build_error(code: int, message: str = None, *args, exc=OSError, **kwargs):
    if message is None:
        try:
            message = os.strerror(code)
        except ValueError:
            message = 'Error'

    raise exc(code, message, *args, **kwargs)


def ioctl(fd: int, code: int, arg: ffi.CData):
    ret = lib.ioctl(fd, code, arg)

    if ret < 0:
        raise build_error(ffi.errno)

    return ret
