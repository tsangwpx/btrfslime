from __future__ import annotations

from functools import lru_cache

# Add salt to avoid unlikely conflict

SALT = b'\x27\xf0\x74\x02\x9e\x11\x2b\x82\x1e\x21\x5d\xe0\x39\xfa\x77\xe7' \
       b'\x37\x32\xa1\x8d\xef\x11\xba\xb8\xe2\x3b\x23\x96\x74\x54\xca\x63' \
       b'\x20\x4c\x08\x04\x7b\x35\xe3\xd0\x21\xc2\x5f\xc5\x3a\x98\xcd\x12' \
       b'\x2b\x54\x38\xf2\xa4\x8d\x9e\xdf\xd7\x30\x7e\xe1\x25\x44\x9f\xa3' \
       b'\x17\x22\x83\xbb\xe1\xd3\xcd\x05\x4d\x29\xcb\x5b\x06\xe8\xd0\x5a' \
       b'\x45\xae\x0b\x6f\xa4\x3e\x84\x06\xe3\x89\x97\xf4\xb2\x5f\xea\xe7' \
       b'\x4f\x2e\x63\xcf\x85\xd0\x64\x2e\xd3\x98\xb5\xa3\xe3\xf3\x22\x11' \
       b'\x5e\x01\xd5\xb2\x61\x32\xf6\x07\xb6\xb1\x8a\xa3\xfa\x53\x7b\x78' \
       b'\x06\xb4\xbd\x57\xe4\x29\x08\xa0\xea\x42\x8f\x63\x46\x88\x84\x48' \
       b'\xd4\x4d\x8b\x9b\xb2\x60\xa3\xcc\xcd\x34\x2e\x06\x35\xe9\xf9\x70' \
       b'\xea\x47\xef\x8a\xc4\xe3\x57\x71\xed\x72\x0f\x6f\x45\x18\xd6\x32' \
       b'\xce\xd4\x7d\x5b\x0b\xba\x8b\x00\x6b\x7b\x30\xa7\x29\xab\xad\xe2' \
       b'\xd4\xec\xa1\x5a\x66\x4b\xc1\xa9\x37\x33\x1a\xf9\x9e\x33\x8b\x1b' \
       b'\xcf\x29\x1f\x92\xda\x1e\xe6\x4d\xfd\x37\xb0\xbc\x52\x87\xb5\xb6' \
       b'\x70\x32\xfe\xef\x9d\x02\x54\x1c\x57\xf8\xe5\xbf\x6e\xe6\x33\xd0' \
       b'\x98\x57\x8b\x43\x27\xb0\x45\x12\xa3\x28\xb2\xcd\x2b\x38\x4e\xda'


@lru_cache()
def _precompute_hash(hash_cls, offset):
    h = hash_cls()
    h.update(SALT[offset:offset + h.block_size * 2])
    return h


def salted_algo(hash_cls, offset=0):
    return _precompute_hash(hash_cls, offset).copy()


__all__ = (
    'salted_algo',
)
