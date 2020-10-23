from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, Column, Float, Integer, LargeBinary, Unicode
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class File(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True)

    # create an index on the path digest instead of the path to save disk space by almost half
    # because SQLite store a copy of the column values in the index and path length is usually larger than 32
    # currently SHA1 is overkill choice
    path = Column(Unicode, nullable=False)
    path_key = Column(LargeBinary(32), nullable=False, index=True)

    size = Column(BigInteger, nullable=False, index=True)
    mtime = Column(Float, nullable=False)  # stat_result.st_mtime is a float

    # Since size is firstly grouped, no index for hash columns
    hash1 = Column(LargeBinary(32))
    hash2 = Column(LargeBinary(32))
    hash3 = Column(LargeBinary(32))

    # flag indicate whether this file have been processed before
    done = Column(Boolean, nullable=False, default=False)

    def __repr__(self):
        digests = []
        if self.hash1:
            digests.append(self.hash1[:4].hex())

        if self.hash2:
            digests.append(self.hash2[:4].hex())

        hashes = ' '.join(digests)

        return f'<File {self.path!r} size {self.size} hash {hashes}>'
