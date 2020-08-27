# This file is part of dbb_buffmngrs_handoff.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Definitions of object-relational mappings.
"""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Interval,
    Numeric,
    String,
    Table,
    Text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


__all__ = ["Base", "Batch", "File"]


Base = declarative_base()


# Association table establishing many-to-many relationship between files and
# transfer batches.
association_table = Table(
    "file_transfer_attempts", Base.metadata,
    Column("files_id", BigInteger, ForeignKey("files.id")),
    Column("batch_id", BigInteger, ForeignKey("transfer_batches.id"))
)


class File(Base):
    """Declarative for file database entry.
    """
    __tablename__ = "files"
    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    relpath = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    checksum = Column(String, nullable=False)
    size_bytes = Column(Integer, nullable=False)
    created_on = Column(DateTime, nullable=False)
    held_on = Column(DateTime, nullable=True)
    deleted_on = Column(DateTime, nullable=True)
    batches = relationship("Batch",
                           secondary=association_table,
                           back_populates="files")


class Batch(Base):
    """Declarative for transfer batch database entry.
    """
    __tablename__ = "transfer_batches"
    id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True)
    pre_start_time = Column(DateTime, nullable=False)
    pre_duration = Column(Interval, nullable=False)
    trans_start_time = Column(DateTime, nullable=False)
    trans_duration = Column(Interval, nullable=True)
    post_start_time = Column(DateTime, nullable=True)
    post_duration = Column(Interval, nullable=True)
    size_bytes = Column(Integer, nullable=True)
    rate_mbytes_per_sec = Column(Numeric, nullable=True)
    status = Column(Integer, nullable=False)
    err_msg = Column(Text, nullable=True)
    files = relationship("File",
                         secondary=association_table,
                         back_populates="batches")
