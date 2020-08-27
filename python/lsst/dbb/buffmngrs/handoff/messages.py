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
"""Specifications of messages allowed in inter-thread/command communication.
"""
from dataclasses import dataclass


@dataclass
class FileMsg:
    """A message containing information about a file.
    """

    head: str = None
    """Root directory.
    """

    tail: str = None
    """Path (directory) to file, relative to "head".
    """

    name: str = None
    """File name equivalent to Bash `basename path
    """

    size: int = None
    """File size in bytes.
    """

    timestamp: float = None
    """A timestamp for an arbitrary file event, e.g., creation, deletion, etc.
    """


@dataclass
class TransferMsg:
    """A message containing information about a transfer batch.
    """

    pre_start: float = None
    """Timestamp showing when pre-transfer actions started.
    """

    pre_duration: float = None
    """Duration of pre-transfer actions.
    """

    trans_start: float = None
    """Timestamp showing when transfer started.
    """

    trans_duration: float = None
    """Duration of transfer.
    """

    post_start: float = None
    """Timestamp showing when post-transfer actions started.
    """

    post_duration: float = None
    """Duration of post-transfer actions.
    """

    size: int = None
    """Amount of data transferred (in bytes).
    """

    rate: float = None
    """Transfer rate (in MBytes/s)
    """

    status: int = None
    """Transfer attempt status (0 for success, non-zero for failure)
    """

    error: str = None
    """Error message when transfer failed.
    """

    files: tuple = None
    """List of files in the batch.
    """
