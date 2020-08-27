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
"""Default settings for general options.
"""

from dataclasses import dataclass


__all__ = ["Defaults"]


@dataclass
class Defaults:
    """Default values for general settings.
    """

    chunk_size = 10
    """Maximal number of messages/items to retrieve from the input queue.

    In case of file transfers it corresponds to the maximal number of files
    that will be transferred to the endpoint site with a single transfer
    command, e.g., scp or bbcp.
    """

    timeout = None
    """Time (in sec.) after a shell command will be terminated.
    """

    pause = 1
    """Time (in sec.) a thread spent idling after a session is finished.
    """

    num_threads = 1
    """Number of transfer threads to run concurrently.
    """

    expiration_time = 86400
    """Time (in sec.) after an empty directories in the buffer will be removed.
    """
