# This file is part of dbb_buffer_mngr.
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

import logging
import os
import time
from .command import Command


logger = logging.getLogger(__name__)


class Scanner(Command):
    """Command finding out all files in the buffer on the handoff site.

    Parameters
    ----------
    config : dict
        Configuration of the handoff site.
    queue : queue.Queue
        Container where the files found in the given directory will be stored.

    Raises
    ------
    ValueError
        If provided path does not exist or is not a directory.
    """

    def __init__(self, config, queue):
        try:
            path = config["buffer"]
        except KeyError:
            raise ValueError("Buffer not specified.")
        if not os.path.isdir(path):
            raise ValueError(f"{path}: directory not found.")
        self.root = path
        self.queue = queue

    def run(self):
        """Scan recursively the directory to find all files it contains.
        """
        for topdir, subdirs, filenames in os.walk(self.root):
            for name in filenames:
                path = os.path.join(topdir, name)
                tail = os.path.relpath(path, start=self.root)
                dirname, basename = os.path.split(tail)
                self.queue.put((self.root, dirname, basename))


class Mover(Command):
    """Command moving files between the buffer and the holding area.

    Parameters
    ----------
    config : dict
        Configuration of the handoff site.
    queue : queue.Queue
        Container where the files need to be moved are stored.

    Raises
    ------
    ValueError
       If holding area is not specified or it does not exist.
    """

    def __init__(self, config, queue):
        try:
            path = config["holding"]
        except KeyError:
            msg = "Holding area not specified."
            logger.critical(msg)
            raise ValueError(msg)
        if not os.path.isdir(path):
            msg = f"{path}: directory not found."
            logger.critical(msg)
            raise ValueError(msg)
        self.root = path
        self.queue = queue

    def run(self):
        """Move files from the buffer to the holding area.
        """
        while not self.queue.empty():
            topdir, subdir, file = self.queue.get(block=False)
            os.makedirs(os.path.join(self.root, subdir), exist_ok=True)
            src = os.path.join(topdir, subdir, file)
            dst = os.path.join(self.root, subdir, file)
            os.rename(src, dst)


class Eraser(Command):
    """Command removing empty directories from the buffer.

    To avoid possible race condition between the application writing files to
    the buffer and the command itself, empty directories are removed only if
    they were not modified for a certain period of time.

    Parameters
    ----------
    config : dict
        Configuration of the handoff site.
    exp_time : int
        Time (in seconds) that need to pass from the last modification before
        an empty directory can be removed.

    Raises
    ------
    ValueError
       If holding area is not specified or it does not exist.
    """

    def __init__(self, config, exp_time=86400):
        try:
            path = config["buffer"]
        except KeyError:
            msg = "Buffer not specified."
            logger.critical(msg)
            raise ValueError(msg)
        if not os.path.isdir(path):
            msg = f"{path}: directory not found."
            logger.critical(msg)
            raise ValueError(msg)
        self.root = path
        self.exp_time = exp_time

    def run(self):
        """Remove old, empty directories from the buffer.
        """
        empty_dirs = []
        for topdir, subdirs, files in os.walk(self.root, topdown=False):
            for name in subdirs:
                path = os.path.join(topdir, name)
                if len(os.listdir(path)) == 0:
                    empty_dirs.append(path)
        logger.debug(f"Empty directories found: '{empty_dirs}'.")

        now = time.time()
        for path in empty_dirs:
            stat_info = os.stat(path)
            mod_time = stat_info.st_mtime
            duration = now - mod_time
            if duration > self.exp_time:
                try:
                    os.rmdir(path)
                except (FileNotFoundError, OSError) as ex:
                    logger.warning(f"Cannot remove '{path}': {ex}")
                else:
                    logger.debug(f"Directory '{path}' removed successfully.")
