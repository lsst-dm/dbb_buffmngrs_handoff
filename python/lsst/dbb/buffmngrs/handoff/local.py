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
"""Definitions of command that need to be executed on the handoff site.
"""
import logging
import os
import shutil
import time
from datetime import datetime
from .abcs import Command
from .messages import FileMsg


__all__ = ["Finder", "Eraser", "Mover"]


logger = logging.getLogger(__name__)


class Finder(Command):
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
        If buffer is not specified, does not exists, or is not a directory.
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
        for topdir, _, filenames in os.walk(self.root):
            for name in filenames:
                path = os.path.join(topdir, name)
                tail = os.path.relpath(path, start=self.root)
                dirname, basename = os.path.split(tail)
                try:
                    status = os.stat(path)
                except FileNotFoundError as ex:
                    logger.error(f"{ex}")
                else:
                    msg = FileMsg()
                    msg.head = self.root
                    msg.tail = dirname
                    msg.name = basename
                    msg.size = status.st_size
                    msg.timestamp = status.st_mtime
                    self.queue.put(msg)


class Mover(Command):
    """Command moving files between the buffer and the holding area.

    Parameters
    ----------
    config : dict
        Configuration of the handoff site.
    inp : queue.Queue
        Input message queue with files to move.
    out : queue.Queue
        Output message queue with files that were moved.

    Raises
    ------
    ValueError
       If holding area is not specified, does not exist, or is not a directory.
    """

    def __init__(self, config, inp, out):
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
        self.inp = inp
        self.out = out

    def run(self):
        """Move files from the buffer to the holding area.
        """
        while not self.inp.empty():
            msg = self.inp.get(block=False)
            os.makedirs(os.path.join(self.root, msg.tail), exist_ok=True)
            src = os.path.join(msg.head, msg.tail, msg.name)
            dst = os.path.join(self.root, msg.tail, msg.name)
            logger.debug(f"Moving '{src}' to '{dst}'.")
            try:
                shutil.move(src, dst)
            except OSError as ex:
                logger.warning(f"Cannot move '{src}': {ex}.")
                continue
            else:
                msg.head = self.root
                msg.timestamp = datetime.now().timestamp()
                self.out.put(msg)


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
        for topdir, subdirs, _ in os.walk(self.root, topdown=False):
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
