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

import collections
import errno
import logging
import os
import queue
import re
import shlex
import subprocess
from .abcs import Command

__all__ = ['Porter', 'Wiper']


logger = logging.getLogger(__name__)
keywords = {"batch", "command", "dest", "file"}


class Porter(Command):
    """Command transferring files between handoff and endpoint sites.

    If only endpoint site's buffer is specified in the configuration,
    files will be transfer directly to it.

    To make file transfers look like atomic operations, a separate location,
    staging area, need to be specified as well.  In that case, each file is
    initially transferred to it and moved to the endpoint's buffer only
    after the transfer is finished.

    Parameters
    ----------
    config : dict
        Configuration of the endpoint where files should be transferred to.
    pending : queue.Queue
        Files that need to be transferred.
    completed : queue.Queue
        Files that were transferred successfully.
    chunk_size : int, optional
        Number of files to process in a single iteration of the transfer
        loop, defaults to 1.
    timeout : int, optional
        Time (in seconds) after which the child process executing a bash
        command will be terminated. If None (default), the command will wait
        indefinitely for the child process to complete.

    Raises
    ------
    ValueError
        If endpoint's specification is invalid.
    """

    def __init__(self, config, pending, completed, chunk_size=1, timeout=None):
        required = {"user", "host", "buffer", "commands"}
        missing = required - set(config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.critical(msg)
            raise ValueError(msg)

        self.cmds = config["commands"]
        self.params = {k: v for k, v in config.items() if k != "commands"}

        # Verify if all parameters in use were provided.
        actual = set(self.params)
        for cmd in self.cmds.values():
            formal = set(re.findall(r"{(\w+)}", cmd))
            remaining = formal - actual
            undefined = remaining - keywords
            if undefined:
                msg = f"parameters {', '.join(undefined)} are used, " \
                      f"but not defined in '{cmd}'"
                logger.error(msg)
                raise ValueError(msg)

        self.batch_mode = False
        if "batch" in self.cmds["transfer"]:
            self.batch_mode = True

        cmd = self.cmds["transfer"]
        self.cmds["transfer"] = re.sub(r"(batch|file)", "source", cmd)

        self.chunk_size = chunk_size
        self.timeout = timeout

        self.todo = pending
        self.done = completed

    def run(self):
        """Transfer files to the endpoint site.
        """
        buffer = self.params["buffer"]
        stage = self.params.get("staging", buffer)
        while not self.todo.empty():
            # Grab a bunch of files from the work queue.
            chunk = []
            for _ in range(self.chunk_size):
                try:
                    topdir, subdir, file = self.todo.get(block=False)
                except queue.Empty:
                    break
                else:
                    chunk.append((topdir, subdir, file))
            if not chunk:
                continue

            # Group files based on their location.
            mapping = {}
            for topdir, subdir, filename in chunk:
                mapping.setdefault((topdir, subdir), []).append(filename)

            # Transfer files to the handoff site to the endpoint site.
            for location, filenames in mapping.items():
                head, tail = location

                # Transfer files to the staging area on the endpoint site.
                relocated = collections.deque()

                batch = [(head, tail, fn) for fn in filenames]
                if self.batch_mode:
                    batch = [batch]
                dest = os.path.join(stage, tail)

                tpl = self.cmds["remote"]
                cmd = tpl.format(**self.params, command=f"mkdir -p {dest}")
                status, stdout, stderr = execute(cmd, timeout=self.timeout)
                if status != 0:
                    msg = f"Command '{cmd}' failed with error: '{stderr}'"
                    logger.warning(msg)
                    continue

                tpl = self.cmds["transfer"]
                for member in batch:
                    if isinstance(member, tuple):
                        member = [member]
                    src = " ".join([os.path.join(*t) for t in member])
                    cmd = tpl.format(**self.params, source=src, dest=dest)
                    status, stdout, stderr = execute(cmd, timeout=self.timeout)
                    if status != 0:
                        msg = f"command '{cmd}' failed with error: '{stderr}'"
                        logger.warning(msg)
                        continue
                    for _, _, fn in member:
                        relocated.append((head, tail, fn))

                # If files were transferred directly to the buffer on the
                # endpoint site, skip the next step.
                if stage == buffer:
                    while relocated:
                        _, _, fn = relocated.popleft()
                        self.done.put(head, tail, fn)
                    continue

                # Transfer files from the staging area to the buffer.
                relocated.clear()

                batch = [(stage, tail, fn) for fn in filenames]
                if self.batch_mode:
                    batch = [batch]
                dest = os.path.join(buffer, tail)

                tpl = self.cmds["remote"]
                cmd = tpl.format(**self.params, command=f"mkdir -p {dest}")
                status, stdout, stderr = execute(cmd, timeout=self.timeout)
                if status != 0:
                    msg = f"Command '{cmd}' failed with error: '{stderr}'"
                    logger.warning(msg)
                    continue

                tpl = self.cmds["remote"]
                for member in batch:
                    if isinstance(member, tuple):
                        member = [member]
                    src = " ".join([os.path.join(*t) for t in member])
                    cmd = tpl.format(**self.params, command=f"mv {src} {dest}")
                    status, stdout, stderr = execute(cmd, timeout=self.timeout)
                    if status != 0:
                        msg = f"Command '{cmd}' failed with error: '{stderr}'"
                        logger.warning(msg)
                        continue
                    for _, _, fn in member:
                        relocated.append((head, tail, fn))

                while relocated:
                    _, _, fn = relocated.popleft()
                    self.done.put((head, tail, fn))


class Wiper(Command):
    """Command removing empty directories from the staging area.

    Parameters
    ----------
    config : dict
        Configuration of the endpoint where empty directories should be
        removed.
    timeout : int, optional
        Time (in seconds) after which the child process executing a bash
        command will be terminated. If None (default), the command will wait
        indefinitely for the child process to complete.

    Raises
    ------
    ValueError
        If endpoint's specification is invalid.
    """

    def __init__(self, config, timeout=None):
        required = {"user", "host", "commands"}
        missing = required - set(config)
        if missing:
            msg = f"Invalid configuration: {', '.join(missing)} not provided."
            logger.critical(msg)
            raise ValueError(msg)

        self.cmds = config["commands"]
        self.params = {k: v for k, v in config.items() if k != "commands"}

        self.stage = self.params.get("staging", None)

        self.time = timeout

    def run(self):
        """Remove empty directories from the staging area.
        """
        if self.stage is None:
            return
        tpl = self.cmds["remote"]
        args = dict(command=f"find {self.stage} -type d -empty -mindepth 1 "
                            f"-delete")
        cmd = tpl.format(**self.params, **args)
        status, stdout, stderr = execute(cmd, timeout=self.time)
        if status != 0:
            msg = f"Command '{cmd}' failed with error: '{stderr}'"
            logger.warning(msg)


def execute(cmd, timeout=None):
    """Run a shell command.

    Parameters
    ----------
    cmd : basestring
        String representing the command, its options and arguments.
    timeout : int, optional
        Time (in seconds) after which the child process executing a bash
        command will be terminated. If None (default), the command will wait
        indefinitely for the child process to complete.

    Returns
    -------
    (int, basestring, basestring)
        Shell command exit status, stdout, and stderr
    """
    logger.debug(f"Executing {cmd}.")

    args = shlex.split(cmd)
    opts = dict(capture_output=True, timeout=timeout, check=True, text=True)
    try:
        proc = subprocess.run(args, **opts)
    except subprocess.CalledProcessError as ex:
        status = errno.EREMOTEIO
        stdout, stderr = ex.stdout, ex.stderr
    except subprocess.TimeoutExpired as ex:
        status = errno.ETIME
        stdout, stderr = ex.stdout, ex.stderr
    else:
        status = proc.returncode
        stdout, stderr = proc.stdout, proc.stderr

    msg = f"(status: {status}, output: '{stdout}', errors: '{stderr}')."
    logger.debug("Finished " + msg)
    return status, stdout, stderr
