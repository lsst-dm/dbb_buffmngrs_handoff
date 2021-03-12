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
"""Definitions of commands that need to be executed on the endpoint site.
"""

import datetime
import dataclasses
import errno
import logging
import os
import re
import shlex
import subprocess
from .abcs import Command
from .messages import TransferMsg
from .utils import get_chunk

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

        # If the source in the transfer command is specified with keyword
        # 'file', a separate transfer attempt will be made for each file. If
        # the keyword 'batch' is used instead a single transfer attempt will
        # be made for multiple files when possible.
        self.batch_mode = False
        if "batch" in self.cmds["transfer"]:
            self.batch_mode = True

        # Once the transfer mode set for future reference, replace 'file/batch'
        # with generic 'source' to make generating concrete commands easier
        # later on.
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
            # Grab a bunch of file items from the input queue.
            files = get_chunk(self.todo, size=self.chunk_size)
            if not files:
                continue

            # Group files based on their location as only the files sharing the
            # same location can be transferred as a group with a single
            # transfer command when the batch mode is enabled.
            mapping = {}
            for item in files:
                head, tail, *rest = dataclasses.astuple(item)
                mapping.setdefault((head, tail), []).append(item)

            # Transfer files to the handoff site to the endpoint site.
            for location, files in mapping.items():
                head, tail = location

                filenames = [item.name for item in files]
                sizes = {item.name: item.size for item in files}

                # Divide files into batches. If batch mode is enabled,
                # all files grouped into a single batch. Otherwise,
                # each batch will consist of a single file.
                batch_size = len(filenames) if self.batch_mode else 1
                batches = [filenames[i:i+batch_size]
                           for i in range(0, len(filenames), batch_size)]

                # Create corresponding number of transfer items to put in the
                # output queue.
                transfers = [TransferMsg() for _ in batches]
                for batch, transfer in zip(batches, transfers):
                    transfer.files = tuple((head, tail, fn) for fn in batch)
                    transfer.size = sum(sizes[fn] for fn in batch)

                # 1. PRE-TRANSFER actions
                # -----------------------
                dest = os.path.join(stage, tail)
                relocated = []

                # Create a relevant subdirectory in the staging area.
                tpl = self.cmds["remote"]
                cmd = tpl.format(**self.params, command=f"mkdir -p {dest}")
                start = datetime.datetime.now()
                status, _, stderr, dur = execute(cmd, timeout=self.timeout)
                for transfer in transfers:
                    transfer.pre_start = start.timestamp()
                    transfer.pre_duration = dur.total_seconds()
                    transfer.status = status
                    transfer.error = stderr
                if status != 0:
                    self._flush(transfers)
                    msg = f"Command '{cmd}' failed with error: '{stderr}'"
                    logger.warning(msg)
                    continue

                # 2. TRANSFER
                # -----------
                tpl = self.cmds["transfer"]
                for batch, transfer in zip(batches, transfers):
                    src = " ".join([os.path.join(head, tail, name)
                                    for name in batch])
                    cmd = tpl.format(**self.params, source=src, dest=dest)
                    start = datetime.datetime.now()
                    status, _, stderr, dur = execute(cmd, timeout=self.timeout)
                    transfer.trans_start = start.timestamp()
                    transfer.trans_duration = dur.total_seconds()
                    transfer.status = status
                    transfer.error = stderr

                    if status != 0:
                        self._flush([transfer])
                        msg = f"command '{cmd}' failed with error: '{stderr}'"
                        logger.warning(msg)
                        continue

                    # If transfer successfully, calculate transfer rate.
                    transfer.rate = transfer.size / dur.total_seconds()  # B/s
                    transfer.rate /= pow(1024, 2)                        # MB/s

                    relocated.append((batch, transfer))

                # Recreate lists without items corresponding to failed
                # transfers.
                batches, transfers = [], []
                for batch, transfer in relocated:
                    batches.append(batch)
                    transfers.append(transfer)
                if not batches:
                    continue

                # If files were transferred directly to the buffer on the
                # endpoint site, skip the next step.
                if stage == buffer:
                    self._flush(transfers)
                    continue

                # 3. POST-TRANSFER actions
                # ------------------------
                dest = os.path.join(buffer, tail)
                completed = []
                total = datetime.timedelta()

                # Create a relevant subdirectory in the buffer.
                tpl = self.cmds["remote"]
                cmd = tpl.format(**self.params, command=f"mkdir -p {dest}")
                start = datetime.datetime.now()
                status, _, stderr, dur = execute(cmd, timeout=self.timeout)
                total += dur
                for transfer in transfers:
                    transfer.post_start = start.timestamp()
                    transfer.post_duration = total
                    transfer.status = status
                    transfer.error = stderr

                if status != 0:
                    self._flush(transfers)
                    msg = f"Command '{cmd}' failed with error: '{stderr}'"
                    logger.warning(msg)
                    continue

                # Move files from the staging area to the buffer.
                tpl = self.cmds["remote"]
                for batch, transfer in zip(batches, transfers):
                    src = " ".join([os.path.join(stage, tail, name)
                                    for name in batch])
                    cmd = tpl.format(**self.params, command=f"mv {src} {dest}")
                    start = datetime.datetime.now()
                    status, _, stderr, dur = execute(cmd, timeout=self.timeout)
                    transfer.post_start = start.timestamp()
                    transfer.post_duration = (total+dur).total_seconds()
                    transfer.status = status
                    transfer.error = stderr

                    if status != 0:
                        self._flush([transfer])
                        msg = f"Command '{cmd}' failed with error: '{stderr}'"
                        logger.warning(msg)
                        continue

                    completed.append(transfer)

                self._flush([transfer for transfer in completed])

    def _flush(self, items):
        """Enqueue messages in the output queue.

        Parameters
        ----------
        items : `list`
            List of items to enqueue in the output queue.
        """
        for item in items:
            self.done.put(item)


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
        status, _, stderr, _ = execute(cmd, timeout=self.time)
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
    (int, str, str, datetime.timedelta)
        Shell command exit status, stdout, stderr, and duration.
    """
    logger.debug(f"Executing {cmd}.")

    start = datetime.datetime.now()
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
    end = datetime.datetime.now()
    duration = end - start

    logger.debug(f"Execution completed in {duration.total_seconds()}: "
                 f"(status: {status}, output: '{stdout}', error: '{stderr}').")
    return status, stdout, stderr, duration
