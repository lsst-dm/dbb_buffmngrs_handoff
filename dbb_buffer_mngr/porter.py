import collections
import errno
import getpass
import logging
import os
import queue
import subprocess


logger = logging.getLogger(__name__)


Location = collections.namedtuple("Location", ["head", "tail"])


class Porter(object):
    """Class representing a transfer command.

    When run it will attempt to transfer the files found in the provided queue
    to a remote location using 'scp' command.

    Parameters
    ----------
    destination : basestring
        Remote location where the files should be copied to specified
        as 'user@host:path'.
    queue : queue.Queue
        Files that need to be transferred.
    chunk_size : int, optional
        Number of files to transfer using single instance of scp,
        defaults to 10.
    holding_area : basestring, optional
        Path to move the files to once they were successfully transferred to
        the remote location. Setting it to None (default), will leave them in
        the untouched in the local buffer.

    Raises
    ------
    ValueError
        If destination is not of the from [user@]host:[path] or when
        the specified holding area does not exist.
    """

    def __init__(self, destination, queue, chunk_size=10, holding_area=None):
        try:
            j = destination.index(":")
        except ValueError as ex:
            msg = f"Invalid remote location: '{destination}'; "
            msg += "[user@]host:[path] form is required."
            logger.critical(msg)
            raise ValueError(msg)
        i = None
        try:
            i = destination.index("@")
        except ValueError as ex:
            pass
        user = getpass.getuser() if i is None else destination[:i]
        host = destination[:j] if i is None else destination[i+1:j]
        root = destination[j+1:]
        self.dst = (user, host, root)

        self.queue = queue
        self.size = chunk_size

        self.area = holding_area
        if self.area is not None:
            if not os.path.exists(self.area):
                msg = f"Holding area '{self.area}' not found."
                logger.critical(msg)
                raise ValueError(msg)

    def run(self):
        """Start transferring files enqueued in the transfer queue.
        """
        user, host, root = self.dst
        while not self.queue.empty():
            chunk = []
            for _ in range(self.size):
                try:
                    topdir, subdir, file = self.queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    chunk.append((topdir, subdir, file))
            if not chunk:
                continue
            mapping = dict()
            for topdir, subdir, file in chunk:
                loc = Location(topdir, subdir)
                mapping.setdefault(loc, []).append(file)
            for loc, files in mapping.items():
                head, tail = loc
                src = os.path.join(head, tail)
                dst = os.path.join(root, tail)

                # Create the directory at the remote location.
                cmd = f"ssh -l {user} {host} mkdir -p {dst}"
                status, stdout, stderr = execute(cmd)
                if status != 0:
                    msg = f"Command '{cmd}' failed with error: '{stderr}'"
                    logger.warning(msg)
                    continue

                # Transfer files to the remote location.
                sources = [os.path.join(src, fn) for fn in files]
                cmd = f"scp -BCpq {' '.join(sources)} {user}@{host}:{dst}"
                status, stdout, stderr = execute(cmd)
                if status != 0:
                    msg = f"Command '{cmd}' failed with error: '{stderr}'"
                    logger.warning(msg)
                    continue

                # Move files to the holding area, if specified.
                if self.area is None:
                    continue
                hld = os.path.join(self.area, tail)
                destinations = [os.path.join(hld, fn) for fn in files]
                for s, d in zip(sources, destinations):
                    os.makedirs(hld, exist_ok=True)
                    os.rename(s, d)
                    try:
                        os.rmdir(src)
                    except OSError:
                        pass


def execute(cmd, timeout=None):
    """Run a shell command.

    Parameters
    ----------
    cmd : str
        String representing the command, its options and arguments.
    timeout : int, optional
        Time (in seconds) after the child process will be killed.

    Returns
    -------
    int
        Shell command exit status, 0 if successful, non-zero otherwise.
    """
    args = cmd.split()
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
    msg = f"'{cmd}': execution finished "
    msg += f"(status: {status}, output: '{stdout}', errors: '{stderr}')."
    logger.debug(msg)
    return status, stdout, stderr
