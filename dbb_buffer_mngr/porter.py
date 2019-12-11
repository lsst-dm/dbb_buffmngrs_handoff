import collections
import logging
import os
import queue
import subprocess
import threading
import time


logger = logging.getLogger(__name__)


Location = collections.namedtuple("Location", ["head", "tail"])


class Porter(threading.Thread):
    """Class representing a transfer thread.

    Once started, it keeps transferring the files found in the provided queue
    to a remote location using 'scp' command.

    Parameters
    ----------
    destination : string
        Remote location where the files should be copied to specified
        as 'user@host:path'.
    queue : queue.Queue
        List of files awaiting for transfer.
    chunk_size : int, optional
        Number of files to transfer using single instance of scp,
        defaults to 10.
    """

    def __init__(self, destination, queue, chunk_size=10):
        threading.Thread.__init__(self)
        self.dest = destination
        self.queue = queue
        self.size = chunk_size

    def run(self):
        """Start transferring files enqueued in the transfer queue.
        """
        i = self.dest.index("@")
        j = self.dest.index(":")
        user = self.dest[:i]
        host = self.dest[i+1:j]
        root = self.dest[j+1:]

        while not self.queue.empty():
            chunk = []
            for _ in range(self.size):
                try:
                    head, tail = self.queue.get(block=False)
                except queue.Empty:
                    break
                else:
                    chunk.append((head, tail))
            if chunk:
                mapping = dict()
                for head, tail in chunk:
                    dirname, basename = os.path.split(tail)
                    if not dirname:
                        dirname = "."
                    loc = Location(head, dirname)
                    mapping.setdefault(loc, []).append(basename)

                for loc, filenames in mapping.items():
                    src = os.path.join(loc.head, loc.tail)
                    dest = os.path.join(root, loc.tail)
                    cmd = "ssh -l {} {} mkdir -p {}".format(user, host, dest)
                    status = execute(cmd)
                    if status != 0:
                        msg = "Command '{cmd}' failed."
                        logger.info(msg.format(cmd=cmd))
                        # TODO: Do something if copying failed. Enqueue again?
                        continue

                    files = " ".join([os.path.join(src, n) for n in filenames])
                    path = "{u}@{h}:{p}".format(u=user, h=host, p=dest)
                    cmd = "scp -BCpq {} {}".format(files, path)
                    status = execute(cmd)
                    if status != 0:
                        msg = "Command '{cmd}' failed."
                        logger.warning(msg.format(cmd=files))
                        # TODO: Do something if copying failed. Enqueue again?
                chunk.clear()

            time.sleep(1)


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
    status = 0
    args = cmd.split()
    opts = dict(capture_output=True, timeout=timeout, check=True, text=True)
    try:
        proc = subprocess.run(args, **opts)
    except subprocess.CalledProcessError as ex:
        if args[0] == "mkdir":
            if ex.stderr.endswith("File exists"):
                msg = "Path '{path}' already exists on the remote site."
                logger.info(msg.format(path=args[-1]))
        else:
            msg = "Command '{cmd}' failed: {err}"
            logger.warning(msg.format(cmd=args, err=ex.stderr))
            status = ex.returncode
    except subprocess.TimeoutExpired as ex:
        msg = "Command '{cmd}' timed out after {time} seconds."
        logger.warning(msg.format(cmd=args, time=ex.timeout))
        status = 1
    else:
        status = proc.returncode
    return status
