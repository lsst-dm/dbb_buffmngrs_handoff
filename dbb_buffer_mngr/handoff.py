import contextlib
import logging
import os


logger = logging.getLogger(__name__)


class Scanner(object):
    """Class representing a scanning command.

    When run it scans a given directory searching for files.  All found files
    are put in the provided queue.

    Parameters
    ----------
    config : dict
        Scanner configuration.
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
        if not os.path.exists(path) or not os.path.isdir(path):
            raise ValueError(f"{path}: directory not found.")
        self.root = path
        self.queue = queue

    def run(self):
        """Scan recursively the directory to find all files it contains.
        """
        paths = []
        for topdir, subdir, filenames in os.walk(self.root):
            paths.extend([os.path.join(topdir, f) for f in filenames])
        for path in paths:
            tail = path.replace(self.root, "").lstrip(os.sep)
            dirname, filename = os.path.split(tail)
            self.queue.put((self.root, dirname, filename))


class Cleaner(object):
    """Class representing a clean up command.

    Parameters
    ----------
    config : dict
        Cleaner configuration
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
            raise ValueError("Holding area not specified.")
        if not os.path.exists(path) or not os.path.isdir(path):
            raise ValueError(f"{path}: directory not found.")
        self.root = path
        self.queue = queue

    def run(self):
        """Moves files from the buffer to the holding area.
        """
        while not self.queue.empty():
            topdir, subdir, file = self.queue.get(block=False)
            os.makedirs(os.path.join(self.root, subdir), exist_ok=True)
            src = os.path.join(topdir, subdir, file)
            dst = os.path.join(self.root, subdir, file)
            os.rename(src, dst)
            with cd(topdir):
                try:
                    os.removedirs(subdir)
                except OSError:
                    logger.debug(f"Directory '{subdir}' not empty.")
                    pass


@contextlib.contextmanager
def cd(new):
    """Change current working directory.

    Parameters
    ----------
    new : string
        Directory to change to.
    """
    old = os.getcwd()
    os.chdir(new)
    try:
        yield
    finally:
        os.chdir(old)
