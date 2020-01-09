import logging
import os


logger = logging.getLogger(__name__)


class Scanner(object):
    """Class representing a scanning command.

    When run it scans a given directory searching for files.  All found files
    are put in the provided queue.

    Parameters
    ----------
    directory : basestring
        A directory to scan.
    queue : queue.Queue
        Container where found files will be stored.

    Raises
    ------
    ValueError
        If provided path does not exist or is not a directory.
    """

    def __init__(self, directory, queue):
        if not os.path.exists(directory) or not os.path.isdir(directory):
            raise ValueError(f"{directory}: directory not found.")
        self.root = directory
        self.queue = queue

    def run(self):
        """Scan a directory to find files.
        """
        paths = []
        for topdir, subdir, filenames in os.walk(self.root):
            paths.extend([os.path.join(topdir, f) for f in filenames])
        for path in paths:
            tail = path.replace(self.root, "").lstrip(os.sep)
            dirname, filename = os.path.split(tail)
            self.queue.put((self.root, dirname, filename))
