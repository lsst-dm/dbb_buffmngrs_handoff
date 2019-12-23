import logging
import os
import threading


logger = logging.getLogger(__name__)


class Scanner(threading.Thread):
    """Class representing a directory scanner.

    Once started, it scans a directory searching for files.

    Parameters
    ----------
    directory : string
        A directory to scan.
    queue : queue.Queue
        Place to add the produced snapshot.

    Raises
    ------
    ValueError
        If provided path does not exist or is not a directory.
    """

    def __init__(self, directory, queue):
        threading.Thread.__init__(self)
        if not os.path.exists(directory) or not os.path.isdir(directory):
            raise ValueError("{}: directory not found.".format(directory))
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
