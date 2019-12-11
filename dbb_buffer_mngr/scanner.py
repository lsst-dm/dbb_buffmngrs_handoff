import logging
import threading

from dbb_buffer_mngr.snapshot import Snapshot


logger = logging.getLogger(__name__)


class Scanner(threading.Thread):
    """Class representing a directory scanner.

    Once started, it creates a snapshot of a given  directory.

    Parameters
    ----------
    directory : string
        A directory to scan.
    queue : queue.Queue
        Place to add the produced snapshot.

    """

    def __init__(self, directory, queue):
        threading.Thread.__init__(self)
        self.root = directory
        self.queue = queue

    def run(self):
        """Scan a directory to find files.
        """
        snap = Snapshot(self.root)
        self.queue.put(snap)
