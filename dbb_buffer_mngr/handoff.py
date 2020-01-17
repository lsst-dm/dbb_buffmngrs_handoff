import logging
import os
import time


logger = logging.getLogger(__name__)


class Scanner(object):
    """Command finding out all files in a given directory tree.

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


class Mover(object):
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
        if not os.path.exists(path) or not os.path.isdir(path):
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


class Eraser(object):
    """Command removing empty directories from the holding area.

    Parameters
    ----------
    config : dict
        Configuration of the handoff site.

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
        if not os.path.exists(path) or not os.path.isdir(path):
            msg = f"{path}: directory not found."
            logger.critical(msg)
            raise ValueError(msg)
        self.root = path
        self.exp_time = exp_time

    def run(self):
        """Remove empty directories older than a given time.
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
                os.rmdir(path)
                logger.debug(f"Directory '{path}' removed successfully.")


class Cleaner(object):
    """Macro command.
    """

    def __init__(self):
        self.cmds = []

    def add(self, cmd):
        """Add a command to the macro.

        Parameters
        ----------
        cmd : Command
            A command to execute.
        """
        self.cmds.append(cmd)

    def run(self):
        """Execute macro.
        """
        for cmd in self.cmds:
            cmd.run()
