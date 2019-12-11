import os


class Snapshot(object):
    """Class representing a directory snapshot.

    Parameters
    ----------
    path : basestring
        Directory to make snapshot for.

    Notes
    -----
    Current implementation does not represent a directory snapshot in a strict
    sense. It is only concerned with files.
    """

    def __init__(self, path):
        self._root = path
        self._stats = {}
        for topdir, subdir, filenames in os.walk(self.root):
            for fn in filenames:
                p = os.path.join(topdir, fn)
                self._stats[p] = os.stat(p)

    @property
    def paths(self):
        """Set of all paths in the current snapshot.
        """
        return set(self._stats)

    @property
    def root(self):
        """Directory then snapshot was made for.
        """
        return self._root

    def modified(self, other):
        """Find out paths which status was changed.

        Parameters
        ----------
        other : Snapshot
            The directory snapshot to compare to.

        Returns
        -------
        set
            Files which were modified.
        """
        modified = set()
        for path in self.paths & other.paths:
            curr_info, other_info = self.stat_info(path), other.stat_info(path)
            if curr_info.st_ctime != other_info.st_ctime:
                modified.add(path)
        return modified

    def created(self, other):
        """Find out paths which exists only in the current snapshot.

        Parameters
        ----------
        other : Snapshot
            The directory snapshot to compare to.

        Returns
        -------
        set
           Paths which exists only in the current snapshot, but not the other.
        """
        return self.paths - other.paths

    def diff(self, other):
        """Find out paths which differs comparing to other snapshot.

        Parameters
        ----------
        other : Snapshot
            Directory snapshot to compare to.

        Returns
        -------
        set
            Paths which somehow differs (are new, were modified) from paths in
            the other snapshot.
        """
        changed = set()
        changed.update(self.created(other))
        changed.update(self.modified(other))
        return changed

    def stat_info(self, path):
        """Get path status.

        Parameters
        ----------
        path : basestring
            Path to get the status for.
        """
        return self._stats[path]
