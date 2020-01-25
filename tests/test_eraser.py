import os
import shutil
import tempfile
import time
import unittest
from dbb_buffer_mngr import Eraser


class EraserTestCase(unittest.TestCase):
    """Test the command cleaning up the buffer on the handoff site.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir)

    def testInvalidConfig(self):
        """Test if Eraser complains about invalid configuration.
        """
        config = dict()
        args = [config]
        self.assertRaises(ValueError, Eraser, *args)

    def testInvalidBuffer(self):
        """Test if Eraser complains about a non-existing buffer.
        """
        config = dict(buffer="/not/a/path")
        args = [config]
        self.assertRaises(ValueError, Eraser, *args)

    def testNonExpiredDir(self):
        """Test if Eraser does not remove a non-expired directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)

        config = dict(buffer=self.dir)
        cmd = Eraser(config)
        cmd.run()

        dirs = []
        for topdir, subdirs, files in os.walk(self.dir):
            for d in subdirs:
                dirs.append(d)
        self.assertEqual(len(dirs), 1)

    def testExpiredDir(self):
        """Test if Eraser removes an expired directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)
        time.sleep(2)

        config = dict(buffer=self.dir)
        cmd = Eraser(config, exp_time=1)
        cmd.run()

        dirs = []
        for top, subs, files in os.walk(self.dir):
            for d in subs:
                dirs.append(d)
        self.assertEqual(len(dirs), 0)

    def testNonEmptyDir(self):
        """Test if Eraser does not remove a non-empty directory.
        """
        subdir = tempfile.mkdtemp(dir=self.dir)
        files = dict([tempfile.mkstemp(dir=subdir) for _ in range(2)])
        for fd in files:
            os.close(fd)
        time.sleep(2)

        dirs = []
        for top, subs, files in os.walk(self.dir):
            for d in subs:
                dirs.append(d)
        self.assertEqual(len(dirs), 1)

