import getpass
import os
import shutil
import tempfile
import unittest
from dbb_buffer_mngr import Wiper


class WiperTestCase(unittest.TestCase):
    """Test the command cleaning up the staging area on the endpoint site.
    """

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.user = getpass.getuser()
        self.host = "localhost"

    def tearDown(self):
        shutil.rmtree(self.dir)

    def testInvalidConfig(self):
        config = dict()
        args = [config]
        self.assertRaises(ValueError, Wiper, *args)

    def testEmpty(self):
        subdir = tempfile.mkdtemp(dir=self.dir)

        config = dict(user=self.user, host=self.host, staging=self.dir)
        cmd = Wiper(config)
        cmd.run()

        dirs = []
        for topdir, subdirs, files in os.walk(self.dir):
            for d in subdirs:
                dirs.append(d)
        self.assertEqual(len(dirs), 0)

    def testNonEmpty(self):
        subdir = tempfile.mkdtemp(dir=self.dir)
        fd, fn = tempfile.mkstemp(dir=os.path.join(self.dir, subdir))
        os.close(fd)

        config = dict(user=self.user, host=self.host, staging=self.dir)
        cmd = Wiper(config)
        cmd.run()

        dirs = []
        for top, subs, files in os.walk(self.dir):
            for d in subs:
                dirs.append(d)
        self.assertEqual(len(dirs), 1)
