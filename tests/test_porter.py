import getpass
import os
import queue
import shutil
import tempfile
import unittest
from dbb_buffer_mngr import Porter


class PorterTestCase(unittest.TestCase):
    """Test the command transferring files between handoff and endpoint sites.
    """

    def setUp(self):
        self.src = tempfile.mkdtemp()
        self.dst = tempfile.mkdtemp()
        self.stg = tempfile.mkdtemp()
        self.user = getpass.getuser()
        self.host = "localhost"
        sub = tempfile.mkdtemp(dir=self.src)
        src = dict([tempfile.mkstemp(dir=self.src)])
        src.update(dict(tempfile.mkstemp(dir=sub) for _ in range(2)))
        for fd in src:
            os.close(fd)

        self.todo = queue.Queue()
        for path in src.values():
            tail = os.path.relpath(path, start=self.src)
            dn, bn = os.path.split(tail)
            self.todo.put((self.src, dn, bn))
        self.done = queue.Queue()

    def tearDown(self):
        shutil.rmtree(self.src)
        shutil.rmtree(self.dst)
        shutil.rmtree(self.stg)

    def testInvalidConfig(self):
        """Test if Porter complains about an invalid configurations.
        """
        config = dict()
        args = [config, self.todo, self.done]
        self.assertRaises(ValueError, Porter, *args)

        config = dict(buffer=self.src, staging=self.stg)
        args = [config, self.todo, self.done]
        self.assertRaises(ValueError, Porter, *args)

        config = dict(buffer=self.src, staging=self.stg, user=self.user)
        args = [config, self.todo, self.done]
        self.assertRaises(ValueError, Porter, *args)

    def testRun(self):
        """Test if Porter transfers files between handoff and endpoint sites.
        """
        config = dict(buffer=self.dst, staging=self.stg, user=self.user, host=self.host)
        cmd = Porter(config, self.todo, self.done)
        cmd.run()

        src = set()
        for top, subs, names in os.walk(self.src):
            for name in names:
                src.add(name)
        dst = set()
        for top, subs, names in os.walk(self.dst):
            for name in names:
                dst.add(name)
        self.assertEqual(src, dst)
        self.assertEqual(self.todo.qsize(), 0)
        self.assertEqual(self.done.qsize(), 3)
