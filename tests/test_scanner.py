import logging
import os
import queue
import shutil
import tempfile
import unittest
from dbb_buffer_mngr import Scanner


class ScannerTestCase(unittest.TestCase):
    """Test the command finding files in a give directory.
    """

    def setUp(self):
        self.queue = queue.Queue()
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root)

    def testInvalidConfig(self):
        config = dict()
        args = [config, self.queue]
        self.assertRaises(ValueError, Scanner, *args)

    def testInvalidBuffer(self):
        config = dict(buffer="/not/a/path")
        args = [config, self.queue]
        self.assertRaises(ValueError, Scanner, *args)

    def testEmptyDir(self):
        config = dict(buffer=self.root)
        s = Scanner(config, self.queue)
        s.run()
        self.assertEqual(self.queue.qsize(), 0)

    def testNonEmptyDir(self):
        leaf = tempfile.mkdtemp(dir=self.root)
        files = dict([tempfile.mkstemp(dir=self.root)])
        files.update(dict(tempfile.mkstemp(dir=leaf) for _ in range(2)))
        for fd in files:
            os.close(fd)

        config = dict(buffer=self.root)
        s = Scanner(config, self.queue)
        s.run()
        self.assertEqual(self.queue.qsize(), 3)








