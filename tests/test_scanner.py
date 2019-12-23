import logging
import os
import queue
import shutil
import tempfile
import unittest
from dbb_buffer_mngr.scanner import  Scanner


class ScannerTestCase(unittest.TestCase):
    """Test directory scanner.
    """

    def setUp(self):
        self.queue = queue.Queue()
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root)

    def testValidPath(self):
        pass

    def testInvalidPath(self):
        self.assertRaises(ValueError, Scanner, "/not/a/path", self.queue)

    def testEmptyDir(self):
        s = Scanner(self.root, self.queue)
        s.start()
        s.join()
        self.assertEqual(self.queue.qsize(), 0)

    def testNonEmptyDir(self):
        leaf = tempfile.mkdtemp(dir=self.root)
        files = dict([tempfile.mkstemp(dir=self.root)])
        files.update(dict(tempfile.mkstemp(dir=leaf) for _ in range(2)))
        for fd, _ in files.items():
            os.close(fd)

        s = Scanner(self.root, self.queue)
        s.start()
        s.join()
        self.assertEqual(self.queue.qsize(), 3)








