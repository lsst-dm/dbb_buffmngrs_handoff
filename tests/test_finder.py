# This file is part of dbb_buffer_mngr.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import queue
import shutil
import tempfile
import unittest
from lsst.dbb.buffmngrs.handoff import Finder


class ScannerTestCase(unittest.TestCase):
    """Test the command finding files in a give directory.
    """

    def setUp(self):
        self.queue = queue.Queue()
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.root)

    def testInvalidConfig(self):
        """Test if Scanner complains about an invalid configuration.
        """
        config = dict()
        args = [config, self.queue]
        self.assertRaises(ValueError, Finder, *args)

    def testInvalidBuffer(self):
        """Test if Scanner complains about a non-existing buffer.
        """
        config = dict(buffer="/not/a/path")
        args = [config, self.queue]
        self.assertRaises(ValueError, Finder, *args)

    def testEmptyDir(self):
        """Test if Scanner handles empty directories correctly.
        """
        config = dict(buffer=self.root)
        s = Finder(config, self.queue)
        s.run()
        self.assertEqual(self.queue.qsize(), 0)

    def testNonEmptyDir(self):
        """Test if Scanner finds all files in a directory.
        """
        leaf = tempfile.mkdtemp(dir=self.root)
        files = dict([tempfile.mkstemp(dir=self.root)])
        files.update(dict(tempfile.mkstemp(dir=leaf) for _ in range(2)))
        for fd in files:
            os.close(fd)

        config = dict(buffer=self.root)
        s = Finder(config, self.queue)
        s.run()
        self.assertEqual(self.queue.qsize(), 3)
