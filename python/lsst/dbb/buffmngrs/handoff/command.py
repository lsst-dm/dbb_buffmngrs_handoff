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

import abc

__all__ = ["Command", "Macro"]


class Command(abc.ABC):
    """Class representing a command.
    """

    def __str__(self):
        return type(self).__name__

    @abc.abstractmethod
    def run(self):
        """Execute the command.
        """
        pass


class Macro(Command):
    """Class representing a sequence of commands.
    """

    def __init__(self):
        self.commands = []

    def add(self, cmd):
        """Add a command.

        Parameters
        ----------
        cmd : Command
            A command to be added to the sequence.
        """
        if not isinstance(cmd, Command):
            name = type(cmd).__name__
            raise ValueError(f"'{name}' object is not a valid command")
        self.commands.append(cmd)

    def run(self):
        """Execute the command sequence.
        """
        for cmd in self.commands:
            cmd.run()
