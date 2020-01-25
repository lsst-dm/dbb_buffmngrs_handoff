import abc


class Command(abc.ABC):
    """Class representing a command.
    """

    @abc.abstractmethod
    def run(self):
        pass

    def __str__(self):
        return type(self).__name__


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
