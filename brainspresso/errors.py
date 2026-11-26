"""Exceptions shown to users without a traceback."""


class UsageError(ValueError):
    """
    A command-line value that cannot be used.

    Raised by option validation (unknown site, option not available for
    a dataset, conflicting options...). The command line prints its
    message, without a traceback, and exits with status 1.
    """


class DatasetNotFound(UsageError, FileNotFoundError):
    """A dataset folder that a command needs does not exist."""
