import logging
from pathlib import Path


def setup_filelog(
    filename: str | Path | None,
    level: int | str | None = None
) -> None:
    """
    Set file log
    """
    if level is not None:
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)
        logging.getLogger("brainspresso").setLevel(level)

    if filename:
        handler = logging.FileHandler(str(filename))
        handler.setFormatter(logging.Formatter(
            "(%(asctime)s)\t[%(levelname)-5.5s]\t%(message)s\t{%(name)s}"
        ))
        logging.getLogger().addHandler(handler)


class LoggingOutputSuppressor:
    """Context manager to prevent global logger from printing"""

    def __init__(self, logger) -> None:
        self.logger = logger

    def __enter__(self) -> None:
        logger = self.logger
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        self.orig_handlers = logger.handlers
        for handler in self.orig_handlers:
            logger.removeHandler(handler)

    def __exit__(self, exc, value, tb) -> None:
        logger = self.logger
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        for handler in self.orig_handlers:
            logger.addHandler(handler)


class HideLoggingStream:
    # This is the logic from dandi's LogSafeTabular

    @staticmethod
    def exclude_all(r):
        return False

    def __enter__(self):
        root = logging.getLogger()
        if root.handlers:
            for h in root.handlers:
                # Use `type()` instead of `isinstance()` because FileHandler is
                # a subclass of StreamHandler, and we don"t want to disable it:
                if type(h) is logging.StreamHandler:
                    h.addFilter(self.exclude_all)
            self.__added_handler = None
        else:
            self.__added_handler = logging.NullHandler()
            root.addHandler(self.__added_handler)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        root = logging.getLogger()
        for h in root.handlers:
            # Use `type()` instead of `isinstance()` because FileHandler is
            # a subclass of StreamHandler, and we don"t want to disable it:
            if type(h) is logging.StreamHandler:
                h.removeFilter(self.exclude_all)
        if self.__added_handler is not None:
            root.removeHandler(self.__added_handler)
            self.__added_handler = None
