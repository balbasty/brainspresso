"""Generic helpers for BIDS datasets hosted remotely."""
from .select import BidsName, Selection  # noqa: F401
from .tables import query_participants, trim_tables  # noqa: F401
