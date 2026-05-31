import sys
from importlib.metadata import version as _get_version

from .listener import (
    NO_TIMEOUT,
    ConnectFunc,
    ListenPolicy,
    Notification,
    NotificationHandler,
    NotificationListener,
    NotificationOrTimeout,
    Timeout,
    connect_func,
)

__all__: tuple[str, ...] = (
    # listener.py
    "NO_TIMEOUT",
    "ConnectFunc",
    "ListenPolicy",
    "Notification",
    "NotificationHandler",
    "NotificationListener",
    "NotificationOrTimeout",
    "Timeout",
    "connect_func",
)

__version__ = _get_version("asyncpg-listen")

version = f"{__version__}, Python {sys.version}"
