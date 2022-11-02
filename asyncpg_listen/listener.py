import asyncio
import contextlib
import dataclasses
import enum
import logging
import sys
from typing import Any, Callable, Coroutine, Dict, List, Optional, Union

if sys.version_info < (3, 11, 0):
    from async_timeout import timeout
else:
    from asyncio import timeout  # type: ignore

import asyncpg

logger = logging.getLogger(__package__)


class ListenPolicy(str, enum.Enum):
    ALL = "ALL"
    LAST = "LAST"

    def __str__(self) -> str:
        return self.value


@dataclasses.dataclass(frozen=True)
class Timeout:
    __slots__ = ("channel",)

    channel: str


@dataclasses.dataclass(frozen=True)
class Notification:
    __slots__ = ("channel", "payload")

    channel: str
    payload: Optional[str]


ConnectFunc = Callable[[], Coroutine[Any, Any, asyncpg.Connection]]
NotificationOrTimeout = Union[Notification, Timeout]
NotificationHandler = Callable[[NotificationOrTimeout], Coroutine]

NO_TIMEOUT: float = -1


def connect_func(*args: Any, **kwargs: Any) -> ConnectFunc:
    async def _connect() -> asyncpg.Connection:
        return await asyncpg.connect(*args, **kwargs)

    return _connect


class NotificationListener:
    __slots__ = ("_connect", "_reconnect_delay")

    def __init__(self, connect: ConnectFunc, reconnect_delay: float = 5) -> None:
        self._reconnect_delay = reconnect_delay
        self._connect = connect

    async def run(
        self,
        handler_per_channel: Dict[str, NotificationHandler],
        *,
        policy: ListenPolicy = ListenPolicy.ALL,
        notification_timeout: float = 30,
    ) -> None:
        queue_per_channel: Dict[str, "asyncio.Queue[Notification]"] = {
            channel: asyncio.Queue() for channel in handler_per_channel.keys()
        }

        read_notifications_task = asyncio.create_task(
            self._read_notifications(
                queue_per_channel=queue_per_channel, check_interval=max(1.0, notification_timeout / 3.0)
            ),
            name=__package__,
        )
        process_notifications_tasks = [
            asyncio.create_task(
                self._process_notifications(
                    channel,
                    notifications=queue_per_channel[channel],
                    handler=handler,
                    policy=policy,
                    notification_timeout=notification_timeout,
                ),
                name=f"{__package__}.{channel}",
            )
            for channel, handler in handler_per_channel.items()
        ]
        try:
            await asyncio.gather(read_notifications_task, *process_notifications_tasks)
        finally:
            await self._cancel_and_await_tasks([read_notifications_task, *process_notifications_tasks])

    @staticmethod
    async def _cancel_and_await_tasks(tasks: "List[asyncio.Task[None]]") -> None:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t

    @staticmethod
    async def _process_notifications(
        channel: str,
        *,
        notifications: "asyncio.Queue[Notification]",
        handler: NotificationHandler,
        policy: ListenPolicy,
        notification_timeout: float,
    ) -> None:
        while True:
            notification: NotificationOrTimeout

            if notifications.empty():
                if notification_timeout == NO_TIMEOUT:
                    notification = await notifications.get()
                else:
                    try:
                        async with timeout(notification_timeout):
                            notification = await notifications.get()
                    except asyncio.TimeoutError:
                        notification = Timeout(channel)
            else:
                while not notifications.empty():
                    notification = notifications.get_nowait()
                    if policy == ListenPolicy.ALL:
                        break

            # to have independent async context per run
            # to protect from misuse of contextvars
            try:
                await asyncio.create_task(handler(notification), name=f"{__package__}.{channel}")
            except Exception:
                logger.exception("Failed to handle %s", notification)

    async def _read_notifications(
        self, queue_per_channel: Dict[str, "asyncio.Queue[Notification]"], check_interval: float
    ) -> None:
        failed_connect_attempts = 0
        while True:
            try:
                connection = await self._connect()
                failed_connect_attempts = 0
                try:
                    for channel, queue in queue_per_channel.items():
                        await connection.add_listener(channel, self._get_push_callback(queue))

                    while True:
                        await asyncio.sleep(check_interval)
                        await connection.execute("SELECT 1")
                finally:
                    await asyncio.shield(connection.close())
            except Exception:
                logger.exception("Connection was lost or not established")

                await asyncio.sleep(self._reconnect_delay * failed_connect_attempts)
                failed_connect_attempts += 1

    @staticmethod
    def _get_push_callback(queue: "asyncio.Queue[Notification]") -> Callable[[Any, Any, Any, Any], None]:
        def _push(_: Any, __: Any, channel: Any, payload: Any) -> None:
            queue.put_nowait(Notification(channel, payload))

        return _push
