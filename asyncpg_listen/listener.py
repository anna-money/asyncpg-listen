import asyncio
import dataclasses
import enum
import logging
import sys
from typing import Any, Callable, Coroutine

import asyncpg

logger = logging.getLogger(__package__)


class ListenPolicy(enum.StrEnum):
    ALL = "ALL"
    LAST = "LAST"


@dataclasses.dataclass(frozen=True, slots=True)
class Timeout:
    channel: str


@dataclasses.dataclass(frozen=True, slots=True)
class Notification:
    channel: str
    payload: str | None


ConnectFunc = Callable[[], Coroutine[Any, Any, asyncpg.Connection]]
NotificationOrTimeout = Notification | Timeout
NotificationHandler = Callable[[NotificationOrTimeout], Coroutine]

NO_TIMEOUT: float = -1


def connect_func(*args: Any, **kwargs: Any) -> ConnectFunc:
    async def _connect() -> asyncpg.Connection:
        return await asyncpg.connect(*args, **kwargs)

    return _connect


class NotificationListener:
    __slots__ = (
        "__connect",
        "__reconnect_delay",
    )

    def __init__(self, connect: ConnectFunc, reconnect_delay: float = 5) -> None:
        self.__reconnect_delay = reconnect_delay
        self.__connect = connect

    async def run(
        self,
        handler_per_channel: dict[str, NotificationHandler],
        *,
        policy: ListenPolicy = ListenPolicy.ALL,
        notification_timeout: float = 30,
    ) -> None:
        queue_per_channel: dict[str, asyncio.Queue[Notification]] = {
            channel: asyncio.Queue() for channel in handler_per_channel.keys()
        }
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                self.__read_notifications(
                    queue_per_channel=queue_per_channel, check_interval=max(1.0, notification_timeout / 3.0)
                ),
                name=__package__,
            )
            for channel, handler in handler_per_channel.items():
                tg.create_task(
                    self.__process_notifications(
                        channel,
                        notifications=queue_per_channel[channel],
                        handler=handler,
                        policy=policy,
                        notification_timeout=notification_timeout,
                    ),
                    name=f"{__package__}.{channel}",
                )

    @staticmethod
    async def __process_notifications(
        channel: str,
        *,
        notifications: asyncio.Queue[Notification],
        handler: NotificationHandler,
        policy: ListenPolicy,
        notification_timeout: float,
    ) -> None:
        # to have independent async context per run
        # to protect from misuse of contextvars
        if sys.version_info >= (3, 12):
            loop = asyncio.get_running_loop()

            async def run_coro(c: Coroutine) -> None:
                await asyncio.Task(c, loop=loop, eager_start=True, name=f"{__package__}.{channel}")

        else:

            async def run_coro(c: Coroutine) -> None:
                await asyncio.create_task(c, name=f"{__package__}.{channel}")

        while True:
            notification: NotificationOrTimeout | None = None

            if notifications.empty():
                if notification_timeout == NO_TIMEOUT:
                    notification = await notifications.get()
                else:
                    try:
                        async with asyncio.timeout(notification_timeout):
                            notification = await notifications.get()
                    except asyncio.TimeoutError:
                        notification = Timeout(channel)
            else:
                while not notifications.empty():
                    notification = notifications.get_nowait()
                    if policy == ListenPolicy.ALL:
                        break

            if notification is None:
                continue

            try:
                await run_coro(handler(notification))
            except Exception:
                logger.exception("Failed to handle %s", notification)

    async def __read_notifications(
        self, queue_per_channel: dict[str, asyncio.Queue[Notification]], check_interval: float
    ) -> None:
        failed_connect_attempts = 0
        while True:
            try:
                connection = await self.__connect()
                failed_connect_attempts = 0
                try:
                    for channel, queue in queue_per_channel.items():
                        await connection.add_listener(channel, self.__get_push_callback(queue))

                    while True:
                        await asyncio.sleep(check_interval)
                        await connection.execute("SELECT 1")
                finally:
                    await asyncio.shield(connection.close())
            except Exception:
                logger.exception("Connection was lost or not established")

                await asyncio.sleep(self.__reconnect_delay * failed_connect_attempts)
                failed_connect_attempts += 1

    @staticmethod
    def __get_push_callback(queue: asyncio.Queue[Notification]) -> Callable[[Any, Any, Any, Any], None]:
        def _push(_: Any, __: Any, channel: Any, payload: Any) -> None:
            queue.put_nowait(Notification(channel, payload))

        return _push
