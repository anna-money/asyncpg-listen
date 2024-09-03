import asyncio
import contextlib
import dataclasses
import enum
import logging
import sys
import time
from typing import Any, Callable, Coroutine

import asyncpg
import opentelemetry.metrics
import opentelemetry.trace

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
    __slots__ = ("_connect", "_reconnect_delay", "_tracer", "_meter", "_notification_histogram")

    def __init__(self, connect: ConnectFunc, reconnect_delay: float = 5) -> None:
        self._reconnect_delay = reconnect_delay
        self._connect = connect
        self._tracer = None
        self._meter = None
        self._notification_histogram = None

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
    async def _cancel_and_await_tasks(tasks: list[asyncio.Task[None]]) -> None:
        for t in tasks:
            t.cancel()
        for t in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t

    async def _process_notifications(
        self,
        channel: str,
        *,
        notifications: asyncio.Queue[Notification],
        handler: NotificationHandler,
        policy: ListenPolicy,
        notification_timeout: float,
    ) -> None:
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
                # to have independent async context per run
                # to protect from misuse of contextvars
                await run_coro(self._process_notification(handler, notification))
            except Exception:
                logger.exception("Failed to handle %s", notification)

    async def _read_notifications(
        self, queue_per_channel: dict[str, asyncio.Queue[Notification]], check_interval: float
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
    def _get_push_callback(queue: asyncio.Queue[Notification]) -> Callable[[Any, Any, Any, Any], None]:
        def _push(_: Any, __: Any, channel: Any, payload: Any) -> None:
            queue.put_nowait(Notification(channel, payload))

        return _push

    async def _process_notification(self, handler: NotificationHandler, notification: NotificationOrTimeout) -> None:
        if self._tracer is None:
            self._tracer = opentelemetry.trace.get_tracer(__package__)  # type: ignore
        if self._meter is None:
            self._meter = opentelemetry.metrics.get_meter(__package__)  # type: ignore
        if self._notification_histogram is None:
            self._notification_histogram = self._meter.create_histogram("asyncpg_listener_latency")

        start_time = time.time_ns()

        with self._tracer.start_as_current_span(
            name=self._get_span_name(notification),
            kind=opentelemetry.trace.SpanKind.INTERNAL,
            start_time=start_time,
            attributes={"channel": notification.channel},
        ):
            try:
                await handler(notification)
            finally:
                elapsed = max(0, time.time_ns() - start_time)
                self._notification_histogram.record(elapsed, {"channel": notification.channel})

    @staticmethod
    def _get_span_name(notification: NotificationOrTimeout) -> str:
        if isinstance(notification, Timeout):
            return f"Notification timeout #{notification.channel}"
        if isinstance(notification, Notification):
            return f"Notification #{notification.channel}"

        raise TypeError(f"Unexpected notification type: {type(notification)}")
