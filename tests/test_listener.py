import asyncio
import contextlib
import dataclasses
import logging
from typing import Awaitable, Callable

import asyncpg
import pytest
import pytest_pg

import asyncpg_listen

from .conftest import TcpProxy


class Handler:
    def __init__(self, delay: float = 0) -> None:
        self.delay = delay
        self.notifications: list[asyncpg_listen.NotificationOrTimeout] = []

    async def handle(self, notification: asyncpg_listen.NotificationOrTimeout) -> None:
        await asyncio.sleep(self.delay)
        self.notifications.append(notification)


async def cancel_and_wait(future: "asyncio.Future[None]") -> None:
    future.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await future


async def test_two_inactive_channels(pg_14: pytest_pg.PG) -> None:
    handler_1 = Handler()
    handler_2 = Handler()
    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(**dataclasses.asdict(pg_14)))
    listener_task = asyncio.create_task(
        listener.run({"inactive_1": handler_1.handle, "inactive_2": handler_2.handle}, notification_timeout=1)
    )

    await asyncio.sleep(1.5)
    await cancel_and_wait(listener_task)

    assert handler_1.notifications == [asyncpg_listen.Timeout("inactive_1")]
    assert handler_2.notifications == [asyncpg_listen.Timeout("inactive_2")]


async def test_one_active_channel_and_one_passive_channel(pg_14: pytest_pg.PG) -> None:
    active_handler = Handler()
    inactive_handler = Handler()
    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(**dataclasses.asdict(pg_14)))
    listener_task = asyncio.create_task(
        listener.run({"active": active_handler.handle, "inactive": inactive_handler.handle}, notification_timeout=1)
    )
    connection = await asyncpg.connect(**dataclasses.asdict(pg_14))
    try:
        await asyncio.sleep(0.75)
        await connection.execute("NOTIFY active, '1'")
        await connection.execute("NOTIFY active, '2'")
        await asyncio.sleep(0.75)
    finally:
        await asyncio.shield(connection.close())

    await cancel_and_wait(listener_task)

    assert active_handler.notifications == [
        asyncpg_listen.Notification("active", "1"),
        asyncpg_listen.Notification("active", "2"),
    ]
    assert inactive_handler.notifications == [
        asyncpg_listen.Timeout("inactive"),
    ]


async def test_two_active_channels(pg_14: pytest_pg.PG) -> None:
    handler_1 = Handler()
    handler_2 = Handler()
    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(**dataclasses.asdict(pg_14)))
    listener_task = asyncio.create_task(
        listener.run({"active_1": handler_1.handle, "active_2": handler_2.handle}, notification_timeout=1)
    )
    await asyncio.sleep(0.1)

    connection = await asyncpg.connect(**dataclasses.asdict(pg_14))
    try:
        await connection.execute("NOTIFY active_1, '1'")
        await connection.execute("NOTIFY active_2, '2'")
        await connection.execute("NOTIFY active_2, '3'")
        await connection.execute("NOTIFY active_1, '4'")
        await asyncio.sleep(0.75)
    finally:
        await asyncio.shield(connection.close())

    await cancel_and_wait(listener_task)

    assert handler_1.notifications == [
        asyncpg_listen.Notification("active_1", "1"),
        asyncpg_listen.Notification("active_1", "4"),
    ]
    assert handler_2.notifications == [
        asyncpg_listen.Notification("active_2", "2"),
        asyncpg_listen.Notification("active_2", "3"),
    ]


async def test_listen_policy_last(pg_14: pytest_pg.PG) -> None:
    handler = Handler(delay=0.1)
    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(**dataclasses.asdict(pg_14)))
    listener_task = asyncio.create_task(
        listener.run({"simple": handler.handle}, policy=asyncpg_listen.ListenPolicy.LAST, notification_timeout=1)
    )
    await asyncio.sleep(0.1)

    connection = await asyncpg.connect(**dataclasses.asdict(pg_14))
    try:
        for i in range(10):
            await connection.execute(f"NOTIFY simple, '{i}'")
        await asyncio.sleep(0.75)
    finally:
        await asyncio.shield(connection.close())

    await cancel_and_wait(listener_task)

    assert handler.notifications == [
        asyncpg_listen.Notification("simple", "0"),
        asyncpg_listen.Notification("simple", "9"),
    ]


async def test_listen_policy_all(pg_14: pytest_pg.PG) -> None:
    handler = Handler(delay=0.05)
    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(**dataclasses.asdict(pg_14)))
    listener_task = asyncio.create_task(listener.run({"simple": handler.handle}, notification_timeout=1))
    await asyncio.sleep(0.1)

    connection = await asyncpg.connect(**dataclasses.asdict(pg_14))
    try:
        for i in range(10):
            await connection.execute(f"NOTIFY simple, '{i}'")
        await asyncio.sleep(0.75)
    finally:
        await asyncio.shield(connection.close())

    await cancel_and_wait(listener_task)

    assert handler.notifications == [asyncpg_listen.Notification("simple", str(i)) for i in range(10)]


async def test_failed_to_connect() -> None:
    async def connect() -> asyncpg.Connection:
        raise RuntimeError("Failed to connect")

    handler = Handler()
    listener = asyncpg_listen.NotificationListener(connect)
    listener_task = asyncio.create_task(listener.run({"simple": handler.handle}, notification_timeout=1))
    await asyncio.sleep(1.5)
    await cancel_and_wait(listener_task)

    assert handler.notifications == [asyncpg_listen.Timeout("simple")]


async def test_failed_to_connect_no_timeout() -> None:
    async def connect() -> asyncpg.Connection:
        raise RuntimeError("Failed to connect")

    handler = Handler()
    listener = asyncpg_listen.NotificationListener(connect)
    listen_task = asyncio.create_task(
        listener.run({"simple": handler.handle}, notification_timeout=asyncpg_listen.NO_TIMEOUT)
    )
    await asyncio.sleep(1.5)
    await cancel_and_wait(listen_task)

    assert handler.notifications == []


async def test_failing_handler(pg_14: pytest_pg.PG) -> None:
    async def handle(_: asyncpg_listen.NotificationOrTimeout) -> None:
        raise RuntimeError("Oops")

    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(**dataclasses.asdict(pg_14)))
    listener_task = asyncio.create_task(listener.run({"simple": handle}, notification_timeout=1))

    await asyncio.sleep(0.1)

    connection = await asyncpg.connect(**dataclasses.asdict(pg_14))
    try:
        await connection.execute("NOTIFY simple")
        await connection.execute("NOTIFY simple")
        await connection.execute("NOTIFY simple")
    finally:
        await asyncio.shield(connection.close())

    await asyncio.sleep(0.75)

    assert not listener_task.done()

    await cancel_and_wait(listener_task)


async def test_reconnect(
    tcp_proxy: Callable[[int, int], Awaitable[TcpProxy]],
    unused_port: Callable[[], int],
    pg_14: pytest_pg.PG,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    proxy_port = unused_port()

    handler = Handler()
    proxy = await tcp_proxy(proxy_port, pg_14.port)
    listener = asyncpg_listen.NotificationListener(
        asyncpg_listen.connect_func(**{**(dataclasses.asdict(pg_14)), **{"port": proxy_port}})
    )

    listener_task = asyncio.create_task(listener.run({"simple": handler.handle}, notification_timeout=1))

    await asyncio.sleep(0.5)

    connection = await asyncpg.connect(**dataclasses.asdict(pg_14))
    try:
        await connection.execute("NOTIFY simple, 'before'")
    finally:
        await asyncio.shield(connection.close())

    await asyncio.sleep(0.5)
    await proxy.drop_connections()
    await asyncio.sleep(2)

    connection = await asyncpg.connect(**dataclasses.asdict(pg_14))
    try:
        await connection.execute("NOTIFY simple, 'after'")
    finally:
        await asyncio.shield(connection.close())

    await asyncio.sleep(0.5)
    await cancel_and_wait(listener_task)

    assert asyncpg_listen.Notification("simple", "before") in handler.notifications
    assert asyncpg_listen.Notification("simple", "after") in handler.notifications

    assert any(
        record
        for record in caplog.records
        if "Connection was lost or not established" in record.message or "Connection was lost" in record.message
    )
