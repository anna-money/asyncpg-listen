import asyncio
import logging
import socket
from typing import AsyncIterable, Awaitable, Callable, Optional

import pytest

logging.basicConfig()


@pytest.fixture(scope="session")
def unused_port() -> Callable[[], int]:
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    return f


class TcpProxy:
    """
    TCP proxy. Allows simulating connection breaks in tests.
    """

    MAX_BYTES = 1024

    def __init__(self, *, src_port: int, dst_port: int):
        self.src_host = "127.0.0.1"
        self.src_port = src_port
        self.dst_host = "127.0.0.1"
        self.dst_port = dst_port
        self.connections = set()

    async def start(self) -> None:
        await asyncio.start_server(
            self._handle_client,
            host=self.src_host,
            port=self.src_port,
        )

    async def drop_connections(self) -> None:
        while self.connections:
            writer = self.connections.pop()
            writer.close()
            if hasattr(writer, "wait_closed"):
                await writer.wait_closed()

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                bytes_read = await reader.read(TcpProxy.MAX_BYTES)
                writer.write(bytes_read)
                await writer.drain()
        finally:
            writer.close()
            if hasattr(writer, "wait_closed"):
                await writer.wait_closed()

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ):
        server_reader, server_writer = await asyncio.open_connection(host=self.dst_host, port=self.dst_port)

        self.connections.add(server_writer)
        self.connections.add(client_writer)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._pipe(server_reader, client_writer))
            tg.create_task(self._pipe(client_reader, server_writer))


@pytest.fixture
async def tcp_proxy(event_loop: asyncio.AbstractEventLoop) -> AsyncIterable[Callable[[int, int], Awaitable[TcpProxy]]]:
    proxy: Optional[TcpProxy] = None

    async def go(src_port: int, dst_port: int) -> TcpProxy:
        nonlocal proxy
        proxy = TcpProxy(
            dst_port=dst_port,
            src_port=src_port,
        )
        await proxy.start()
        return proxy

    try:
        yield go
    finally:
        if proxy is not None:
            await asyncio.shield(proxy.drop_connections())
