import asyncio
import logging
import socket
import time
import uuid
from typing import AsyncIterable, Awaitable, Callable, Optional

import docker
import psycopg2
import pytest

logging.basicConfig()


@pytest.fixture(scope="session")
def unused_port() -> Callable[[], int]:
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    return f


@pytest.fixture(scope="session")
def session_id():
    return str(uuid.uuid4())


@pytest.fixture(scope="session")
def docker_client():
    return docker.APIClient(version="auto")


@pytest.fixture(scope="session")
def pg_server(unused_port, docker_client, session_id):
    docker_client.pull("postgres:11.6")

    container_args = dict(
        image="postgres:11.6",
        name=str(session_id),
        ports=[5432],
        detach=True,
    )

    # bound IPs do not work on OSX
    host = "127.0.0.1"
    host_port = unused_port()
    container_args["host_config"] = docker_client.create_host_config(port_bindings={5432: (host, host_port)})
    container_args["environment"] = {"POSTGRES_HOST_AUTH_METHOD": "trust"}

    container = docker_client.create_container(**container_args)

    try:
        docker_client.start(container=container["Id"])
        server_params = dict(
            database="postgres",
            user="postgres",
            password="mysecretpassword",
            host=host,
            port=host_port,
        )
        delay = 0.001
        for i in range(100):
            try:
                connection = psycopg2.connect(**server_params)
                connection.close()
                break
            except psycopg2.Error:
                time.sleep(delay)
                delay *= 2
        else:
            pytest.fail("Cannot start postgres server")

        container["host"] = host
        container["port"] = host_port
        container["pg_params"] = server_params

        yield container
    finally:
        docker_client.kill(container=container["Id"])
        docker_client.remove_container(container["Id"])


@pytest.fixture
def pg_params(pg_server):
    return dict(**pg_server["pg_params"])


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

    async def disconnect(self) -> None:
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

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ):
        server_reader, server_writer = await asyncio.open_connection(host=self.dst_host, port=self.dst_port)

        self.connections.add(server_writer)
        self.connections.add(client_writer)

        await asyncio.wait(
            [
                asyncio.ensure_future(self._pipe(server_reader, client_writer)),
                asyncio.ensure_future(self._pipe(client_reader, server_writer)),
            ]
        )


@pytest.fixture
async def tcp_proxy(loop: asyncio.AbstractEventLoop) -> AsyncIterable[Callable[[int, int], Awaitable[TcpProxy]]]:
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
            await asyncio.shield(proxy.disconnect())
