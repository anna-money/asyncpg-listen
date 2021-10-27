import logging
import socket
import time
import uuid

import docker
import psycopg2
import pytest

logging.basicConfig()


@pytest.fixture(scope="session")
def unused_port():
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
