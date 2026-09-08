"""Integration tests need a nats-server binary: NATS_SERVER env var, PATH, or ~/go/bin."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest

from channels_nats import NatsChannelLayer


def find_nats_server() -> str | None:
    candidates = [
        os.environ.get("NATS_SERVER"),
        shutil.which("nats-server"),
        str(Path.home() / "go" / "bin" / "nats-server"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_port(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("nats-server did not start")


@pytest.fixture(scope="session")
def nats_url():
    binary = find_nats_server()
    if binary is None:
        pytest.skip("nats-server binary not found (set NATS_SERVER or put it on PATH)")
    port = _free_port()
    proc = subprocess.Popen(
        [binary, "-a", "127.0.0.1", "-p", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        _wait_for_port(port)
        yield f"nats://127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture
async def layer(nats_url):
    layer = NatsChannelLayer(servers=nats_url)
    try:
        yield layer
    finally:
        await layer.flush()
        await layer.close()


@pytest.fixture
async def make_layer(nats_url):
    """Extra layer instances stand in for other processes; all are closed on teardown."""
    created: list[NatsChannelLayer] = []

    def factory(**config) -> NatsChannelLayer:
        instance = NatsChannelLayer(servers=nats_url, **config)
        created.append(instance)
        return instance

    yield factory
    for instance in created:
        await instance.flush()
        await instance.close()
