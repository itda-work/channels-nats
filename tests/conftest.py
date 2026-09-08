"""Integration tests need a nats-server binary: NATS_SERVER env var, PATH, or ~/go/bin."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
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


def _wait_for_routes(monitor_port: int, expected: int, timeout: float = 20.0) -> None:
    """A route is up only when the node says so; publishing before that goes nowhere."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{monitor_port}/routez", timeout=1) as response:
                if json.load(response)["num_routes"] >= expected:
                    return
        except (urllib.error.URLError, OSError, KeyError, ValueError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"node on monitor port {monitor_port} never saw {expected} routes")


@dataclass
class _Cluster:
    urls: list[str]
    processes: list[subprocess.Popen] = field(default_factory=list)

    def stop(self, index: int) -> None:
        """Take one node down, as a rolling restart or a crash would."""
        process = self.processes[index]
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


@pytest.fixture
def nats_cluster():
    """Three nodes in one cluster, as in README's 클러스터로 확장.

    Function-scoped: a test may take a node down, and the next one gets a whole
    cluster back.
    """
    binary = find_nats_server()
    if binary is None:
        pytest.skip("nats-server binary not found (set NATS_SERVER or put it on PATH)")
    client_ports = [_free_port() for _ in range(3)]
    route_ports = [_free_port() for _ in range(3)]
    monitor_ports = [_free_port() for _ in range(3)]
    routes = ",".join(f"nats://127.0.0.1:{port}" for port in route_ports)

    cluster = _Cluster(urls=[f"nats://127.0.0.1:{port}" for port in client_ports])
    for index in range(3):
        cluster.processes.append(
            subprocess.Popen(
                [
                    binary,
                    "-a",
                    "127.0.0.1",
                    "-p",
                    str(client_ports[index]),
                    "-m",
                    str(monitor_ports[index]),
                    "--name",
                    f"n{index}",
                    "--cluster_name",
                    "channels-nats-test",
                    "--cluster",
                    f"nats://127.0.0.1:{route_ports[index]}",
                    "--routes",
                    routes,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        )
    try:
        for port in client_ports:
            _wait_for_port(port)
        for port in monitor_ports:
            _wait_for_routes(port, expected=2)
        yield cluster
    finally:
        for index in range(len(cluster.processes)):
            cluster.stop(index)


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
        instance = NatsChannelLayer(**{"servers": nats_url, **config})
        created.append(instance)
        return instance

    yield factory
    for instance in created:
        try:  # a test may have taken the node this layer was talking to down
            await instance.flush()
        except Exception:
            pass
        await instance.close()
