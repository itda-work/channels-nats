"""The test harness itself: finding a nats-server binary on every supported platform.

Not marked ``integration`` -- these run without a server, which is the point:
they cover the path that decides whether the integration tests run at all.
"""

from pathlib import Path

import pytest
from conftest import find_nats_server


@pytest.fixture
def no_nats_server_around(monkeypatch, tmp_path):
    """No NATS_SERVER, nothing on PATH: only the ~/go/bin fallback is left."""
    monkeypatch.delenv("NATS_SERVER", raising=False)
    monkeypatch.setattr("conftest.shutil.which", lambda _: None)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    return tmp_path


def _install(home: Path, name: str) -> Path:
    binary = home / "go" / "bin" / name
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.touch()
    return binary


def test_go_install_binary_is_found(no_nats_server_around):
    binary = _install(no_nats_server_around, "nats-server")
    assert find_nats_server() == str(binary)


def test_go_install_binary_is_found_on_windows(no_nats_server_around):
    """``go install`` writes nats-server.exe there; skipping every integration
    test on Windows is not what "Windows is first class" means."""
    binary = _install(no_nats_server_around, "nats-server.exe")
    assert find_nats_server() == str(binary)


def test_nothing_installed_means_nothing_found(no_nats_server_around):
    assert find_nats_server() is None
