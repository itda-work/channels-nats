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


def test_a_server_that_does_not_start_says_why():
    """The fixture used to send nats-server's output to DEVNULL, and a CI job died
    on "nats-server did not start" with nothing else to go on."""
    from conftest import _free_port, _start_server, _wait_for_port

    binary = find_nats_server()
    if binary is None:
        pytest.skip("nats-server binary not found")
    process = _start_server([binary, "--no-such-flag"])
    try:
        with pytest.raises(RuntimeError) as failure:
            _wait_for_port(_free_port(), process, timeout=2)
    finally:
        process.terminate()
        process.wait(timeout=5)
    message = str(failure.value)
    assert "exited with" in message, message
    assert len(message.split("It said:", 1)[1].strip()) > 100, message  # its usage text, not silence
