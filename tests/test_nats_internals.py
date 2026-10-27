"""The nats-py internals the layer stands on, checked on a real connection (#51).

The layer reaches past nats-py's public API in a few places. Each has a fallback
that keeps it running with the feature off, and says so once in the log -- in the
field, where nobody may read it. This is where a nats-py upgrade that moved one
of them fails instead, naming what went and what the layer loses with it.

Every name is checked on every platform, the proactor-only ones included: the
macOS and Linux cells see a rename as soon as the Windows one does.
"""

import asyncio
import inspect
import logging

import pytest
from nats.aio.client import Client
from nats.aio.transport import TcpTransport

from channels_nats import NatsChannelLayer, _nats_patches
from channels_nats import layer as layer_module

pytestmark = pytest.mark.integration

CUT = "the layer's _cut: a close that has to be cut can no longer abort the socket or count what it discarded (#35)"
SNDBUF = (
    "the layer's _bound_kernel_sends: no SO_SNDBUF=0 on Windows' proactor loop, "
    "so a wedged broker is not bounded by publish_deadline (#47)"
)
PONG = "the #41 patch in _nats_patches: a late PONG for a cancelled flush ends the read loop again"


def _changed(problems: list[str], what: str, *affected: str) -> None:
    problems.append(f"{what}. Affects {'; '.join(affected)}")


def _fail(problems: list[str]) -> None:
    if problems:
        pytest.fail(
            "nats-py internals the layer relies on have changed:\n" + "\n".join(f"- {problem}" for problem in problems),
            pytrace=False,
        )


async def test_the_internals_the_layer_reaches_for_are_there(layer):
    client = await layer._client()
    problems: list[str] = []

    if not isinstance(getattr(client, "pending_data_size", None), int):
        _changed(problems, "Client.pending_data_size is not an int", CUT)

    if not hasattr(client, "_transport"):
        _changed(problems, "Client._transport is gone", CUT, SNDBUF)
        _fail(problems)
    transport = client._transport
    if not isinstance(transport, TcpTransport):
        _changed(
            problems, f"Client._transport is {type(transport).__name__}, not TcpTransport, on nats://", CUT, SNDBUF
        )
        _fail(problems)

    if not inspect.iscoroutinefunction(TcpTransport.connect):
        _changed(problems, "TcpTransport.connect is no longer a coroutine function", SNDBUF)
    if not hasattr(transport, "__dict__"):
        _changed(problems, "a TcpTransport takes no instance attributes, so its connect cannot be wrapped", SNDBUF)

    writer = getattr(transport, "_io_writer", None)
    if not hasattr(transport, "_io_writer"):
        _changed(problems, "TcpTransport._io_writer is gone", CUT, SNDBUF)
    elif not isinstance(writer, asyncio.StreamWriter):
        _changed(problems, f"TcpTransport._io_writer is {type(writer).__name__}, not asyncio.StreamWriter", CUT, SNDBUF)
    else:
        if not all(hasattr(writer.transport, name) for name in ("abort", "get_write_buffer_size")):
            _changed(problems, "TcpTransport._io_writer.transport has no abort() or get_write_buffer_size()", CUT)
        if not hasattr(writer.get_extra_info("socket"), "setsockopt"):
            _changed(problems, "TcpTransport._io_writer has no socket to set an option on", SNDBUF)

    # Only the names the patch writes, and only where it went on: on a nats-py
    # that fixed #41 itself, they are no longer the layer's business.
    if Client.__dict__.get("_process_pong") is _nats_patches._process_pong:
        for name, kind in (("_pongs", list), ("_pongs_received", int), ("_pings_outstanding", int)):
            if not isinstance(getattr(client, name, None), kind):
                _changed(problems, f"Client.{name} is gone or not a {kind.__name__}", PONG)

    _fail(problems)


async def test_a_reconnect_connects_the_same_transport_again(layer):
    """The proactor's ``SO_SNDBUF=0`` goes on a reconnect's socket only through
    the transport's own ``connect``, which nats-py calls again on the transport it
    already has. Checked here on every loop by wrapping ``connect`` as the layer does."""
    client = await layer._client()
    transport = getattr(client, "_transport", None)
    if not isinstance(transport, TcpTransport) or not hasattr(transport, "_io_writer"):
        pytest.skip("no TcpTransport with an _io_writer to cut; the test above says what changed")
    connects = 0
    connect = transport.connect

    async def counted(*args, **kwargs):
        nonlocal connects
        connects += 1
        await connect(*args, **kwargs)

    transport.connect = counted  # type: ignore[method-assign]
    reconnects = client.stats["reconnects"]
    transport._io_writer.transport.abort()
    for _ in range(100):
        await asyncio.sleep(0.1)
        if client.stats["reconnects"] > reconnects and client.is_connected:
            break
    assert client.stats["reconnects"] > reconnects, "no reconnect"

    problems: list[str] = []
    if client._transport is not transport:
        _changed(problems, "a reconnect replaced Client._transport instead of connecting it again", SNDBUF)
    elif not connects:
        _changed(problems, "a reconnect did not go through TcpTransport.connect", SNDBUF)
    _fail(problems)


def test_a_missing_internal_turns_its_feature_off_with_one_warning(caplog, monkeypatch):
    """Outside this suite the fallback is all there is, so it says what it turned off."""
    monkeypatch.setattr(layer_module, "_missing_internals", set(), raising=False)
    transport = TcpTransport()
    monkeypatch.delattr(transport, "_io_writer", raising=False)
    client = Client()
    client._transport = transport

    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        assert NatsChannelLayer._cut(client) == 0
        assert NatsChannelLayer._cut(client) == 0

    warned = [
        record.getMessage() for record in caplog.records if "TcpTransport._io_writer is gone" in record.getMessage()
    ]
    assert len(warned) == 1, warned
    assert "a close that has to be cut" in warned[0]
