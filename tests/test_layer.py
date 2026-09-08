import asyncio

import pytest
from asgiref.sync import async_to_sync

from channels_nats import NatsChannelLayer

pytestmark = pytest.mark.integration


async def test_send_and_receive_on_a_process_channel(layer):
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "test.message", "text": "안녕"})
    assert await asyncio.wait_for(layer.receive(channel), 5) == {"type": "test.message", "text": "안녕"}


async def test_new_channel_names_are_valid_and_unique(layer):
    a, b = await layer.new_channel(), await layer.new_channel()
    assert a != b
    assert a.startswith("specific.") and "!" in a
    layer.require_valid_channel_name(a)


async def test_named_channel_receives_after_subscribing(layer):
    waiting = asyncio.create_task(layer.receive("test-channel"))
    await asyncio.sleep(0.1)  # let the receiver subscribe first
    await layer.send("test-channel", {"type": "hello"})
    assert await asyncio.wait_for(waiting, 5) == {"type": "hello"}


async def test_group_send_reaches_every_member_in_every_process(make_layer):
    worker_a, worker_b, publisher = make_layer(), make_layer(), make_layer()
    a1, a2 = await worker_a.new_channel(), await worker_a.new_channel()
    b1 = await worker_b.new_channel()
    for worker, channel in ((worker_a, a1), (worker_a, a2), (worker_b, b1)):
        await worker.group_add("room", channel)

    await publisher.group_send("room", {"type": "chat", "n": 1})

    for worker, channel in ((worker_a, a1), (worker_a, a2), (worker_b, b1)):
        assert await asyncio.wait_for(worker.receive(channel), 5) == {"type": "chat", "n": 1}


async def test_group_discard_stops_delivery(layer):
    channel = await layer.new_channel()
    await layer.group_add("room", channel)
    await layer.group_discard("room", channel)

    await layer.group_send("room", {"type": "chat"})

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(layer.receive(channel), 0.3)


async def test_group_send_from_a_sync_context_uses_its_own_connection(layer):
    """Django signals and views call the layer through async_to_sync on another loop."""
    channel = await layer.new_channel()
    await layer.group_add("signals", channel)

    await asyncio.to_thread(lambda: async_to_sync(layer.group_send)("signals", {"type": "mutation"}))

    assert await asyncio.wait_for(layer.receive(channel), 5) == {"type": "mutation"}


async def test_expired_messages_are_skipped(make_layer):
    quick = make_layer(expiry=0.05)
    channel = await quick.new_channel()
    await quick.send(channel, {"type": "stale"})
    await asyncio.sleep(0.2)
    await quick.send(channel, {"type": "fresh"})

    assert await asyncio.wait_for(quick.receive(channel), 5) == {"type": "fresh"}


async def test_full_mailbox_drops_new_messages(make_layer):
    small = make_layer(capacity=2)
    channel = await small.new_channel()
    for i in range(5):
        await small.send(channel, {"type": "n", "i": i})
    await asyncio.sleep(0.2)

    assert (await small.receive(channel))["i"] == 0
    assert (await small.receive(channel))["i"] == 1
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(small.receive(channel), 0.3)


async def test_flush_clears_local_state(layer):
    channel = await layer.new_channel()
    await layer.group_add("room", channel)

    await layer.flush()

    state = layer._state()
    assert not state.mailboxes and not state.groups and not state.group_subscriptions


async def test_invalid_names_are_rejected(layer):
    with pytest.raises(TypeError):
        await layer.group_send("bad*name", {"type": "x"})
    with pytest.raises(TypeError):
        await layer.send("has space", {"type": "x"})
    with pytest.raises(AssertionError):
        await layer.send("ok", {"__asgi_channel__": "x"})


async def test_msgpack_serializer_keeps_bytes(make_layer):
    packed = make_layer(serializer="msgpack")
    channel = await packed.new_channel()
    await packed.send(channel, {"type": "binary", "data": b"\x00\x01"})
    assert await asyncio.wait_for(packed.receive(channel), 5) == {"type": "binary", "data": b"\x00\x01"}


def test_subjects_are_the_contract():
    layer = NatsChannelLayer(prefix="app")
    assert layer.channel_subject("test-channel") == "app.ch.test-channel"
    assert layer.channel_subject("specific.ab!cd") == "app.pc.specific.ab"
    assert layer.group_subject("room-1") == "app.grp.room-1"


async def test_process_channels_share_one_subscription(layer):
    channels = [await layer.new_channel() for _ in range(20)]

    state = layer._state()
    assert len(state.process_subscriptions) == 1
    assert all(state.mailboxes[c].subscription is None for c in channels)
    for channel in channels:
        await layer.send(channel, {"type": "n", "to": channel})
    for channel in channels:
        assert (await asyncio.wait_for(layer.receive(channel), 5))["to"] == channel


async def test_send_to_another_process_channel(make_layer):
    """A message for ``specific.<other>!<id>`` is routed to the owning process by the Channel header."""
    owner, sender = make_layer(), make_layer()
    channel = await owner.new_channel()

    await sender.send(channel, {"type": "direct", "n": 3})

    assert await asyncio.wait_for(owner.receive(channel), 5) == {"type": "direct", "n": 3}


def test_process_subject_is_derived_from_the_channel_prefix():
    layer = NatsChannelLayer(prefix="app")
    assert layer.channel_subject("specific.abc123!deadbeef") == "app.pc.specific.abc123"
    assert layer.process_subject("specific.abc123!deadbeef") == "app.pc.specific.abc123"
