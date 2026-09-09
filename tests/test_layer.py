import asyncio
import logging
import time

import pytest
from asgiref.sync import async_to_sync
from channels.exceptions import MessageTooLarge

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


async def test_group_members_do_not_share_one_message_object(make_layer):
    """One consumer editing what it received must not change what another one gets."""
    worker = make_layer()
    first, second = await worker.new_channel(), await worker.new_channel()
    await worker.group_add("room", first)
    await worker.group_add("room", second)

    await worker.group_send("room", {"type": "chat", "items": [1]})

    one = await asyncio.wait_for(worker.receive(first), 5)
    two = await asyncio.wait_for(worker.receive(second), 5)
    assert one is not two
    one["items"].append(2)
    assert two["items"] == [1]


async def test_ordering_holds_within_one_subscription(make_layer):
    """What the layer guarantees is order within a single subscription.

    Between subscriptions -- direct against group, or one group against another --
    it does not hold: nats-py gives every subscription its own queue and task, so
    one drains ahead of the next. See the README.
    """
    worker = make_layer()

    direct = await worker.new_channel()
    for i in range(20):
        await worker.send(direct, {"n": i})
    assert [(await asyncio.wait_for(worker.receive(direct), 5))["n"] for _ in range(20)] == list(range(20))

    grouped = await worker.new_channel()
    await worker.group_add("ordered", grouped)
    for i in range(20):
        await worker.group_send("ordered", {"n": i})
    assert [(await asyncio.wait_for(worker.receive(grouped), 5))["n"] for _ in range(20)] == list(range(20))


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


async def test_a_full_mailbox_warns_once_not_per_dropped_message(make_layer, caplog):
    """A full mailbox stays full; one line per drop would bury the logs under load."""
    small = make_layer(capacity=2)
    channel = await small.new_channel()

    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        for i in range(20):  # 2 fit, 18 are dropped
            await small.send(channel, {"type": "n", "i": i})
        await asyncio.sleep(0.3)
        assert len([r for r in caplog.records if "is full" in r.getMessage()]) == 1

        assert (await small.receive(channel))["i"] == 0
        await small.send(channel, {"type": "n", "i": 99})
        await asyncio.sleep(0.2)

    recovered = [r for r in caplog.records if "has room again" in r.getMessage()]
    assert len(recovered) == 1
    assert "18 message(s)" in recovered[0].getMessage()


async def test_a_plain_channel_reaches_one_reader_not_every_process(make_layer):
    """A channel is a queue: a worker pool sharing a name must not run the job N times."""
    workers = [make_layer() for _ in range(3)]
    receiving = [asyncio.create_task(worker.receive("worker-queue")) for worker in workers]
    await asyncio.sleep(0.3)

    await make_layer().send("worker-queue", {"type": "job"})
    await asyncio.sleep(0.4)

    delivered = [task for task in receiving if task.done()]
    assert len(delivered) == 1
    assert delivered[0].result() == {"type": "job"}
    for task in receiving:
        task.cancel()


async def test_two_readers_in_one_process_share_the_channel(make_layer):
    worker = make_layer()
    first = asyncio.create_task(worker.receive("shared"))
    second = asyncio.create_task(worker.receive("shared"))
    await asyncio.sleep(0.2)

    await make_layer().send("shared", {"type": "job"})
    await asyncio.sleep(0.3)

    assert len([task for task in (first, second) if task.done()]) == 1
    first.cancel()
    second.cancel()


async def test_consumers_arriving_together_share_one_subscription(make_layer):
    """Connections do not arrive one at a time; racing callers must not each subscribe."""
    worker = make_layer()
    channels = await asyncio.gather(*(worker.new_channel() for _ in range(8)))
    state = worker._state()

    assert len(state.process_subscriptions) == 1
    assert len(state.client._subs) == 1  # what the server actually has, not just what we track

    await worker.send(channels[0], {"type": "once"})
    await asyncio.sleep(0.3)
    assert state.mailboxes[channels[0]].queue.qsize() == 1


def test_channel_capacity_patterns_are_compiled():
    """get_capacity iterates compiled (pattern, capacity) pairs, not the dict as given."""
    layer = NatsChannelLayer(channel_capacity={"specific.*": 2})

    assert layer.get_capacity("specific.abc!def") == 2
    assert layer.get_capacity("other-channel") == layer.capacity


async def test_expired_messages_do_not_hold_the_capacity(make_layer):
    """A stalled consumer must not end up with a queue full of messages nobody will get."""
    small = make_layer(capacity=1, expiry=0.2)
    channel = await small.new_channel()

    await small.send(channel, {"n": "stale"})
    await asyncio.sleep(0.5)  # the queued message is past its expiry now
    await small.send(channel, {"n": "fresh"})

    assert (await asyncio.wait_for(small.receive(channel), 5))["n"] == "fresh"


async def test_flush_clears_local_state(layer):
    channel = await layer.new_channel()
    await layer.group_add("room", channel)

    await layer.flush()

    state = layer._state()
    assert not state.mailboxes and not state.groups and not state.group_subscriptions


async def test_a_disconnecting_consumer_releases_its_mailbox(make_layer):
    """A consumer leaves its groups and then its pending receive() is cancelled."""
    worker = make_layer()
    state = worker._state()
    for _ in range(5):  # five connect/disconnect cycles must not accumulate anything
        channel = await worker.new_channel()
        await worker.group_add("room", channel)
        receiving = asyncio.create_task(worker.receive(channel))
        await asyncio.sleep(0.05)
        await worker.group_discard("room", channel)  # what websocket_disconnect does
        receiving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await receiving

    assert state.mailboxes == {}
    assert state.groups == {}
    assert state.group_subscriptions == {}


async def test_a_timed_out_read_is_not_a_disconnect(make_layer):
    """Polling with wait_for cancels a read every timeout; groups must survive it."""
    worker = make_layer()
    channel = await worker.new_channel()
    await worker.group_add("room", channel)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(worker.receive(channel), 0.2)

    await worker.group_send("room", {"type": "still a member"})
    assert await asyncio.wait_for(worker.receive(channel), 5) == {"type": "still a member"}


async def test_a_failed_resubscribe_leaves_no_healthy_looking_client(make_layer):
    """An open connection carrying none of our subscriptions would look fine and receive nothing."""
    worker = make_layer()
    await worker.new_channel()
    state = worker._state()

    async def failing(*args, **kwargs):
        raise RuntimeError("resubscribe failed")

    worker._resubscribe = failing
    await state.client.close()

    with pytest.raises(RuntimeError):
        await worker.send("plain", {"type": "x"})
    assert state.client.is_closed  # so the next attempt reconnects instead of going quiet


async def test_a_cancelled_receiver_drops_a_plain_channel_subscription(make_layer):
    worker = make_layer()
    receiving = asyncio.create_task(worker.receive("plain"))
    await asyncio.sleep(0.1)
    subscription = worker._state().mailboxes["plain"].subscription
    assert subscription is not None

    receiving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await receiving

    assert "plain" not in worker._state().mailboxes
    assert subscription._closed


async def test_a_second_receiver_keeps_the_mailbox_alive(make_layer):
    worker = make_layer()
    channel = await worker.new_channel()
    first = asyncio.create_task(worker.receive(channel))
    second = asyncio.create_task(worker.receive(channel))
    await asyncio.sleep(0.05)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    await worker.send(channel, {"type": "still here"})
    assert await asyncio.wait_for(second, 5) == {"type": "still here"}


async def test_subscriptions_are_restored_after_the_client_is_closed(make_layer):
    """nats-py closes the client once it has spent max_reconnect_attempts."""
    worker = make_layer()
    channel = await worker.new_channel()
    await worker.group_add("room", channel)
    plain = asyncio.create_task(worker.receive("plain-channel"))
    await asyncio.sleep(0.1)

    await worker._state().client.close()
    assert worker._state().client.is_closed

    await worker.send(channel, {"type": "direct"})
    assert await asyncio.wait_for(worker.receive(channel), 5) == {"type": "direct"}
    await worker.group_send("room", {"type": "group"})
    assert await asyncio.wait_for(worker.receive(channel), 5) == {"type": "group"}
    await worker.send("plain-channel", {"type": "plain"})
    assert await asyncio.wait_for(plain, 5) == {"type": "plain"}


def test_state_of_a_closed_loop_is_forgotten():
    """A layer used from short-lived loops must not accumulate one state each."""
    layer = NatsChannelLayer()

    async def touch() -> None:
        layer._state()

    for _ in range(4):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(touch())
        finally:
            loop.close()

    assert len(layer._states) == 1  # only the newest; its own successor will sweep it


async def test_flush_survives_a_connection_that_is_already_gone(make_layer):
    """Shutting down while NATS is unreachable must not turn into an error."""
    worker = make_layer()
    channel = await worker.new_channel()
    await worker.group_add("room", channel)
    receiving = asyncio.create_task(worker.receive("plain"))
    await asyncio.sleep(0.1)

    await worker._state().client.close()
    await worker.flush()

    state = worker._state()
    assert not state.mailboxes and not state.groups and not state.group_subscriptions
    assert not state.process_subscriptions
    receiving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await receiving


def test_close_also_forgets_other_closed_loops():
    layer = NatsChannelLayer()

    async def touch() -> None:
        layer._state()

    for _ in range(3):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(touch())
        finally:
            loop.close()
    assert len(layer._states) == 1  # the newest is still there, its loop now closed

    asyncio.run(layer.close())

    assert len(layer._states) == 0


def test_loops_abandoned_without_close_are_reported_once(caplog):
    """Nothing can tell an abandoned loop from a live one, so say so rather than grow quietly."""
    layer = NatsChannelLayer()
    layer.loop_state_warn_at = 3
    loops = []

    async def touch() -> None:
        layer._state()

    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        for _ in range(6):
            loop = asyncio.new_event_loop()
            loops.append(loop)  # kept alive and never closed, which is the whole point
            loop.run_until_complete(touch())

    assert len([r for r in caplog.records if "event loops have used this layer" in r.getMessage()]) == 1
    assert len(layer._states) == 6
    for loop in loops:
        loop.close()


async def test_recovery_does_not_bring_back_a_group_discarded_while_it_ran(make_layer):
    """Rebuilding subscriptions takes many round trips, and the process keeps running."""
    worker = make_layer()
    channel = await worker.new_channel()
    await worker.group_add("room", channel)
    state = worker._state()
    client = state.client
    assert client is not None

    entered, gate = asyncio.Event(), asyncio.Event()
    real_subscribe = client.subscribe

    async def slow_subscribe(subject, **kwargs):
        subscription = await real_subscribe(subject, **kwargs)
        if subject == worker.group_subject("room"):
            entered.set()  # hold the recovery with the group already snapshotted
            await gate.wait()
        return subscription

    client.subscribe = slow_subscribe
    recovering = asyncio.create_task(worker._resubscribe(state, client))
    await asyncio.wait_for(entered.wait(), 5)

    await worker.group_discard("room", channel)
    gate.set()
    await asyncio.wait_for(recovering, 5)

    assert "room" not in state.group_subscriptions
    await worker.group_send("room", {"type": "gone"})
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(worker.receive(channel), 1)


async def test_a_receive_only_process_comes_back_after_the_connection_closes(make_layer):
    """Nothing calls _client() on the receive path, so the layer has to notice by itself."""
    worker = make_layer()
    channel = await worker.new_channel()
    publisher = make_layer()

    await worker._state().client.close()

    receiving = asyncio.create_task(worker.receive(channel))
    deadline = time.monotonic() + 20
    while True:  # nothing is stored, so keep publishing until the worker is back
        await publisher.send(channel, {"type": "after"})
        try:
            assert await asyncio.wait_for(asyncio.shield(receiving), 0.5) == {"type": "after"}
            return
        except asyncio.TimeoutError:
            if time.monotonic() > deadline:
                receiving.cancel()
                pytest.fail("the receive-only worker never came back")


async def test_a_failing_closed_cb_does_not_take_the_recovery_with_it(make_layer):
    """The layer's safety net is what a receive-only process depends on."""

    failures = []

    async def closed_cb() -> None:
        failures.append(1)
        if len(failures) == 1:  # only the unexpected close; teardown closes on purpose
            raise RuntimeError("the application's own callback failed")

    worker = make_layer(connect_options={"closed_cb": closed_cb})
    channel = await worker.new_channel()
    publisher = make_layer()

    try:
        await worker._state().client.close()
    except RuntimeError:
        pass  # the user's callback is allowed to fail; the layer is not

    receiving = asyncio.create_task(worker.receive(channel))
    deadline = time.monotonic() + 20
    while True:
        await publisher.send(channel, {"type": "after"})
        try:
            assert await asyncio.wait_for(asyncio.shield(receiving), 0.5) == {"type": "after"}
            return
        except asyncio.TimeoutError:
            if time.monotonic() > deadline:
                receiving.cancel()
                pytest.fail("a failing closed_cb stopped the layer from recovering")


async def test_a_group_message_to_a_shared_channel_reaches_one_reader(make_layer):
    """A channel is a queue on the group path too, not only on the direct one."""
    workers = [make_layer(), make_layer()]
    reading = [asyncio.create_task(worker.receive("shared-worker")) for worker in workers]
    await asyncio.sleep(0.2)
    for worker in workers:
        await worker.group_add("jobs", "shared-worker")
    await asyncio.sleep(0.2)

    await make_layer().group_send("jobs", {"type": "job"})
    await asyncio.sleep(0.4)

    assert len([task for task in reading if task.done()]) == 1
    for task in reading:
        task.cancel()


async def test_an_oversized_message_is_refused_without_killing_the_connection(make_layer):
    """The server counts headers against max_payload; nats-py does not, and answers by hanging up."""
    worker = make_layer()
    channel = await worker.new_channel()
    client = worker._state().client

    # A body that fits on its own, but not once the Channel header is added.
    body = b"x" * (client.max_payload - 60)
    with pytest.raises(MessageTooLarge):
        await worker.send(channel, {"type": "big", "body": body})

    assert worker.MessageTooLarge is MessageTooLarge
    assert not client.is_closed
    await worker.send(channel, {"type": "small"})
    assert await asyncio.wait_for(worker.receive(channel), 5) == {"type": "small"}


async def test_invalid_names_are_rejected(layer):
    with pytest.raises(TypeError):
        await layer.group_send("bad*name", {"type": "x"})
    with pytest.raises(TypeError):
        await layer.send("has space", {"type": "x"})
    with pytest.raises(AssertionError):
        await layer.send("ok", {"__asgi_channel__": "x"})


async def test_messages_carry_every_type_the_spec_allows(make_layer):
    """Byte strings are in the spec's list, and JSON cannot represent them."""
    worker = make_layer()
    channel = await worker.new_channel()
    message = {
        "type": "spec.types",
        "bytes": b"\x00\x01",
        "text": "안녕",
        "int": 2**62,
        "float": 1.5,
        "list": [1, "two", None],
        "dict": {"nested": True},
        "none": None,
    }

    await worker.send(channel, message)

    assert await asyncio.wait_for(worker.receive(channel), 5) == message


def test_subjects_are_the_contract():
    layer = NatsChannelLayer(prefix="app")
    assert layer.channel_subject("test-channel") == "app.ch.test-channel"
    assert layer.channel_subject("specific.ab!cd") == "app.pc.specific.ab"
    assert layer.group_subject("room-1") == "app.grp.room-1"


async def test_a_mailbox_is_not_handed_out_before_it_is_subscribed(make_layer):
    """A caller that gets a mailbox has to be able to trust that it receives.

    ``_mailbox`` registers the mailbox before it subscribes, so a caller arriving
    in between used to get one that nothing feeds yet.
    """
    worker = make_layer()
    first = asyncio.create_task(worker._mailbox("shared-channel"))
    await asyncio.sleep(0)  # let it register the mailbox and start subscribing

    second = await asyncio.wait_for(worker._mailbox("shared-channel"), 5)

    assert second.subscription is not None
    assert second is await first


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


async def test_receiving_on_another_process_channel_is_refused(make_layer):
    """Subscribing to a foreign process subject would deliver its whole traffic twice."""
    owner, other = make_layer(), make_layer()
    channel = await owner.new_channel()

    with pytest.raises(ValueError):
        await other.receive(channel)
    with pytest.raises(ValueError):
        await other.group_add("room", channel)

    await other.send(channel, {"type": "direct"})
    assert await asyncio.wait_for(owner.receive(channel), 5) == {"type": "direct"}
    assert channel not in other._state().mailboxes


async def test_receiving_on_a_process_channel_of_another_loop_is_refused(make_layer):
    """A channel belongs to the loop that made it: two loops holding one would each get a copy.

    Both loops subscribe to the same process subject, and each delivers to its own
    mailbox, so one send() arrives twice -- at-most-once broken.
    """
    worker = make_layer()
    channel = await worker.new_channel()

    def another_loop() -> None:
        async def main() -> None:
            with pytest.raises(ValueError):
                await asyncio.wait_for(worker.receive(channel), 2)
            with pytest.raises(ValueError):
                await asyncio.wait_for(worker.group_add("room", channel), 2)

        asyncio.run(main())

    await asyncio.to_thread(another_loop)

    await worker.send(channel, {"type": "once"})
    assert await asyncio.wait_for(worker.receive(channel), 5) == {"type": "once"}


def test_process_subject_is_derived_from_the_channel_prefix():
    layer = NatsChannelLayer(prefix="app")
    assert layer.channel_subject("specific.abc123!deadbeef") == "app.pc.specific.abc123"
    assert layer.process_subject("specific.abc123!deadbeef") == "app.pc.specific.abc123"
