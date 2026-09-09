import asyncio
import contextlib
import gc
import logging
import time

import pytest
from asgiref.sync import async_to_sync
from channels.exceptions import MessageTooLarge
from nats.errors import FlushTimeoutError

from channels_nats import ChannelLayerClosed, NatsChannelLayer
from channels_nats.layer import _Mailbox

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


async def test_a_cancelled_group_add_leaves_no_membership_behind(make_layer):
    """A membership with no subscription receives nothing and keeps the mailbox alive.

    group_add records the member before it has the group's subscription, so a
    cancellation in between used to leave one that nothing feeds.
    """
    worker = make_layer()
    worker.mailbox_grace = 0.1
    channel = await worker.new_channel()  # its mailbox exists, so group_add gets to the lock
    state = worker._state()

    await state.subscribe_lock.acquire()
    adding = asyncio.create_task(worker.group_add("room", channel))
    await asyncio.sleep(0)  # let it record the membership and block on the lock
    adding.cancel()
    state.subscribe_lock.release()
    with pytest.raises(asyncio.CancelledError):
        await adding

    assert channel not in state.groups.get("room", set())

    receiving = asyncio.create_task(worker.receive(channel))
    await asyncio.sleep(0.1)
    receiving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await receiving
    await asyncio.sleep(0.15)
    await worker.new_channel()  # sweeps
    assert channel not in state.mailboxes  # a phantom membership would have held it


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
    worker.mailbox_grace = 0.1
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

    assert state.groups == {}
    assert state.group_subscriptions == {}
    await asyncio.sleep(0.15)  # the mailboxes go once their grace period is out
    fresh = await worker.new_channel()  # any mailbox lookup sweeps
    assert list(state.mailboxes) == [fresh]


async def test_polling_a_process_channel_keeps_what_arrives_between_polls(make_layer):
    """A cancelled read is not a disconnect, and a channel in no group is no exception."""
    worker, publisher = make_layer(), make_layer()
    channel = await worker.new_channel()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(worker.receive(channel), 0.2)

    await publisher.send(channel, {"type": "between polls"})
    await asyncio.sleep(0.2)  # let it arrive while nothing is reading; a dropped mailbox loses it here

    assert await asyncio.wait_for(worker.receive(channel), 5) == {"type": "between polls"}


async def test_a_consumer_that_ends_on_a_message_releases_its_mailbox(make_layer):
    """Channels cancels the receive task it holds, but that task has already completed.

    A consumer raising StopConsumer from a channel-layer message therefore sends no
    cancellation the layer can see, and nothing here can wait for one.
    """
    worker = make_layer()
    worker.mailbox_grace = 0.1
    channel = await worker.new_channel()
    state = worker._state()

    await worker.send(channel, {"type": "the last one"})
    receiving = asyncio.create_task(worker.receive(channel))
    assert await asyncio.wait_for(receiving, 5) == {"type": "the last one"}
    receiving.cancel()  # await_many_dispatch's finally, on a task that is already done
    await asyncio.sleep(0.15)

    await worker.new_channel()  # any mailbox lookup sweeps
    assert channel not in state.mailboxes


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

    # The reader flush woke re-establishes its own channel rather than waiting on a
    # dead queue (#21), and now does so while flush is still tidying up, so let it
    # finish before reading the state: what is asserted is what flush is responsible
    # for, not a race with a reader doing what it is supposed to.
    receiving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await receiving

    state = worker._state()
    assert not state.mailboxes and not state.groups and not state.group_subscriptions
    assert not state.process_subscriptions


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


async def test_a_cancelled_subscribe_does_not_steal_from_the_next_one(layer, make_layer):
    """A subscription left behind by a cancelled subscribe keeps taking messages.

    ``Client.subscribe()`` registers the subscription and starts its callback task
    before it awaits the wire, so a cancellation inside it never hands the caller
    a Subscription to take down. A plain channel is read under a queue group, so
    that leftover stays a member of it: the server keeps giving it messages, its
    callback files them in a mailbox nobody can reach any more, and the reader
    that retried sees only the rest.

    The cancel point is injected -- whether normal use reaches it depends on
    nats-py internals -- and reaching into ``_send_subscribe`` ties this test to
    them. What it pins is the layer's side: cancellation must not leave a live
    subscription behind.
    """
    client = await layer._client()
    subscribed, resume = asyncio.Event(), asyncio.Event()
    send_subscribe = client._send_subscribe

    async def gated(*args, **kwargs):
        result = await send_subscribe(*args, **kwargs)
        await client.flush()  # cancel only once the server has the subscription
        subscribed.set()
        await resume.wait()
        return result

    client._send_subscribe = gated
    try:
        subscribing = asyncio.create_task(layer._mailbox("shared"))
        await asyncio.wait_for(subscribed.wait(), 5)
        subscribing.cancel()
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await subscribing
    finally:
        client._send_subscribe = send_subscribe

    await layer._mailbox("shared")  # the retry, which subscribes again -- and only now is there a reader
    sender = make_layer()
    for index in range(20):
        await sender.send("shared", {"type": "chat", "index": index})
    delivered = [(await asyncio.wait_for(layer.receive("shared"), 5))["index"] for _ in range(20)]
    assert delivered == list(range(20))


async def test_a_cancelled_flush_leaves_no_unconfirmed_subscription(layer):
    """The process subscription is established only once the server has it.

    ``_process_subscription`` used to record the subscription before flushing.
    A cancellation in that flush deleted the mailbox but kept the record, so the
    next caller took the "already subscribed" early return and nothing ever
    confirmed the SUB. Whether the server had already received it decides whether
    messages are lost, which is what made that gap conditional; what is not
    conditional is that the attempt must not leave a subscription on the
    connection that no caller holds.
    """
    client = await layer._client()
    flushing, resume = asyncio.Event(), asyncio.Event()
    flush = client.flush

    async def gated(*args, **kwargs):
        flushing.set()
        await resume.wait()
        return await flush(*args, **kwargs)

    client.flush = gated
    try:
        opening = asyncio.create_task(layer.new_channel())
        await asyncio.wait_for(flushing.wait(), 5)
        opening.cancel()
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await opening
    finally:
        client.flush = flush

    process_subject = f"{layer.prefix}.pc.specific.{layer._state().client_id}"
    assert [s for s in client._subs.values() if s.subject == process_subject] == []
    assert layer._state().process_subscriptions == {}

    # And the retry really does establish it: a message from elsewhere arrives.
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "after"})
    assert await asyncio.wait_for(layer.receive(channel), 5) == {"type": "after"}


async def test_flush_does_not_strand_a_waiting_receiver(layer):
    """``flush()`` drops the mailboxes, including one a receiver is waiting on.

    That queue is never fed again -- the subscription is gone and the next message
    goes to the mailbox that replaces it -- so the receiver would wait for as long
    as nobody cancels it. Reproduced against a real server before the fix: the
    read stayed pending while the new mailbox held the message.
    """
    channel = await layer.new_channel()
    waiting = asyncio.create_task(layer.receive(channel))
    await asyncio.sleep(0.1)  # let it subscribe and settle on the queue

    await layer.flush()
    await layer._mailbox(channel)  # the channel is established again, however it happens
    await layer.send(channel, {"type": "after"})

    assert await asyncio.wait_for(waiting, 5) == {"type": "after"}
    assert layer._state().mailboxes[channel].receivers == 0


async def test_flush_wakes_every_reader_of_a_shared_channel(layer):
    readers = [asyncio.create_task(layer.receive("shared")) for _ in range(2)]
    await asyncio.sleep(0.1)

    await layer.flush()
    await layer._mailbox("shared")
    for index in range(2):
        await layer.send("shared", {"type": "chat", "index": index})

    delivered = [(await asyncio.wait_for(reader, 5))["index"] for reader in readers]
    assert sorted(delivered) == [0, 1]


async def test_drops_before_the_mailbox_are_reported(make_layer, caplog):
    """nats-py throws messages away a layer above the mailbox, and used to do it silently.

    A subscription over its pending limits drops on arrival: the message never
    reaches a mailbox, so no mailbox's ``dropped`` counts it and the operator
    reads "nothing was dropped" while messages are missing. The limit is lowered
    here rather than filling the default one (512K messages / 128MB), but the
    drop itself is nats-py's own path, not a synthesised error.
    """
    seen: list[Exception] = []

    async def error_cb(error: Exception) -> None:
        seen.append(error)

    reader = make_layer(connect_options={"error_cb": error_cb})
    box = await reader._mailbox("shared")
    assert box.subscription is not None
    box.subscription._pending_bytes_limit = 1  # one message is already too much

    sender = make_layer()
    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        for index in range(3):
            await sender.send("shared", {"type": "chat", "index": index})
        await asyncio.sleep(0.3)

    assert box.queue.qsize() == 0  # nothing arrived
    assert box.dropped == 0  # and the mailbox has no idea

    warned = [r for r in caplog.records if "slow consumer" in r.getMessage()]
    assert len(warned) == 1  # first drop warns, the rest are rate-limited like a full mailbox
    assert reader.channel_subject("shared") in warned[0].getMessage()
    assert reader._state().slow_consumer_drops == 3
    assert len(seen) == 3  # the caller's own error_cb still sees every one


async def test_errors_still_reach_the_nats_logger_without_an_error_cb(make_layer, caplog):
    """Installing an error_cb of our own takes nats-py's default out of the way.

    A guard, not a defect: whoever reads the nats logger for connection errors
    must keep getting them.
    """
    reader = make_layer()
    box = await reader._mailbox("shared")
    assert box.subscription is not None
    box.subscription._pending_bytes_limit = 1

    sender = make_layer()
    with caplog.at_level(logging.ERROR, logger="nats.aio.client"):
        await sender.send("shared", {"type": "chat"})
        await asyncio.sleep(0.3)

    assert [record for record in caplog.records if record.name == "nats.aio.client"]


async def test_receiving_on_the_process_prefix_gets_that_process_channels(make_layer):
    """The spec models ``<name>.<id>!<random>`` with the part before ``!`` naming
    the process, so a read on the prefix is a read on that process's channels.

    Ordinary consumers pass the full name ``new_channel()`` gave them, which is
    why this went unnoticed; a prefix read used to sit on a mailbox nothing ever
    routed to.
    """
    owner = make_layer()
    prefix = f"specific.{owner._state().client_id}!"
    waiting = asyncio.create_task(owner.receive(prefix))
    await asyncio.sleep(0.1)

    sender = make_layer()
    await sender.send(prefix + "aa11", {"type": "direct"})
    assert await asyncio.wait_for(waiting, 5) == {"type": "direct"}


async def test_a_channel_with_its_own_mailbox_is_not_taken_by_the_prefix(make_layer):
    owner = make_layer()
    channel = await owner.new_channel()
    prefix = channel[: channel.index("!") + 1]
    on_prefix = asyncio.create_task(owner.receive(prefix))
    await asyncio.sleep(0.1)

    sender = make_layer()
    await sender.send(channel, {"type": "for the channel"})
    assert await asyncio.wait_for(owner.receive(channel), 5) == {"type": "for the channel"}
    assert not on_prefix.done()
    on_prefix.cancel()


async def test_a_frame_that_is_not_a_message_does_not_end_the_consumer(layer, caplog):
    """Subjects are the contract, and the README invites other publishers to use
    them. A frame that is not a msgpack map is the sender breaking that contract;
    raising out of receive() would end a consumer that did nothing wrong.
    """
    await layer._mailbox("shared")
    client = await layer._client()
    subject = layer.channel_subject("shared")

    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        await client.publish(subject, b"\x91\x01")  # msgpack, but an array
        await client.publish(subject, b"\xc1")  # not msgpack at all
        await asyncio.sleep(0.2)
        await layer.send("shared", {"type": "good"})
        assert await asyncio.wait_for(layer.receive("shared"), 5) == {"type": "good"}

    warned = [r for r in caplog.records if "could not be read as a Channels message" in r.getMessage()]
    assert len(warned) == 1  # rate-limited like the other drops
    assert layer._state().bad_frames == 2


async def test_expiry_counts_time_in_the_mailbox_not_the_age_of_the_message(make_layer):
    """The clock starts when the callback files the message, so a message can be
    older than ``expiry`` and still be handed over.

    Publish time would mean trusting the publisher's clock: with processes on
    different clocks the same message would expire at different times in each.
    That trade is deliberate, and this pins which side of it the layer is on.
    """
    reader = make_layer(expiry=1)
    channel = await reader.new_channel()
    sender = make_layer()
    await sender.send(channel, {"type": "old"})

    time.sleep(1.5)  # blocks the loop: the callback cannot file it while we wait

    assert await asyncio.wait_for(reader.receive(channel), 5) == {"type": "old"}


async def test_close_ends_a_waiting_receiver(layer):
    """``close()`` drops this loop's state, so nothing feeds the mailbox again.

    A ``receive()`` already waiting on it would stay pending until somebody
    cancels it (reproduced against a real server before the fix). ``flush()``'s
    remedy -- move to the mailbox that replaces this one -- cannot be used here:
    it would reopen the connection the caller has just closed.
    """
    channel = await layer.new_channel()
    waiting = asyncio.create_task(layer.receive(channel))
    await asyncio.sleep(0.1)  # let it subscribe and settle on the queue

    await layer.close()

    with pytest.raises(ChannelLayerClosed):
        await asyncio.wait_for(waiting, 5)


async def test_close_ends_every_reader_of_a_shared_channel(layer):
    readers = [asyncio.create_task(layer.receive("shared")) for _ in range(2)]
    await asyncio.sleep(0.1)

    await layer.close()

    for reader in readers:
        with pytest.raises(ChannelLayerClosed):
            await asyncio.wait_for(reader, 5)


async def test_a_cancelled_read_does_not_revive_a_closed_loop(layer):
    """``close()`` drops this loop's state; a cancelled read must not put it back.

    ``receive()`` releases a plain channel's mailbox on a cancelled read, and that
    used to ask for the loop's state -- which creates one when it is gone. The
    revived entry holds no connection and no subscription, but it counts towards
    ``loop_state_warn_at``, whose warning is about loops that still hold one.
    """
    waiting = asyncio.create_task(layer.receive("plain-name"))
    await asyncio.sleep(0.1)  # let it subscribe and settle on the queue

    closing = asyncio.create_task(layer.close())
    await asyncio.sleep(0)  # close() has popped the state and woken the waiter
    waiting.cancel()  # and the cancel arrives before the wake-up gets to run
    await closing
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert layer._states == {}


async def test_a_cancellation_the_client_swallows_still_reaches_the_caller(layer):
    """nats-py discards a cancellation delivered while it waits to flush.

    ``Client._flush_pending()`` ends with ``except asyncio.CancelledError: pass``,
    so a cancel that lands while a publish waits for the flusher is thrown away and
    ``publish()`` returns as if nothing happened -- a consumer cancelled at shutdown
    goes back to its loop and never ends (reproduced against a real server, #23).
    It never calls ``uncancel()``, which is what leaves the request countable.

    This test does not prove nats-py swallows anything; it stands in for that with a
    publish that swallows the same way, and pins what the layer does about it.
    """
    client = await layer._client()
    original, parked = client.publish, asyncio.Event()

    async def swallowing_publish(*args, **kwargs):
        parked.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass  # exactly what _flush_pending does, uncancel() included: it does not

    client.publish = swallowing_publish
    try:
        sending = asyncio.create_task(layer.group_send("room", {"type": "test.message"}))
        await asyncio.wait_for(parked.wait(), 5)
        assert sending.cancel(), "the send should still have been running"

        with pytest.raises(asyncio.CancelledError):
            await sending
    finally:
        client.publish = original


async def test_a_cancelled_subscribe_does_not_wait_out_a_jammed_connection(layer):
    """The cleanup for a cancelled subscribe must not block on the connection.

    ``_subscribe()`` runs ``Client.subscribe()`` as a task so that a cancellation
    cannot lose the handle (#11), and waits for it on the way out so the leftover
    can be taken down. Under send backpressure that wait has no end: the cancelled
    caller stays alive until the connection drains, which is the shutdown hang of
    #23 by another road (reproduced against a real server with a stopped server).

    The subscribe here is parked by hand rather than by backpressure -- deterministic,
    and it runs on Windows, where there is no SIGSTOP.
    """
    client = await layer._client()
    original, started, release, created = client.subscribe, asyncio.Event(), asyncio.Event(), []

    async def parked_subscribe(*args, **kwargs):
        started.set()
        await release.wait()
        created.append(await original(*args, **kwargs))
        return created[-1]

    client.subscribe = parked_subscribe
    try:
        opening = asyncio.create_task(layer._mailbox("jammed-channel"))
        await asyncio.wait_for(started.wait(), 5)
        opening.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(opening, 5)  # not "once the connection frees up"
    finally:
        release.set()
        client.subscribe = original

    # The handle still has to be taken down; that is what the waiting was for.
    for _ in range(50):
        await asyncio.sleep(0.1)
        if created and created[0]._closed:
            break
    assert created and created[0]._closed, "the leftover subscription was never unsubscribed"


async def test_a_cancelled_read_does_not_wait_out_a_jammed_unsubscribe(layer):
    """Taking a subscription down blocks on a jammed connection too.

    ``Subscription.unsubscribe()`` sends a command, so under send backpressure it
    waits in the same place ``Client.subscribe()`` does -- measured against a real
    server, still parked after eight seconds while the connection stayed saturated.
    Every cleanup that unsubscribes after a cancellation inherits that wait, which
    is what #26 fixed one level up.
    """
    box = await layer._mailbox("plain-with-a-jammed-unsubscribe")
    release = asyncio.Event()
    original = box.subscription.unsubscribe

    async def parked_unsubscribe(*args, **kwargs):
        await release.wait()
        return await original(*args, **kwargs)

    box.subscription.unsubscribe = parked_unsubscribe
    try:
        reading = asyncio.create_task(layer.receive("plain-with-a-jammed-unsubscribe"))
        await asyncio.sleep(0.1)
        reading.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(reading, 5)  # not "once the connection frees up"
    finally:
        release.set()


async def test_close_survives_a_connection_that_cannot_be_drained(layer, caplog):
    """Draining flushes what is pending, so a jammed connection cannot be drained.

    nats-py gives up after its own ``drain_timeout``/``flush_timeout`` and raises
    ``FlushTimeoutError`` -- measured against a real server whose reader was stopped:
    ``close()`` came back after 24 seconds with that exception, and after 12 with both
    timeouts lowered. Tearing down must not become an error for the caller, so the
    connection is closed the hard way instead.
    """
    client = await layer._client()

    async def failing_drain():
        raise FlushTimeoutError

    client.drain = failing_drain
    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        await layer.close()  # must not raise

    assert client.is_closed, "the connection has to go even when it cannot be drained"
    assert any("could not drain" in record.message for record in caplog.records)


async def test_handing_a_command_off_does_not_revive_a_closed_loop(layer):
    """The cleanup handoff must not do what #24 was about.

    ``_finish_later()`` keeps the task on the loop's state so nothing is left
    pending at loop shutdown -- but reaching for that state through ``_state()``
    creates one when ``close()`` has just taken it away, and the revived entry
    counts towards ``loop_state_warn_at`` while holding nothing.
    """
    client = await layer._client()
    original, started, release = client.subscribe, asyncio.Event(), asyncio.Event()

    async def parked_subscribe(*args, **kwargs):
        started.set()
        await release.wait()
        return await original(*args, **kwargs)

    client.subscribe = parked_subscribe
    try:
        opening = asyncio.create_task(layer._mailbox("channel-being-subscribed"))
        await asyncio.wait_for(started.wait(), 5)

        await layer.close()  # the state goes
        opening.cancel()  # and the cleanup, which cannot finish, is handed off
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(opening, 5)

        assert layer._states == {}
    finally:
        release.set()
        client.subscribe = original


async def test_close_finishes_the_commands_it_cancelled(layer):
    """A cancelled task is not a finished one until the loop has run it.

    ``close()`` cancels the commands it handed to the background; if it returns
    before they have taken the cancellation, a loop that closes right after prints
    "Task was destroyed but it is pending!" for each one.
    """
    box = await layer._mailbox("plain-with-a-parked-unsubscribe")
    release = asyncio.Event()
    original = box.subscription.unsubscribe

    async def parked_unsubscribe(*args, **kwargs):
        await release.wait()
        return await original(*args, **kwargs)

    box.subscription.unsubscribe = parked_unsubscribe
    try:
        state = layer._state()
        await layer._unsubscribe([box.subscription])  # gives up waiting, hands it off
        handed_off = set(state.cleanups)
        assert handed_off, "the parked unsubscribe should have been handed off"

        await layer.close()

        assert all(task.done() for task in handed_off), "close() left a task mid-cancellation"
    finally:
        release.set()


async def test_close_settles_the_recovery_it_cancels(layer):
    """Recovery runs exactly when there is no connection left to drain.

    ``close()`` cancels it and then awaits the drain, which is what gives the loop
    the turn a cancelled task needs to finish -- but a closed client is not drained,
    so there is nothing to await and ``close()`` would return with the task still
    mid-cancellation. Under ``asyncio.run`` the runner cancels it later anyway;
    under a loop that simply stops, such as Twisted's, it is reported as destroyed
    while pending.
    """
    client = await layer._client()
    state = layer._state()
    await client.close()  # the state recovery exists for
    state.recovery = asyncio.create_task(asyncio.sleep(3600))
    await asyncio.sleep(0)

    await layer.close()

    assert state.recovery.done(), "close() returned with recovery still mid-cancellation"


async def test_connects_that_time_out_do_not_pile_up_sockets():
    """A connect that fails has to release the socket it opened.

    ``nats.connect()`` builds the client inside, so a failed call hands back nothing
    and the socket waited for the garbage collector -- measured against a server that
    accepts and stays silent: five timed-out connects, five sockets still held. The
    recovery loop retries for as long as the layer is up, so those add up. The layer
    builds the client itself now and closes it when the connect does not come up.

    The server here is a plain asyncio one; no nats-server is involved.
    """
    peers: dict = {}

    async def silent(reader, writer):
        peer = writer.get_extra_info("peername")
        peers[peer] = writer
        await reader.read(1)  # never send INFO, so the handshake times out
        peers.pop(peer, None)
        writer.close()

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    layer = NatsChannelLayer(
        servers=f"nats://127.0.0.1:{port}",
        connect_options={"connect_timeout": 1, "allow_reconnect": False, "max_reconnect_attempts": 0},
    )
    try:
        for _ in range(3):
            with pytest.raises(Exception):
                await layer.new_channel()
        for _ in range(30):
            await asyncio.sleep(0.1)
            if not peers:
                break
        assert not peers, f"{len(peers)} socket(s) left behind by connects that never came up"
    finally:
        await layer.close()
        for writer in list(peers.values()):
            writer.close()
        server.close()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(server.wait_closed(), 5)


async def test_a_plain_channel_in_a_group_survives_a_reconnect(make_layer, caplog):
    """A shared channel name in a group takes each message once, not once per process.

    ``group_add`` on a plain channel subscribes to the group's subject under the
    channel's queue group, so the group message reaches that name once however many
    processes read it. That is the fourth kind of subscription ``_resubscribe()``
    rebuilds, and the only one no test covered -- also the one its "restored N"
    count left out.
    """
    a, b, publisher = make_layer(), make_layer(), make_layer()
    got_a: list = []
    got_b: list = []

    async def collect(worker, into):
        while True:
            into.append(await worker.receive("shared"))

    readers = [asyncio.create_task(collect(a, got_a)), asyncio.create_task(collect(b, got_b))]
    try:
        await asyncio.sleep(0.2)  # let both subscribe before anything is published
        await a.group_add("room", "shared")
        await b.group_add("room", "shared")

        async def fan_out(count=10):
            got_a.clear()
            got_b.clear()
            for index in range(count):
                await publisher.group_send("room", {"type": "test.message", "index": index})
            for _ in range(50):
                await asyncio.sleep(0.1)
                if len(got_a) + len(got_b) >= count:
                    break
            await asyncio.sleep(0.2)  # a duplicate would arrive by now
            return len(got_a) + len(got_b)

        assert await fan_out() == 10, "a group message reached the shared name more than once"

        with caplog.at_level(logging.WARNING, logger="channels_nats"):
            for worker in (a, b):
                await worker._state().client.close()
            for _ in range(80):
                await asyncio.sleep(0.25)
                if all(not worker._state().client.is_closed for worker in (a, b)):
                    break
            assert all(not worker._state().client.is_closed for worker in (a, b)), "no reconnect"

        assert await fan_out() == 10, "the group's queue subscription did not come back"

        restored = [record.message for record in caplog.records if "restored" in record.message]
        assert restored, "the reconnect was not reported"
        assert all("restored 2 subscriptions" in message for message in restored), restored
    finally:
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)


async def test_a_group_subscription_goes_when_its_last_process_channel_leaves(layer):
    """The group subscription serves this process's own channels, nothing else.

    A plain member is served by its own queue subscription, so once the last
    channel with a "!" leaves, the group subscription has nobody left to deliver to
    -- and every message published to that group still arrives, is handed to a
    callback that drops all of it, and counts against the connection's pending
    limits on the way (confirmed on the server's /subsz: the subscription stayed).
    """
    channel = await layer.new_channel()
    reading = asyncio.create_task(layer.receive("shared"))
    await asyncio.sleep(0.1)
    await layer.group_add("room", channel)
    await layer.group_add("room", "shared")
    state = layer._state()
    subscription = state.group_subscriptions["room"]

    await layer.group_discard("room", channel)

    assert "room" not in state.group_subscriptions, "the group subscription has nobody to deliver to"
    assert subscription._closed, "it was dropped from the records but left on the connection"
    assert ("room", "shared") in state.group_channel_subscriptions, "the plain member lost its subscription"

    await layer.group_send("room", {"type": "test.message"})
    assert await asyncio.wait_for(reading, 5) == {"type": "test.message"}


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
def test_a_loop_that_closes_while_holding_a_connection_is_reported(nats_url, caplog):
    """The layer cannot close that connection, so it has to say so.

    Connections are per event loop. A loop that closes takes its own state with it
    and the connection is left to the garbage collector -- measured: neither
    ``writer.close()`` nor ``transport.abort()`` releases it once the loop is gone.
    ``loop_state_warn_at`` does not catch this: those states are pruned, so the count
    it watches stays low. Twenty ``async_to_sync`` calls left seventeen connections
    open on the server and no warning at all.
    """
    layer = NatsChannelLayer(servers=nats_url)
    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        for _ in range(2):
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(layer.group_send("room", {"type": "test.message"}))
            finally:
                loop.close()
        asyncio.run(layer.group_send("room", {"type": "test.message"}))  # notices them
        orphaned = layer._orphaned_connections

    # Collect here rather than leaving it to land in some later test: the abandoned
    # client's own tasks are reported as destroyed while pending when it goes, which
    # is the same leak this test is about.
    del layer
    gc.collect()

    reported = [record.message for record in caplog.records if "closed while still holding" in record.message]
    assert reported, [record.message for record in caplog.records]
    # Both were counted; the warning is rate-limited like the other drop reports, so the
    # second is carried by the count rather than by a second line.
    assert orphaned == 2


async def test_flush_does_not_strand_a_receive_that_was_still_subscribing(layer):
    """``flush()`` while the channel's first ``receive()`` is still coming up.

    ``_mailbox()`` registers the box before it subscribes, so ``flush()`` finds it
    and marks it superseded -- but with no receiver counted yet there is nothing to
    wake, and ``_mailbox()`` hands that box back anyway. The read then waits on a
    queue nothing feeds, and ``close()`` cannot reach it either: the box is no longer
    in ``mailboxes``. Same stranding as #21 and #22, one window earlier.
    """
    client = await layer._client()
    original, subscribed, release = client.flush, asyncio.Event(), asyncio.Event()

    async def gated_flush(*args, **kwargs):
        result = await original(*args, **kwargs)
        subscribed.set()  # the server has the subscription; the box is registered
        await release.wait()
        return result

    client.flush = gated_flush
    try:
        waiting = asyncio.create_task(layer.receive("raceplain"))
        await asyncio.wait_for(subscribed.wait(), 5)

        await layer.flush()  # takes the mailbox away while the read is still coming up
    finally:
        release.set()
        client.flush = original

    await layer._mailbox("raceplain")  # the channel is established again, however it happens
    await layer.send("raceplain", {"type": "after"})
    assert await asyncio.wait_for(waiting, 5) == {"type": "after"}


async def test_a_group_add_that_flush_overtook_does_not_leave_its_subscription(layer):
    """``flush()`` landing while ``group_add`` waits for the server to have its subscription.

    flush clears the memberships, so the group_add that finishes afterwards has
    nobody to deliver to -- but it recorded its subscription anyway, and the server
    kept sending that group's traffic to a callback with an empty member list.
    ``_resubscribe`` already re-checks before it writes a subscription back; this
    path did not.
    """
    channel = await layer.new_channel()
    client = await layer._client()
    original, waiting_on_server, release = client.flush, asyncio.Event(), asyncio.Event()

    async def gated_flush(*args, **kwargs):
        result = await original(*args, **kwargs)
        waiting_on_server.set()
        await release.wait()
        return result

    client.flush = gated_flush
    try:
        adding = asyncio.create_task(layer.group_add("room", channel))
        await asyncio.wait_for(waiting_on_server.wait(), 5)

        await layer.flush()  # the membership goes while the subscription is being confirmed
    finally:
        release.set()
        client.flush = original
    await adding

    state = layer._state()
    assert not state.groups.get("room"), "flush should have cleared the membership"
    assert "room" not in state.group_subscriptions, "a group subscription with no member was kept"


async def test_a_message_this_layer_cannot_read_back_is_reported_without_blame(layer, caplog):
    """msgpack packs a non-string map key and then refuses to unpack it.

    So a frame this layer sent can arrive unreadable, and the report used to say
    "something other than this layer is publishing to its subjects" -- which sends
    whoever is diagnosing it looking for a publisher that does not exist.
    """
    channel = await layer.new_channel()
    with caplog.at_level(logging.WARNING, logger="channels_nats"):
        await layer.send(channel, {"type": "test.message", "nested": {1: "packs but does not unpack"}})
        await layer.send(channel, {"type": "test.message", "text": "readable"})
        assert await asyncio.wait_for(layer.receive(channel), 5) == {
            "type": "test.message",
            "text": "readable",
        }

    dropped = [record.message for record in caplog.records if "could not be read" in record.message]
    assert dropped, [record.message for record in caplog.records]
    assert "Either something else is publishing" in dropped[0], dropped[0]


async def test_a_full_mailbox_still_reclaims_what_has_expired(make_layer):
    """The sweep is skipped while nothing can be reclaimed, not when something can.

    Emptying a full queue to refill it costs the whole queue on every message that
    arrives past capacity -- 8.7 ms each at capacity 50,000, measured. The mailbox
    keeps the oldest arrival so that cost is only paid when it can buy something.
    """
    layer = make_layer(capacity=3, expiry=0.2)
    box = _Mailbox(queue=asyncio.Queue())
    for _ in range(3):
        box.queue.put_nowait((time.monotonic(), b"first"))

    layer._enqueue(box, "full", b"turned away")  # nothing has expired yet
    assert box.queue.qsize() == 3 and box.dropped == 1

    await asyncio.sleep(0.25)  # now everything in there is past expiry
    layer._enqueue(box, "full", b"takes their place")

    assert box.queue.qsize() == 1, "the expired messages were not reclaimed"
    assert box.queue.get_nowait()[1] == b"takes their place"
