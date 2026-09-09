"""``NatsChannelLayer``: Django Channels on top of a NATS server.

Subjects are the contract. Anything that speaks NATS (another Python worker,
a Go front, a CLI) can join the same layer by publishing to them:

- ``<prefix>.pc.<process>``  one message for one process-specific channel
  (``send`` to ``specific.<process>!<id>``); the full channel name travels in
  the ``Channel`` header and the receiving process routes it locally
- ``<prefix>.ch.<channel>``  one message for a plain (shared-name) channel,
  read under a queue group of the same name so that one reader gets it rather
  than every process holding that name
- ``<prefix>.grp.<group>``   one message for every member of a group (``group_send``)

Semantics follow the Channels layer spec on an at-most-once transport:

- A process holds **one** subscription for all its process-specific channels
  (consumers), one per group it has members in, and one per plain channel it
  receives on. Per-connection cost is a local queue, not a NATS subscription.
- ``group_send`` is one NATS publish; the server fans out. The group
  subscription copies the message into the local mailbox of every member.
- A message that has waited ``expiry`` seconds **in its mailbox** is dropped on
  ``receive``. The clock starts when the callback files it, so neither the network
  nor nats-py's own pending queue counts towards it: publish time would mean
  trusting the publisher's clock, and expiry would then differ per process.
  A mailbox holding ``capacity`` messages drops new ones (Channels' ``ChannelFull``
  cannot be raised on the sender's side over pub/sub). ``capacity`` is per full
  channel name, not shared across one process prefix as the spec's MUST asks: a
  server with many connections would otherwise have them push each other out.
- Messages published before a channel's first ``receive()`` (or ``new_channel()``)
  are lost: there is no subscriber yet. Consumers always subscribe on connect,
  so this only matters for ad-hoc channel names.
- A process channel's mailbox lives until it has had no receiver, no traffic and
  no group membership for ``mailbox_grace`` seconds. Cancellation cannot carry
  this: an application polling with ``wait_for`` cancels a read every timeout and
  is still there, and a consumer ending on a channel-layer message leaves no
  pending read to cancel at all. A plain channel is different -- its subscription
  is under the channel's queue group, so it is released on the cancelled read
  rather than held, and polling one loses what arrives between two polls.
- ``group_expiry`` is stored and never enforced: a membership goes on
  ``group_discard``, on ``flush()``, or when the process ends.
- ``send``/``group_send`` can raise ``asyncio.CancelledError`` even though the
  publish went out: nats-py discards a cancellation delivered while it waits to
  flush, and the layer restores it rather than let a cancelled consumer run on.
- ``close()`` ends a ``receive()`` that was waiting on this loop with
  ``ChannelLayerClosed``. Its mailbox goes with the loop's state and nothing would
  feed it again, and ``flush()``'s remedy -- move to the mailbox that replaces it --
  would reopen the connection the caller has just closed.
- If nats-py gives up reconnecting and closes the client, the next call opens a
  new connection and restores this loop's subscriptions on it.

Connections are per event loop, so ``async_to_sync(layer.group_send)`` from
Django signals or views works alongside the consumers' loop. A process-specific
channel belongs to the loop whose ``new_channel()`` handed it out: mailboxes and
subscriptions live in that loop, and receiving on it from another one would put a
second subscription on the same process subject and deliver every message twice.

"""

from __future__ import annotations

import asyncio
import logging
import time
import typing as t
import uuid
from dataclasses import dataclass, field

import nats
from channels.exceptions import ChannelFull, MessageTooLarge
from channels.layers import BaseChannelLayer
from nats.aio.client import Client
from nats.aio.subscription import Subscription
from nats.errors import SlowConsumerError

from . import serializers

log = logging.getLogger("channels_nats")

Message = dict[str, t.Any]

#: How long tidying up waits for a command the connection has not sent yet, before
#: handing it to a background task. Both ``Client.subscribe()`` and
#: ``Subscription.unsubscribe()`` wait for the wire, so under send backpressure
#: neither returns until the connection drains -- and a cancellation that waits
#: that long has stopped being a cancellation. The work still happens; what is
#: bounded is how long the caller is held for it.
CLEANUP_GRACE = 1.0


class ChannelLayerClosed(RuntimeError):
    """Ends a ``receive()`` that was waiting when ``close()`` took the layer down.

    ``close()`` drops the event loop's state and drains its connection, so nothing
    will feed that mailbox again. ``flush()``'s remedy -- wake the receiver and move
    it to the mailbox that replaces this one -- cannot be used here: reaching for a
    new mailbox would reopen the connection the caller has just closed. So the read
    ends and says why, rather than waiting for a message that cannot arrive.
    """


@dataclass
class _Mailbox:
    queue: asyncio.Queue[tuple[float, bytes]]
    #: Set once the mailbox either has its subscription or has given up. A mailbox
    #: goes into ``mailboxes`` before subscribing, so a caller that finds one there
    #: has to wait: an unsubscribed mailbox looks ready and receives nothing.
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    subscribed: bool = False
    #: Last time a message arrived or a receiver left. What decides whether the
    #: consumer behind a process channel is still there.
    active_at: float = field(default_factory=time.monotonic)
    subscription: Subscription | None = None
    #: Set when ``flush()`` takes this mailbox away. Nothing feeds the queue after
    #: that, so a receiver waiting on it has to move to the mailbox that replaces
    #: it instead of waiting for a message that can never arrive.
    superseded: bool = False
    #: Set when ``close()`` takes the whole loop's state away. Nothing replaces this
    #: mailbox, so a receiver waiting on it ends with ``ChannelLayerClosed``.
    closed: bool = False
    receivers: int = 0
    dropped: int = 0
    warned_at: float | None = None


@dataclass
class _LoopState:
    """Everything bound to one asyncio event loop."""

    client: Client | None = None
    #: Names the process channels this loop hands out. Per loop, not per instance:
    #: each loop has its own connection, subscription and mailboxes, so two loops
    #: sharing one id would subscribe to one subject and deliver each message twice.
    client_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    #: Held while a subscription is being created, so that coroutines racing to
    #: reach the same subject end up sharing one. Never taken inside ``lock``.
    subscribe_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    mailboxes: dict[str, _Mailbox] = field(default_factory=dict)
    groups: dict[str, set[str]] = field(default_factory=dict)
    group_subscriptions: dict[str, Subscription] = field(default_factory=dict)
    #: (group, plain channel) -> a queue subscription on that group's subject, so a
    #: channel name several processes read gets each message once, not once each.
    group_channel_subscriptions: dict[tuple[str, str], Subscription] = field(default_factory=dict)
    process_subscriptions: dict[str, Subscription] = field(default_factory=dict)
    recovery: asyncio.Task | None = None
    #: Commands that outlived the caller that was cancelled, kept so that close()
    #: can take them down rather than leaving one pending at loop shutdown.
    cleanups: set[asyncio.Future] = field(default_factory=set)
    swept_at: float = 0.0
    #: Messages nats-py threw away before they reached a mailbox, so they are in
    #: no mailbox's ``dropped``. Per connection, which is what the limit is on.
    slow_consumer_drops: int = 0
    slow_consumer_warned_at: float | None = None
    #: Frames that reached a mailbox but are not a Channels message. Only a
    #: publisher other than this layer can produce one.
    bad_frames: int = 0
    bad_frame_warned_at: float | None = None


class NatsChannelLayer(BaseChannelLayer):
    """Channel layer backed by a NATS server.

    settings.py::

        CHANNEL_LAYERS = {
            "default": {
                "BACKEND": "channels_nats.NatsChannelLayer",
                "CONFIG": {"servers": ["nats://127.0.0.1:4222"]},
            }
        }
    """

    #: ``flush`` is not here on purpose. The spec's flush extension has to leave the
    #: layer looking empty to every client of a distributed layer; ``flush()`` below
    #: clears one event loop of one instance, which is enough to reset a test and not
    #: what the extension promises. The method stays; the claim does not.
    extensions = ["groups"]

    #: The spec asks a layer to carry these. ``ChannelFull`` is never raised here:
    #: over pub/sub a sender cannot see the receiver's queue (see the README).
    MessageTooLarge = MessageTooLarge
    ChannelFull = ChannelFull

    #: A full mailbox stays full, so warn about the first drop and then at most
    #: this often, rather than once per message.
    drop_log_interval = 60.0

    #: How long a process channel's mailbox is kept after its last receiver goes
    #: away. It has to be longer than a consumer may spend handling one message:
    #: no ``receive()`` is outstanding while its handler runs. Deliberately not
    #: tied to ``expiry`` -- tightening how long a message may wait must not start
    #: reclaiming mailboxes out from under a slow handler.
    mailbox_grace = 60.0

    #: A process holds a handful of event loops, not dozens. More than this
    #: usually means loops are being abandoned without ``close()``: nothing can
    #: tell those apart from live ones, so their connections stay held.
    loop_state_warn_at = 32

    def __init__(
        self,
        servers: str | t.Sequence[str] = "nats://127.0.0.1:4222",
        prefix: str = "channels",
        expiry: int = 60,
        group_expiry: int = 86400,
        capacity: int = 100,
        channel_capacity: t.Any = None,
        connect_options: dict[str, t.Any] | None = None,
    ) -> None:
        super().__init__(expiry=expiry, capacity=capacity, channel_capacity=channel_capacity)
        # BaseChannelLayer stores the raw dict, but get_capacity iterates compiled
        # (pattern, capacity) pairs, so every layer has to compile it itself.
        # Channels annotates the attribute as the dict its own get_capacity cannot read.
        self.channel_capacity = self.compile_capacities(channel_capacity or {})  # type: ignore
        self.servers = [servers] if isinstance(servers, str) else list(servers)
        self.prefix = prefix
        self.group_expiry = group_expiry
        self.connect_options = dict(connect_options or {})
        self._states: dict[asyncio.AbstractEventLoop, _LoopState] = {}
        self._warned_about_loops = False

    # ------------------------------------------------------------------ plumbing

    def _state(self) -> _LoopState:
        loop = asyncio.get_running_loop()
        state = self._states.get(loop)
        if state is None:
            # A new loop is rare, so this is the cheap moment to forget the loops
            # that have since been closed -- their connections died with them, and
            # a closed loop can still be referenced elsewhere, so waiting for the
            # garbage collector would not do.
            self._forget_closed_loops()
            state = self._states[loop] = _LoopState()
            if len(self._states) > self.loop_state_warn_at and not self._warned_about_loops:
                self._warned_about_loops = True
                log.warning(
                    "channels_nats: %d event loops have used this layer. A loop abandoned "
                    "without close() cannot be told from a live one, so its connection is "
                    "still held; close the loop, or the layer, when you are done with it.",
                    len(self._states),
                )
        return state

    def _state_if_open(self) -> _LoopState | None:
        """This loop's state, or None -- without making one.

        ``_state()`` creates on demand, which is right for a caller that is about to
        use the layer and wrong for one that is only tidying up after itself: a
        cancelled read arriving after ``close()`` would bring the state of a loop the
        caller has just closed back. The revived entry holds no connection and no
        subscription, but it counts towards ``loop_state_warn_at``, whose warning is
        about loops that still hold one.
        """
        return self._states.get(asyncio.get_running_loop())

    def _forget_closed_loops(self) -> None:
        for closed in [loop for loop in self._states if loop.is_closed()]:
            del self._states[closed]

    async def _client(self) -> Client:
        state = self._state()
        if state.client is None or state.client.is_closed:
            async with state.lock:
                if state.client is None or state.client.is_closed:
                    stale = state.client is not None
                    client = await self._connect(state)
                    if stale:
                        try:
                            await self._resubscribe(state, client)
                        except BaseException:
                            # Leaving an open but unsubscribed client behind would look
                            # healthy to every later call, and nothing would receive again.
                            await client.close()
                            raise
                    state.client = client
        return state.client

    async def _connect(self, state: _LoopState) -> Client:
        options = dict(self.connect_options)
        given = options.pop("closed_cb", None)
        given_error = options.pop("error_cb", None)

        async def on_closed() -> None:
            # Before the user's callback, not after: one that raises, hangs or gets
            # cancelled would otherwise take the layer's own safety net with it, and
            # a receive-only process has nothing else that would notice.
            self._start_recovery(state)
            if given is not None:
                await given()

        async def on_error(error: Exception) -> None:
            if isinstance(error, SlowConsumerError):
                self._note_slow_consumer(state, error)
            if given_error is not None:
                await given_error(error)
            else:
                # nats-py installs its own error_cb only when none is given, and this
                # one takes that place. Keep its line so that anything already reading
                # the nats logger for other errors does not go quiet.
                logging.getLogger("nats.aio.client").error("nats: encountered error", exc_info=error)

        return await nats.connect(servers=self.servers, closed_cb=on_closed, error_cb=on_error, **options)

    def _note_slow_consumer(self, state: _LoopState, error: SlowConsumerError) -> None:
        """Report a message nats-py dropped before it could reach a mailbox.

        The layer counts drops in ``_enqueue``, where its own queue overflows. A
        subscription that exceeds nats-py's pending limits loses messages a layer
        above that, so without this the operator reads "nothing was dropped" while
        messages are missing. Not folded into a mailbox's ``dropped``: the loss is
        per subscription, and a process subject carries every channel of a process.
        """
        state.slow_consumer_drops += 1
        now = time.monotonic()
        if state.slow_consumer_warned_at is not None and now - state.slow_consumer_warned_at < self.drop_log_interval:
            return
        state.slow_consumer_warned_at = now
        log.warning(
            "channels_nats: nats-py dropped a message on %s before it reached a mailbox "
            "(slow consumer; %d so far on this connection). Raise pending_bytes_limit / "
            "pending_msgs_limit, or read faster.",
            error.subject,
            state.slow_consumer_drops,
        )

    def _start_recovery(self, state: _LoopState) -> None:
        """Come back on our own once nats-py has given up reconnecting.

        A process that only receives never calls ``_client()`` again, so nothing
        would notice: its subscriptions would sit on a dead connection and
        ``receive()`` would wait forever while other processes keep publishing.
        """
        if state not in self._states.values():
            return  # closed on purpose; nothing to come back to
        if state.recovery is not None and not state.recovery.done():
            return
        state.recovery = asyncio.create_task(self._recover(state))

    async def _recover(self, state: _LoopState) -> None:
        delay = 0.5
        while state in self._states.values() and (state.client is None or state.client.is_closed):
            try:
                await self._client()  # reconnects and restores this loop's subscriptions
                return
            except Exception as error:
                log.warning("channels_nats: reconnect failed (%s); trying again in %.1fs", error, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    async def _resubscribe(self, state: _LoopState, client: Client) -> None:
        """Rebuild every subscription of this loop on a fresh connection.

        nats-py restores subscriptions across its own reconnects, but once it has
        spent ``max_reconnect_attempts`` it closes the client for good. The
        connection opened after that is new and knows nothing about them, so
        without this the process would keep publishing while receiving nothing.
        """
        # Each subscribe is an await, and flush(), group_discard() or a cancelled
        # receive() can run in it. So iterate over snapshots, and before storing a
        # subscription check that what it belongs to is still wanted -- writing it
        # back unconditionally would put a subscription the process has just given
        # up back on the server, and it would keep delivering.
        stale: list[Subscription] = []
        for subject in list(state.process_subscriptions):
            subscription = await self._subscribe(client, subject, cb=self._process_deliver(state))
            if subject in state.process_subscriptions:
                state.process_subscriptions[subject] = subscription
            else:
                stale.append(subscription)
        for channel, box in list(state.mailboxes.items()):
            if box.subscription is None:
                continue
            subscription = await self._subscribe(
                client,
                self.channel_subject(channel),
                queue=self.channel_queue_group(channel),
                cb=self._channel_deliver(box, channel),
            )
            if state.mailboxes.get(channel) is box:
                box.subscription = subscription
            else:
                stale.append(subscription)
        for group in list(state.group_subscriptions):
            subscription = await self._subscribe(
                client, self.group_subject(group), cb=self._group_deliver(state, group)
            )
            if group in state.group_subscriptions:
                state.group_subscriptions[group] = subscription
            else:
                stale.append(subscription)
        for group, channel in list(state.group_channel_subscriptions):
            subscription = await self._subscribe(
                client,
                self.group_subject(group),
                queue=self.channel_queue_group(channel),
                cb=self._group_channel_deliver(state, channel),
            )
            if (group, channel) in state.group_channel_subscriptions:
                state.group_channel_subscriptions[group, channel] = subscription
            else:
                stale.append(subscription)
        await self._unsubscribe(stale)
        await client.flush()
        log.warning(
            "channels_nats: the NATS connection was closed; reconnected and restored %d subscriptions",
            len(state.process_subscriptions)
            + len(state.group_subscriptions)
            + sum(1 for box in state.mailboxes.values() if box.subscription is not None),
        )

    CHANNEL_HEADER = "Channel"

    def channel_subject(self, channel: str) -> str:
        """Subject a message for ``channel`` is published to."""
        if "!" in channel:
            return self.process_subject(channel)
        return f"{self.prefix}.ch.{channel}"

    def process_subject(self, channel: str) -> str:
        """Subject shared by every process-specific channel of one process (``specific.<id>!...``)."""
        return f"{self.prefix}.pc.{channel[: channel.index('!')]}"

    def owns_channel(self, channel: str) -> bool:
        """Whether ``new_channel()`` on the running loop handed this channel out."""
        return channel[: channel.index("!")].rsplit(".", 1)[-1] == self._state().client_id

    def channel_queue_group(self, channel: str) -> str:
        """Queue group a plain channel is read under.

        A channel is a queue: when several processes read the same name, one of
        them gets each message, not all of them. Core NATS gives that with a
        queue group; a plain subscription would fan out and every worker in a
        pool would run every job. Anything else joining ``<prefix>.ch.<channel>``
        has to use this same group or single delivery breaks again.
        """
        return self.channel_subject(channel)

    def group_subject(self, group: str) -> str:
        return f"{self.prefix}.grp.{group}"

    def _channel_deliver(self, box: _Mailbox, channel: str) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            self._enqueue(box, channel, msg.data)

        return deliver

    def _process_deliver(self, state: _LoopState) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            target = (msg.headers or {}).get(self.CHANNEL_HEADER)
            if not isinstance(target, str):
                return
            box = state.mailboxes.get(target)
            if box is None:
                # The spec has the part before "!" name the process, so a read on
                # the bare prefix is a read on that process's channels. Consumers
                # pass the full name new_channel() gave them, which is why this is
                # the fallback rather than the first lookup.
                box = state.mailboxes.get(target[: target.index("!") + 1] if "!" in target else target)
            if box is not None:
                self._enqueue(box, target, msg.data)

        return deliver

    def _group_deliver(self, state: _LoopState, group: str) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            for member in list(state.groups.get(group, ())):
                if "!" not in member:
                    continue  # plain channels come in on their own queue subscription
                box = state.mailboxes.get(member)
                if box is not None:
                    self._enqueue(box, member, msg.data)

        return deliver

    async def _subscribe(self, client: Client, subject: str, cb: t.Callable, queue: str = "") -> Subscription:
        """Subscribe so that a cancellation cannot leave a subscription behind.

        ``Client.subscribe()`` registers the subscription and starts its callback
        task before it awaits the wire, so a cancellation inside it never hands
        back the ``Subscription`` -- and what the caller never receives, it cannot
        take down. The leftover keeps delivering: under a queue group it takes
        messages away from the subscription that replaces it and files them in a
        mailbox nobody can reach, and on a process or group subject it delivers
        every message a second time.

        Running it as a task keeps the handle reachable either way, so the caller
        can unsubscribe on the way out. That wait is bounded: under send backpressure
        ``Client.subscribe()`` does not return until the connection drains, and a
        cancelled caller that waits for that is no longer cancelled in any useful
        sense. Past ``CLEANUP_GRACE`` the subscribe is handed to a
        background task that takes it down whenever it does come up. A second
        cancellation arriving while we wait does leak -- there is nowhere left to wait.
        """
        subscribing = asyncio.ensure_future(client.subscribe(subject, queue=queue, cb=cb))
        try:
            return await asyncio.shield(subscribing)
        except BaseException:
            try:
                # shield again: a timeout here must not cancel the subscribe itself,
                # or the handle goes back to being unreachable, which is #11.
                leftover = await asyncio.wait_for(asyncio.shield(subscribing), CLEANUP_GRACE)
            except asyncio.TimeoutError:
                self._unsubscribe_later(subscribing)
            except Exception:
                pass  # it never came up, so there is nothing to take down
            else:
                await self._unsubscribe([leftover])
            raise

    def _unsubscribe_later(self, subscribing: asyncio.Future) -> None:
        """Take a subscription down once it finally comes up.

        Its caller is gone, so nobody is left to hold the handle.
        """

        async def take_down() -> None:
            try:
                leftover = await subscribing
            except Exception:
                return  # it never came up
            await self._unsubscribe([leftover])

        self._finish_later(asyncio.ensure_future(take_down()))

    def _finish_later(self, work: asyncio.Future) -> None:
        """Let a command the caller no longer waits for run to its end.

        The loop's state holds it: dropping the reference would let the garbage
        collector take the task, and one still pending when the loop closes is
        reported as destroyed. ``close()`` cancels whatever is still here -- the
        connection goes with it, and so do its subscriptions.
        """
        task = asyncio.ensure_future(work)
        state = self._state_if_open()
        if state is None:
            # close() already took this loop's state, and the connection with it, so
            # the command has nothing left to reach. Asking _state() for somewhere to
            # keep the task would revive the loop the caller has just closed (#24).
            task.cancel()
            return
        state.cleanups.add(task)
        task.add_done_callback(state.cleanups.discard)

    async def _flush_or_undo(self, client: Client, subscription: Subscription) -> None:
        """Wait for the server to have the subscription, or take it back down.

        A subscription whose flush was cancelled is live and unaccounted for; the
        caller is about to fail, so it must not be left on the connection.
        """
        try:
            await client.flush()
        except BaseException:
            await self._unsubscribe([subscription])
            raise

    async def _unsubscribe(self, subscriptions: t.Iterable[Subscription]) -> None:
        """Drop subscriptions, tolerating a connection that has already gone.

        Every caller has finished its bookkeeping by now, so a connection that
        died must not turn tearing down into an error. Nor must a jammed one hold
        the caller: ``unsubscribe()`` sends a command, so under send backpressure it
        waits for the connection to drain (measured: still waiting after eight
        seconds). Callers tidying up after a cancellation would wait with it, so
        past ``CLEANUP_GRACE`` the command is left to a background task.
        """
        for subscription in subscriptions:
            dropping = asyncio.ensure_future(subscription.unsubscribe())
            try:
                # shield: the timeout must not cancel the unsubscribe itself, or the
                # subscription stays up and keeps taking messages from its queue group.
                await asyncio.wait_for(asyncio.shield(dropping), CLEANUP_GRACE)
            except asyncio.TimeoutError:
                self._finish_later(dropping)
            except Exception as error:
                log.debug("channels_nats: could not unsubscribe: %s", error)

    def _group_channel_deliver(self, state: _LoopState, channel: str) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            box = state.mailboxes.get(channel)
            if box is not None:
                self._enqueue(box, channel, msg.data)

        return deliver

    def _frame_size(self, payload: bytes, headers: dict[str, str] | None) -> int:
        """What the server weighs against ``max_payload``.

        nats-py only checks the payload, so a body just under the limit plus a
        header goes out, and the server answers by closing the connection --
        taking every other channel on it down too.
        """
        if headers is None:
            return len(payload)
        overhead = len(b"NATS/1.0") + 2 + 2  # the version line and the blank line after the headers
        for key, value in headers.items():
            overhead += len(key) + 2 + len(value) + 2  # "key: value\r\n"
        return len(payload) + overhead

    async def _publish(self, subject: str, message: Message, headers: dict[str, str] | None = None) -> None:
        payload = serializers.dumps(message)
        client = await self._client()
        size = self._frame_size(payload, headers)
        if size > client.max_payload:
            raise MessageTooLarge(
                f"message is {size} bytes on the wire, over the server's max_payload of "
                f"{client.max_payload}; raise max_payload on nats-server or send less"
            )
        task = asyncio.current_task()
        requested = task.cancelling() if task is not None else 0
        await client.publish(subject, payload, headers=headers)
        if task is not None and task.cancelling() > requested:
            # nats-py's _flush_pending() ends with `except asyncio.CancelledError:
            # pass`, so a cancellation delivered while a publish waits for the flusher
            # is discarded and publish() returns as if nothing happened -- a consumer
            # cancelled at shutdown goes back to its loop and never ends (#23; the
            # real fix is upstream's to make). It does not uncancel(), so the request is
            # counted here, which is the only trace it leaves. The publish itself may
            # well have gone out; what is restored is the caller's cancellation, not
            # the message.
            raise asyncio.CancelledError

    def _drop_expired(self, box: _Mailbox, now: float) -> None:
        """Reclaim the room taken by messages ``receive()`` would throw away anyway.

        Synchronous, so the queue is never seen half drained.
        """
        kept = []
        while not box.queue.empty():
            entry = box.queue.get_nowait()
            if now - entry[0] <= self.expiry:
                kept.append(entry)
        for entry in kept:
            box.queue.put_nowait(entry)

    def _enqueue(self, box: _Mailbox, channel: str, payload: bytes) -> None:
        """Queue the encoded message.

        Decoding waits for ``receive()``: it happens once per message actually
        taken, each reader parses its own bytes into its own object, and messages
        dropped for capacity or expiry are never decoded at all.
        """
        now = time.monotonic()
        capacity = self.get_capacity(channel)
        if box.queue.qsize() >= capacity:
            # A stalled consumer leaves stale messages holding every slot, and then
            # the live ones are dropped in favour of messages nobody will be given.
            self._drop_expired(box, now)
        if box.queue.qsize() >= capacity:
            box.dropped += 1
            if box.warned_at is None or now - box.warned_at >= self.drop_log_interval:
                log.warning(
                    "channels_nats: mailbox for %s is full (capacity %d); %d message(s) dropped so far",
                    channel,
                    capacity,
                    box.dropped,
                )
                box.warned_at = now
            return
        if box.dropped:
            log.warning(
                "channels_nats: mailbox for %s has room again after dropping %d message(s)",
                channel,
                box.dropped,
            )
            box.dropped = 0
            box.warned_at = None
        box.active_at = now
        box.queue.put_nowait((now, payload))

    async def _mailbox(self, channel: str) -> _Mailbox:
        """Local queue for ``channel``.

        Process-specific channels share the process subscription; a plain
        channel gets a subscription of its own on first use.
        """
        if "!" in channel and not self.owns_channel(channel):
            # Subscribing would put us on the owner's process subject, so every message
            # for its channels would be delivered twice: once there and once here.
            raise ValueError(
                f"{channel} belongs to another process or event loop; a loop can only "
                "receive on the channels its own new_channel() handed out"
            )
        state = self._state()
        self._sweep_mailboxes(state)
        while (box := state.mailboxes.get(channel)) is not None:
            await box.ready.wait()
            if box.subscribed:
                return box
            # Whoever created it failed to subscribe and took it out again; the error
            # was theirs to raise, so try once more on our own behalf.
        # Subscribing takes a round trip, and connecting takes several. Without this
        # lock, consumers arriving together each get past the check above and each
        # subscribe, so one message arrives once per racing caller.
        async with state.subscribe_lock:
            box = state.mailboxes.get(channel)
            if box is not None:  # somebody won the race while we waited
                return box  # and finished under this lock, so it is subscribed
            box = _Mailbox(queue=asyncio.Queue())
            state.mailboxes[channel] = box
            try:
                if "!" in channel:
                    await self._process_subscription(channel)
                else:
                    client = await self._client()
                    box.subscription = await self._subscribe(
                        client,
                        self.channel_subject(channel),
                        queue=self.channel_queue_group(channel),
                        cb=self._channel_deliver(box, channel),
                    )
                    await client.flush()  # the server knows about the subscription before we return
            except BaseException:  # a cancelled subscribe must not leave a mailbox nothing feeds
                if state.mailboxes.get(channel) is box:
                    del state.mailboxes[channel]
                box.ready.set()  # after the removal, so a waiter retrying cannot find it again
                if box.subscription is not None:
                    await self._unsubscribe([box.subscription])
                raise
            box.subscribed = True
            box.ready.set()
            return box

    async def _process_subscription(self, channel: str) -> None:
        """One subscription per process prefix, routing by the ``Channel`` header.

        Only called with ``subscribe_lock`` held, which is what keeps it to one.
        """
        state = self._state()
        subject = self.process_subject(channel)
        if subject in state.process_subscriptions:
            return
        client = await self._client()
        subscription = await self._subscribe(client, subject, cb=self._process_deliver(state))
        await self._flush_or_undo(client, subscription)
        # Recorded only once the server has it. Recording first and flushing after
        # left a cancelled flush looking established: the entry stayed, the next
        # caller took the early return above, and nothing ever confirmed the SUB.
        state.process_subscriptions[subject] = subscription

    # ------------------------------------------------------------------ channels

    async def send(self, channel: str, message: Message) -> None:
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_channel_name(channel)
        assert "__asgi_channel__" not in message, "Reserved key '__asgi_channel__' in message"
        headers = {self.CHANNEL_HEADER: channel} if "!" in channel else None
        await self._publish(self.channel_subject(channel), message, headers)

    async def receive(self, channel: str) -> Message:
        self.require_valid_channel_name(channel)
        box = await self._mailbox(channel)
        box.receivers += 1
        cancelled = False
        try:
            while True:
                queued_at, payload = await box.queue.get()
                if box.closed:
                    # close() dropped the state this mailbox belonged to. Unlike a
                    # superseded one it has no replacement to move to, and asking for
                    # one would reopen the connection the caller has just closed.
                    raise ChannelLayerClosed(f"receive({channel!r}) was waiting when the layer was closed")
                if box.superseded:
                    # flush() dropped this mailbox, and what was left in its queue
                    # went with it. Take up the one that replaced it -- the two
                    # counter changes have no await between them, so no cancellation
                    # can land with this read counted on both or on neither.
                    replacement = await self._mailbox(channel)
                    replacement.receivers += 1
                    box.receivers -= 1
                    box = replacement
                    continue
                if time.monotonic() - queued_at > self.expiry:
                    continue
                message = self._decode(channel, payload)
                if message is None:
                    continue
                return message
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            box.receivers -= 1
            if box.receivers == 0:
                box.active_at = time.monotonic()  # the grace period starts here
                if cancelled and "!" not in channel:
                    await self._discard_mailbox(channel, box)

    def _decode(self, channel: str, payload: bytes) -> Message | None:
        """Read a frame, or drop it and say so.

        The subjects are the contract and the README invites other publishers to
        use them, so a frame that is not a msgpack map is the sender breaking that
        contract. Letting it out of ``receive()`` would end a consumer that did
        nothing wrong -- and on a group subject one bad publish would end every
        member -- so it is dropped like an expired message and reported instead.
        Messages this layer sent are checked at ``send()``, which asserts a dict.
        """
        try:
            message = serializers.loads(payload)
        except Exception as error:
            reason = f"{type(error).__name__}: {error}"
        else:
            if isinstance(message, dict):
                return message
            reason = f"decoded to {type(message).__name__}, not a dict"
        state = self._state()
        state.bad_frames += 1
        now = time.monotonic()
        if state.bad_frame_warned_at is None or now - state.bad_frame_warned_at >= self.drop_log_interval:
            state.bad_frame_warned_at = now
            log.warning(
                "channels_nats: dropped a frame on %s that is not a Channels message "
                "(%s); %d so far. Something other than this layer is publishing to "
                "its subjects.",
                channel,
                reason,
                state.bad_frames,
            )
        return None

    def _sweep_mailboxes(self, state: _LoopState) -> None:
        """Forget the process channels whose consumers have gone.

        Cancellation cannot carry this on its own. An application polling with
        ``wait_for`` cancels a ``receive()`` every timeout and is still there, and
        a consumer that ends on a channel-layer message (``StopConsumer``) leaves
        no pending ``receive()`` to cancel at all -- Channels cancels the task it
        holds, but that task has already completed. So a process channel's mailbox
        goes when it has had no receiver and no traffic for ``mailbox_grace``.

        Only process channels. A plain channel holds a queue-group subscription,
        and keeping that while nobody drains the mailbox takes messages away from
        the processes that would have read them; those go on being dropped on the
        cancelled read (see ``_discard_mailbox``).

        Called from ``_mailbox()``, so any process still receiving keeps sweeping,
        and throttled because that is every ``receive()``.
        """
        now = time.monotonic()
        if now - state.swept_at < self.mailbox_grace / 2:
            return
        state.swept_at = now
        in_a_group = {member for members in state.groups.values() for member in members}
        for channel, box in list(state.mailboxes.items()):
            if "!" not in channel or box.subscription is not None:
                continue  # a plain channel, whose subscription is not ours to drop here
            if box.receivers or channel in in_a_group:
                continue  # still read, or still a group member and so still in use
            if now - box.active_at <= self.mailbox_grace:
                continue
            del state.mailboxes[channel]

    def _wake_receivers(self, box: _Mailbox) -> None:
        """Wake everyone waiting on a mailbox that is being taken away.

        One wake-up per receiver, since each is waiting on its own ``get()``. The
        entry itself is never read as a message: ``receive()`` sees the flag that
        the caller set before it looks at what came off the queue.
        """
        for _ in range(box.receivers):
            box.queue.put_nowait((time.monotonic(), b""))

    def _supersede(self, box: _Mailbox) -> None:
        """Take a mailbox away from its receivers, who move to its replacement."""
        box.superseded = True
        self._wake_receivers(box)

    async def _discard_mailbox(self, channel: str, box: _Mailbox) -> None:
        """Forget a plain channel whose last receiver was cancelled.

        Its subscription is under the channel's queue group, so the server hands
        it a share of that channel's messages. Holding it while nobody reads takes
        those messages away from the processes that would, which is why a plain
        channel is released on the cancelled read rather than waiting out
        ``mailbox_grace`` like a process channel does (see ``_sweep_mailboxes``).

        The cost is the other half of the trade: an application polling a plain
        channel with ``wait_for`` loses what arrives between two polls.
        """
        state = self._state_if_open()
        if state is None:
            return  # close() took the whole state; there is no bookkeeping to undo
        if state.mailboxes.get(channel) is not box:
            return
        if any(channel in members for members in state.groups.values()):
            # A cancelled read is not proof the consumer is gone: an application
            # polling with wait_for cancels one every timeout. A channel still in a
            # group is still in use, and only group_discard or flush takes it out.
            return
        del state.mailboxes[channel]
        # Done before the first await, so a second cancellation cannot half apply it.
        await self._unsubscribe([box.subscription] if box.subscription is not None else [])

    async def new_channel(self, prefix: str = "specific") -> str:
        channel = f"{prefix}.{self._state().client_id}!{uuid.uuid4().hex}"
        await self._mailbox(channel)  # subscribe now so nothing sent before the first receive() is lost
        return channel

    # ------------------------------------------------------------------ groups

    async def group_add(self, group: str, channel: str) -> None:
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)
        state = self._state()
        await self._mailbox(channel)
        # The member goes in before the subscription, so that a message arriving the
        # moment the subscription comes up already has somewhere to go. The cost is
        # that a failure here has to take it back out again.
        members = state.groups.setdefault(group, set())
        members.add(channel)
        try:
            await self._group_subscribe(state, group, channel)
        except BaseException:
            if not self._group_delivers_to(state, group, channel):
                # Nothing feeds this membership, and _discard_mailbox would read it
                # as "still in use" and keep the mailbox for the life of the process.
                # A racing group_add that did succeed shares this very entry, which
                # is why the subscription, not our own bookkeeping, decides.
                members.discard(channel)
                if not members:
                    state.groups.pop(group, None)
            raise

    def _group_delivers_to(self, state: _LoopState, group: str, channel: str) -> bool:
        """Whether a subscription that would feed ``channel`` for ``group`` exists."""
        if "!" in channel:
            return group in state.group_subscriptions
        return (group, channel) in state.group_channel_subscriptions

    async def _group_subscribe(self, state: _LoopState, group: str, channel: str) -> None:
        async with state.subscribe_lock:  # racing group_add calls must share one subscription
            if "!" in channel:
                if group in state.group_subscriptions:
                    return
                client = await self._client()
                subscription = await self._subscribe(
                    client, self.group_subject(group), cb=self._group_deliver(state, group)
                )
                await self._flush_or_undo(client, subscription)
                state.group_subscriptions[group] = subscription
            else:
                # A plain channel read by several processes is still one channel, so
                # take the group's messages for it under the channel's queue group.
                if (group, channel) in state.group_channel_subscriptions:
                    return
                client = await self._client()
                subscription = await self._subscribe(
                    client,
                    self.group_subject(group),
                    queue=self.channel_queue_group(channel),
                    cb=self._group_channel_deliver(state, channel),
                )
                await self._flush_or_undo(client, subscription)
                state.group_channel_subscriptions[group, channel] = subscription

    async def group_discard(self, group: str, channel: str) -> None:
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)
        state = self._state()
        members = state.groups.get(group)
        if members is None:
            return
        members.discard(channel)
        stale = [state.group_channel_subscriptions.pop((group, channel), None)]
        if not members:
            del state.groups[group]
            stale.append(state.group_subscriptions.pop(group, None))
        await self._unsubscribe([s for s in stale if s is not None])

    async def group_send(self, group: str, message: Message) -> None:
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_group_name(group)
        await self._publish(self.group_subject(group), message)

    # ------------------------------------------------------------------ lifecycle

    async def flush(self) -> None:
        """Drop this instance's subscriptions, mailboxes and group memberships.

        This event loop's, and only this instance's. The spec's ``flush`` extension
        asks for a layer that looks empty to every client of a distributed layer,
        which would need a control subject other processes listen on; ``extensions``
        does not claim it. Enough to reset one process between tests, which is what
        it is for.
        """
        state = self._state()
        stale = [
            *state.group_subscriptions.values(),
            *state.group_channel_subscriptions.values(),
            *(box.subscription for box in state.mailboxes.values() if box.subscription is not None),
            *state.process_subscriptions.values(),
        ]
        for box in state.mailboxes.values():
            self._supersede(box)  # a read already waiting must not be left on a dead queue
        state.group_subscriptions.clear()
        state.group_channel_subscriptions.clear()
        state.process_subscriptions.clear()
        state.mailboxes.clear()
        state.groups.clear()
        await self._unsubscribe(stale)

    async def close(self) -> None:
        """Close the connection owned by the current event loop.

        A ``receive()`` already waiting when this runs ends with
        ``ChannelLayerClosed``: its mailbox goes with the state and would never be
        fed again. Later calls are free to open a new connection -- this closes the
        one that exists, it does not retire the layer -- but a process channel from
        before belongs to the old client id and is no longer this loop's to receive.
        """
        state = self._states.pop(asyncio.get_running_loop(), None)
        self._forget_closed_loops()  # shutting one loop down is a fine time to drop the dead ones
        if state is not None:
            for box in state.mailboxes.values():
                box.closed = True  # before the drain, so a slow one cannot hold them there
                self._wake_receivers(box)
        if state is not None:
            for cleanup in list(state.cleanups):
                cleanup.cancel()  # the connection is going; the subscriptions go with it
        if state is not None and state.recovery is not None:
            state.recovery.cancel()
        if state is not None and state.client is not None and not state.client.is_closed:
            try:
                await state.client.drain()
            except Exception as error:
                # Draining flushes what is pending, so a connection that cannot reach
                # the server does not drain -- nats-py runs out its own drain_timeout
                # and flush_timeout and raises. Tearing down must not become an error
                # for the caller, and the connection must not be left open either, so
                # it goes the hard way. How long this takes is nats-py's drain
                # semantics, not ours: measured on a wedged connection at about 50s
                # with its defaults (30 + 10) and about 14s with both lowered to 2 and
                # 1 through `connect_options`.
                log.warning(
                    "channels_nats: could not drain the connection on close (%s); closing it instead. "
                    "Whatever was still pending did not go out.",
                    error,
                )
                try:
                    # Bounded too: closing writes what is left and waits for the socket,
                    # which is the very thing that is stuck. Past the grace the caller
                    # goes on and the connection dies with the loop.
                    await asyncio.wait_for(state.client.close(), CLEANUP_GRACE)
                except (Exception, asyncio.TimeoutError) as closing_error:
                    log.debug("channels_nats: could not close the connection: %s", closing_error)
