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
- Messages older than ``expiry`` seconds are dropped on ``receive``; a
  mailbox holding ``capacity`` messages drops new ones (Channels' ``ChannelFull``
  cannot be raised on the sender's side over pub/sub).
- Messages published before a channel's first ``receive()`` (or ``new_channel()``)
  are lost: there is no subscriber yet. Consumers always subscribe on connect,
  so this only matters for ad-hoc channel names.
- A mailbox lives until its last ``receive()`` is cancelled, which is how a
  consumer's disconnect reaches the layer; then the mailbox and, for a plain
  channel, its subscription go away.
- If nats-py gives up reconnecting and closes the client, the next call opens a
  new connection and restores this loop's subscriptions on it.

Connections are per event loop, so ``async_to_sync(layer.group_send)`` from
Django signals or views works alongside the consumers' loop.
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

from . import serializers

log = logging.getLogger("channels_nats")

Message = dict[str, t.Any]


@dataclass
class _Mailbox:
    queue: asyncio.Queue[tuple[float, Message]]
    subscription: Subscription | None = None
    receivers: int = 0
    dropped: int = 0
    warned_at: float | None = None


@dataclass
class _LoopState:
    """Everything bound to one asyncio event loop."""

    client: Client | None = None
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

    extensions = ["groups", "flush"]

    #: The spec asks a layer to carry these. ``ChannelFull`` is never raised here:
    #: over pub/sub a sender cannot see the receiver's queue (see the README).
    MessageTooLarge = MessageTooLarge
    ChannelFull = ChannelFull

    #: A full mailbox stays full, so warn about the first drop and then at most
    #: this often, rather than once per message.
    drop_log_interval = 60.0

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
        self.client_id = uuid.uuid4().hex[:12]
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

    def _forget_closed_loops(self) -> None:
        for closed in [loop for loop in self._states if loop.is_closed()]:
            del self._states[closed]

    async def _client(self) -> Client:
        state = self._state()
        if state.client is None or state.client.is_closed:
            async with state.lock:
                if state.client is None or state.client.is_closed:
                    stale = state.client is not None
                    state.client = await self._connect(state)
                    if stale:
                        await self._resubscribe(state, state.client)
        return state.client

    async def _connect(self, state: _LoopState) -> Client:
        options = dict(self.connect_options)
        given = options.pop("closed_cb", None)

        async def on_closed() -> None:
            if given is not None:
                await given()
            self._start_recovery(state)

        return await nats.connect(servers=self.servers, closed_cb=on_closed, **options)

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
        for subject in list(state.process_subscriptions):
            state.process_subscriptions[subject] = await client.subscribe(subject, cb=self._process_deliver(state))
        for channel, box in state.mailboxes.items():
            if box.subscription is not None:
                box.subscription = await client.subscribe(
                    self.channel_subject(channel),
                    queue=self.channel_queue_group(channel),
                    cb=self._channel_deliver(box, channel),
                )
        for group in list(state.group_subscriptions):
            state.group_subscriptions[group] = await client.subscribe(
                self.group_subject(group), cb=self._group_deliver(state, group)
            )
        for group, channel in list(state.group_channel_subscriptions):
            state.group_channel_subscriptions[group, channel] = await client.subscribe(
                self.group_subject(group),
                queue=self.channel_queue_group(channel),
                cb=self._group_channel_deliver(state, channel),
            )
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
        """Whether a process-specific channel was created by this layer instance."""
        return channel[: channel.index("!")].rsplit(".", 1)[-1] == self.client_id

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
            self._enqueue(box, channel, serializers.loads(msg.data))

        return deliver

    def _process_deliver(self, state: _LoopState) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            target = (msg.headers or {}).get(self.CHANNEL_HEADER)
            if not isinstance(target, str):
                return
            box = state.mailboxes.get(target)
            if box is not None:
                self._enqueue(box, target, serializers.loads(msg.data))

        return deliver

    def _group_deliver(self, state: _LoopState, group: str) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            message = serializers.loads(msg.data)
            first = True
            for member in list(state.groups.get(group, ())):
                if "!" not in member:
                    continue  # plain channels come in on their own queue subscription
                box = state.mailboxes.get(member)
                if box is None:
                    continue
                # Every member gets its own object. Handing one dict to several
                # consumers lets one of them edit what the others receive.
                self._enqueue(box, member, message if first else serializers.loads(msg.data))
                first = False

        return deliver

    async def _unsubscribe(self, subscriptions: t.Iterable[Subscription]) -> None:
        """Drop subscriptions, tolerating a connection that has already gone.

        Every caller has finished its bookkeeping by now, so a connection that
        died must not turn tearing down into an error.
        """
        for subscription in subscriptions:
            try:
                await subscription.unsubscribe()
            except Exception as error:
                log.debug("channels_nats: could not unsubscribe: %s", error)

    def _group_channel_deliver(self, state: _LoopState, channel: str) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            box = state.mailboxes.get(channel)
            if box is not None:
                self._enqueue(box, channel, serializers.loads(msg.data))

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
        await client.publish(subject, payload, headers=headers)

    def _enqueue(self, box: _Mailbox, channel: str, message: Message) -> None:
        now = time.monotonic()
        if box.queue.qsize() >= self.get_capacity(channel):
            box.dropped += 1
            if box.warned_at is None or now - box.warned_at >= self.drop_log_interval:
                log.warning(
                    "channels_nats: mailbox for %s is full (capacity %d); %d message(s) dropped so far",
                    channel,
                    self.get_capacity(channel),
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
        box.queue.put_nowait((now, message))

    async def _mailbox(self, channel: str) -> _Mailbox:
        """Local queue for ``channel``.

        Process-specific channels share the process subscription; a plain
        channel gets a subscription of its own on first use.
        """
        if "!" in channel and not self.owns_channel(channel):
            # Subscribing would put us on the owner's process subject, so every message
            # for its channels would be delivered twice: once there and once here.
            raise ValueError(
                f"{channel} belongs to another process; a process can only receive on "
                "the channels its own new_channel() handed out"
            )
        state = self._state()
        box = state.mailboxes.get(channel)
        if box is not None:
            return box
        # Subscribing takes a round trip, and connecting takes several. Without this
        # lock, consumers arriving together each get past the check above and each
        # subscribe, so one message arrives once per racing caller.
        async with state.subscribe_lock:
            box = state.mailboxes.get(channel)
            if box is not None:  # somebody won the race while we waited
                return box
            box = _Mailbox(queue=asyncio.Queue())
            state.mailboxes[channel] = box
            try:
                if "!" in channel:
                    await self._process_subscription(channel)
                else:
                    client = await self._client()
                    box.subscription = await client.subscribe(
                        self.channel_subject(channel),
                        queue=self.channel_queue_group(channel),
                        cb=self._channel_deliver(box, channel),
                    )
                    await client.flush()  # the server knows about the subscription before we return
            except BaseException:  # a cancelled subscribe must not leave a mailbox nothing feeds
                if state.mailboxes.get(channel) is box:
                    del state.mailboxes[channel]
                if box.subscription is not None:
                    await self._unsubscribe([box.subscription])
                raise
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
        state.process_subscriptions[subject] = await client.subscribe(subject, cb=self._process_deliver(state))
        await client.flush()

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
                queued_at, message = await box.queue.get()
                if time.monotonic() - queued_at > self.expiry:
                    continue
                return message
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            box.receivers -= 1
            if cancelled and box.receivers == 0:
                await self._discard_mailbox(channel, box)

    async def _discard_mailbox(self, channel: str, box: _Mailbox) -> None:
        """Forget a channel whose last receiver was cancelled.

        A consumer that goes away leaves its pending ``receive()`` cancelled, and
        that is the only signal the Channels API gives us that a channel is over.
        Without this the mailbox, and for a plain channel its subscription, would
        live as long as the process: memory grows with the number of connections
        the process has ever served, not with the ones it is serving.
        """
        state = self._state()
        if state.mailboxes.get(channel) is not box:
            return
        del state.mailboxes[channel]
        stale = [box.subscription] if box.subscription is not None else []
        for group in [g for g, members in state.groups.items() if channel in members]:
            members = state.groups[group]
            members.discard(channel)
            gone = state.group_channel_subscriptions.pop((group, channel), None)
            if gone is not None:
                stale.append(gone)
            if not members:
                del state.groups[group]
                subscription = state.group_subscriptions.pop(group, None)
                if subscription is not None:
                    stale.append(subscription)
        # The bookkeeping above is done before the first await, so a second
        # cancellation cannot leave this half applied.
        await self._unsubscribe(stale)

    async def new_channel(self, prefix: str = "specific") -> str:
        channel = f"{prefix}.{self.client_id}!{uuid.uuid4().hex}"
        await self._mailbox(channel)  # subscribe now so nothing sent before the first receive() is lost
        return channel

    # ------------------------------------------------------------------ groups

    async def group_add(self, group: str, channel: str) -> None:
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)
        state = self._state()
        await self._mailbox(channel)
        state.groups.setdefault(group, set()).add(channel)
        async with state.subscribe_lock:  # racing group_add calls must share one subscription
            if "!" in channel:
                if group in state.group_subscriptions:
                    return
                client = await self._client()
                state.group_subscriptions[group] = await client.subscribe(
                    self.group_subject(group), cb=self._group_deliver(state, group)
                )
            else:
                # A plain channel read by several processes is still one channel, so
                # take the group's messages for it under the channel's queue group.
                if (group, channel) in state.group_channel_subscriptions:
                    return
                client = await self._client()
                state.group_channel_subscriptions[group, channel] = await client.subscribe(
                    self.group_subject(group),
                    queue=self.channel_queue_group(channel),
                    cb=self._group_channel_deliver(state, channel),
                )
            await client.flush()

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
        """Drop this process's subscriptions, mailboxes and group memberships."""
        state = self._state()
        stale = [
            *state.group_subscriptions.values(),
            *state.group_channel_subscriptions.values(),
            *(box.subscription for box in state.mailboxes.values() if box.subscription is not None),
            *state.process_subscriptions.values(),
        ]
        state.group_subscriptions.clear()
        state.group_channel_subscriptions.clear()
        state.process_subscriptions.clear()
        state.mailboxes.clear()
        state.groups.clear()
        await self._unsubscribe(stale)

    async def close(self) -> None:
        """Close the connection owned by the current event loop."""
        state = self._states.pop(asyncio.get_running_loop(), None)
        self._forget_closed_loops()  # shutting one loop down is a fine time to drop the dead ones
        if state is not None and state.recovery is not None:
            state.recovery.cancel()
        if state is not None and state.client is not None and not state.client.is_closed:
            await state.client.drain()
