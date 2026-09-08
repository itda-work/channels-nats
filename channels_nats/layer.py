"""``NatsChannelLayer``: Django Channels on top of a NATS server.

Subjects are the contract. Anything that speaks NATS (another Python worker,
a Go front, a CLI) can join the same layer by publishing to them:

- ``<prefix>.pc.<process>``  one message for one process-specific channel
  (``send`` to ``specific.<process>!<id>``); the full channel name travels in
  the ``Channel`` header and the receiving process routes it locally
- ``<prefix>.ch.<channel>``  one message for a plain (shared-name) channel
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
from channels.layers import BaseChannelLayer
from nats.aio.client import Client
from nats.aio.subscription import Subscription

from .serializers import Serializer, get_serializer

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
    mailboxes: dict[str, _Mailbox] = field(default_factory=dict)
    groups: dict[str, set[str]] = field(default_factory=dict)
    group_subscriptions: dict[str, Subscription] = field(default_factory=dict)
    process_subscriptions: dict[str, Subscription] = field(default_factory=dict)


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

    #: A full mailbox stays full, so warn about the first drop and then at most
    #: this often, rather than once per message.
    drop_log_interval = 60.0

    def __init__(
        self,
        servers: str | t.Sequence[str] = "nats://127.0.0.1:4222",
        prefix: str = "channels",
        expiry: int = 60,
        group_expiry: int = 86400,
        capacity: int = 100,
        channel_capacity: t.Any = None,
        serializer: str | Serializer = "json",
        connect_options: dict[str, t.Any] | None = None,
    ) -> None:
        super().__init__(expiry=expiry, capacity=capacity, channel_capacity=channel_capacity)
        self.servers = [servers] if isinstance(servers, str) else list(servers)
        self.prefix = prefix
        self.group_expiry = group_expiry
        self.serializer = get_serializer(serializer)
        self.connect_options = dict(connect_options or {})
        self.client_id = uuid.uuid4().hex[:12]
        self._states: dict[asyncio.AbstractEventLoop, _LoopState] = {}

    # ------------------------------------------------------------------ plumbing

    def _state(self) -> _LoopState:
        loop = asyncio.get_running_loop()
        state = self._states.get(loop)
        if state is None:
            # A new loop is rare, so this is the cheap moment to forget the loops
            # that have since been closed -- their connections died with them, and
            # a closed loop can still be referenced elsewhere, so waiting for the
            # garbage collector would not do.
            for closed in [old for old in self._states if old.is_closed()]:
                del self._states[closed]
            state = self._states[loop] = _LoopState()
        return state

    async def _client(self) -> Client:
        state = self._state()
        if state.client is None or state.client.is_closed:
            async with state.lock:
                if state.client is None or state.client.is_closed:
                    stale = state.client is not None
                    state.client = await nats.connect(servers=self.servers, **self.connect_options)
                    if stale:
                        await self._resubscribe(state, state.client)
        return state.client

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
                    self.channel_subject(channel), cb=self._channel_deliver(box, channel)
                )
        for group in list(state.group_subscriptions):
            state.group_subscriptions[group] = await client.subscribe(
                self.group_subject(group), cb=self._group_deliver(state, group)
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

    def group_subject(self, group: str) -> str:
        return f"{self.prefix}.grp.{group}"

    def _channel_deliver(self, box: _Mailbox, channel: str) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            self._enqueue(box, channel, self.serializer.loads(msg.data))

        return deliver

    def _process_deliver(self, state: _LoopState) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            target = (msg.headers or {}).get(self.CHANNEL_HEADER)
            if not isinstance(target, str):
                return
            box = state.mailboxes.get(target)
            if box is not None:
                self._enqueue(box, target, self.serializer.loads(msg.data))

        return deliver

    def _group_deliver(self, state: _LoopState, group: str) -> t.Callable[[t.Any], t.Awaitable[None]]:
        async def deliver(msg: t.Any) -> None:
            message = self.serializer.loads(msg.data)
            for member in list(state.groups.get(group, ())):
                box = state.mailboxes.get(member)
                if box is not None:
                    self._enqueue(box, member, message)

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
        if box is None:
            box = _Mailbox(queue=asyncio.Queue())
            state.mailboxes[channel] = box
            try:
                if "!" in channel:
                    await self._process_subscription(channel)
                else:
                    client = await self._client()
                    box.subscription = await client.subscribe(
                        self.channel_subject(channel), cb=self._channel_deliver(box, channel)
                    )
                    await client.flush()  # the server knows about the subscription before we return
            except BaseException:  # a cancelled subscribe must not leave a mailbox nothing feeds
                if state.mailboxes.get(channel) is box:
                    del state.mailboxes[channel]
                raise
        return box

    async def _process_subscription(self, channel: str) -> None:
        """One subscription per process prefix, routing by the ``Channel`` header."""
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
        client = await self._client()
        headers = {self.CHANNEL_HEADER: channel} if "!" in channel else None
        await client.publish(self.channel_subject(channel), self.serializer.dumps(message), headers=headers)

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
        if group not in state.group_subscriptions:
            client = await self._client()
            state.group_subscriptions[group] = await client.subscribe(
                self.group_subject(group), cb=self._group_deliver(state, group)
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
        if not members:
            del state.groups[group]
            subscription = state.group_subscriptions.pop(group, None)
            if subscription is not None:
                await self._unsubscribe([subscription])

    async def group_send(self, group: str, message: Message) -> None:
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_group_name(group)
        client = await self._client()
        await client.publish(self.group_subject(group), self.serializer.dumps(message))

    # ------------------------------------------------------------------ lifecycle

    async def flush(self) -> None:
        """Drop this process's subscriptions, mailboxes and group memberships."""
        state = self._state()
        stale = [
            *state.group_subscriptions.values(),
            *(box.subscription for box in state.mailboxes.values() if box.subscription is not None),
            *state.process_subscriptions.values(),
        ]
        state.group_subscriptions.clear()
        state.process_subscriptions.clear()
        state.mailboxes.clear()
        state.groups.clear()
        await self._unsubscribe(stale)

    async def close(self) -> None:
        """Close the connection owned by the current event loop."""
        state = self._states.pop(asyncio.get_running_loop(), None)
        if state is not None and state.client is not None and not state.client.is_closed:
            await state.client.drain()
