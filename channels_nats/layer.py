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
            state = self._states[loop] = _LoopState()
        return state

    async def _client(self) -> Client:
        state = self._state()
        if state.client is None or state.client.is_closed:
            async with state.lock:
                if state.client is None or state.client.is_closed:
                    state.client = await nats.connect(servers=self.servers, **self.connect_options)
        return state.client

    CHANNEL_HEADER = "Channel"

    def channel_subject(self, channel: str) -> str:
        """Subject a message for ``channel`` is published to."""
        if "!" in channel:
            return self.process_subject(channel)
        return f"{self.prefix}.ch.{channel}"

    def process_subject(self, channel: str) -> str:
        """Subject shared by every process-specific channel of one process (``specific.<id>!...``)."""
        return f"{self.prefix}.pc.{channel[: channel.index('!')]}"

    def group_subject(self, group: str) -> str:
        return f"{self.prefix}.grp.{group}"

    def _enqueue(self, box: _Mailbox, channel: str, message: Message) -> None:
        if box.queue.qsize() >= self.get_capacity(channel):
            log.warning("channels_nats: mailbox for %s is full, dropping a message", channel)
            return
        box.queue.put_nowait((time.monotonic(), message))

    async def _mailbox(self, channel: str) -> _Mailbox:
        """Local queue for ``channel``.

        Process-specific channels share the process subscription; a plain
        channel gets a subscription of its own on first use.
        """
        state = self._state()
        box = state.mailboxes.get(channel)
        if box is None:
            box = _Mailbox(queue=asyncio.Queue())
            state.mailboxes[channel] = box
            if "!" in channel:
                await self._process_subscription(channel)
            else:
                client = await self._client()

                async def deliver(msg: t.Any) -> None:
                    self._enqueue(box, channel, self.serializer.loads(msg.data))

                box.subscription = await client.subscribe(self.channel_subject(channel), cb=deliver)
                await client.flush()  # the server knows about the subscription before we return
        return box

    async def _process_subscription(self, channel: str) -> None:
        """One subscription per process prefix, routing by the ``Channel`` header."""
        state = self._state()
        subject = self.process_subject(channel)
        if subject in state.process_subscriptions:
            return
        client = await self._client()

        async def deliver(msg: t.Any) -> None:
            target = (msg.headers or {}).get(self.CHANNEL_HEADER)
            if not isinstance(target, str):
                return
            box = state.mailboxes.get(target)
            if box is not None:
                self._enqueue(box, target, self.serializer.loads(msg.data))

        state.process_subscriptions[subject] = await client.subscribe(subject, cb=deliver)
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
        while True:
            queued_at, message = await box.queue.get()
            if time.monotonic() - queued_at > self.expiry:
                continue
            return message

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

            async def deliver(msg: t.Any) -> None:
                message = self.serializer.loads(msg.data)
                for member in list(state.groups.get(group, ())):
                    box = state.mailboxes.get(member)
                    if box is not None:
                        self._enqueue(box, member, message)

            state.group_subscriptions[group] = await client.subscribe(self.group_subject(group), cb=deliver)
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
                await subscription.unsubscribe()

    async def group_send(self, group: str, message: Message) -> None:
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_group_name(group)
        client = await self._client()
        await client.publish(self.group_subject(group), self.serializer.dumps(message))

    # ------------------------------------------------------------------ lifecycle

    async def flush(self) -> None:
        """Drop this process's subscriptions, mailboxes and group memberships."""
        state = self._state()
        for subscription in list(state.group_subscriptions.values()):
            await subscription.unsubscribe()
        for box in list(state.mailboxes.values()):
            if box.subscription is not None:
                await box.subscription.unsubscribe()
        for subscription in list(state.process_subscriptions.values()):
            await subscription.unsubscribe()
        state.group_subscriptions.clear()
        state.process_subscriptions.clear()
        state.mailboxes.clear()
        state.groups.clear()

    async def close(self) -> None:
        """Close the connection owned by the current event loop."""
        state = self._states.pop(asyncio.get_running_loop(), None)
        if state is not None and state.client is not None and not state.client.is_closed:
            await state.client.drain()
