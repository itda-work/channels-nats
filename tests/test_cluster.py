"""The layer against a three-node cluster, as documented in README's 클러스터로 확장.

Nothing in the layer knows about clustering -- only ``servers`` grows -- so what
these check is that the claim holds: subjects route between nodes, and a process
whose node dies comes back on another one.
"""

import asyncio
import time

import pytest

pytestmark = pytest.mark.integration

FAST_RECONNECT = {"reconnect_time_wait": 0.1, "max_reconnect_attempts": -1, "dont_randomize": True}


async def publish_until_it_lands(receive, publish, timeout: float = 30.0):
    """Publish until the message arrives, and return it.

    `client.flush()` only means the node the subscriber is on knows about the
    subscription. Carrying that interest over a route to the publisher's node is
    asynchronous, and NATS stores nothing, so a single publish can fall into that
    window and be dropped -- as it did on Windows CI. An application in a cluster
    faces the same window; publishing once and hoping is what is wrong, not the
    delivery.
    """
    receiving = asyncio.create_task(receive())
    deadline = time.monotonic() + timeout
    while True:
        await publish()
        try:
            return await asyncio.wait_for(asyncio.shield(receiving), 0.5)
        except asyncio.TimeoutError:
            if time.monotonic() > deadline:
                receiving.cancel()
                pytest.fail("nothing arrived before the deadline")


async def test_a_group_send_crosses_nodes(nats_cluster, make_layer):
    """The publisher and the members are on different nodes; the servers route it."""
    worker = make_layer(servers=[nats_cluster.urls[0]])
    publisher = make_layer(servers=[nats_cluster.urls[2]])
    channel = await worker.new_channel()
    await worker.group_add("room", channel)

    delivered = await publish_until_it_lands(
        lambda: worker.receive(channel),
        lambda: publisher.group_send("room", {"type": "chat", "n": 1}),
    )
    assert delivered == {"type": "chat", "n": 1}


async def test_a_process_channel_crosses_nodes(nats_cluster, make_layer):
    owner = make_layer(servers=[nats_cluster.urls[1]])
    sender = make_layer(servers=[nats_cluster.urls[2]])
    channel = await owner.new_channel()

    delivered = await publish_until_it_lands(
        lambda: owner.receive(channel),
        lambda: sender.send(channel, {"type": "direct"}),
    )
    assert delivered == {"type": "direct"}


async def test_a_worker_survives_losing_its_node(nats_cluster, make_layer):
    """What README promises: the client fails over and the layer keeps working."""
    worker = make_layer(servers=nats_cluster.urls, connect_options=FAST_RECONNECT)
    publisher = make_layer(servers=[nats_cluster.urls[2]])
    channel = await worker.new_channel()
    await worker.group_add("room", channel)
    before = await publish_until_it_lands(
        lambda: worker.receive(channel),
        lambda: publisher.group_send("room", {"type": "before"}),
    )
    assert before == {"type": "before"}

    connected = worker._state().client.connected_url
    assert connected is not None and nats_cluster.urls[0].endswith(f":{connected.port}")

    nats_cluster.stop(0)  # the node this worker is connected to

    after = await publish_until_it_lands(
        lambda: worker.receive(channel),
        lambda: publisher.group_send("room", {"type": "after"}),
    )
    assert after == {"type": "after"}
    moved_to = worker._state().client.connected_url
    assert moved_to is not None and moved_to.port != connected.port
