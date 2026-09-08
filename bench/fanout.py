"""Fan-out benchmark: one ``group_send``, N member channels spread over P processes.

    uv run python -m bench.fanout --members 1000 --processes 4 --messages 50

Reports end-to-end latency from publish to each member's ``receive`` and the
delivery throughput, next to the in-memory layer (single process) as a floor.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def find_nats_server() -> str | None:
    for candidate in (
        os.environ.get("NATS_SERVER"),
        shutil.which("nats-server"),
        str(Path.home() / "go/bin/nats-server"),
    ):
        if candidate and Path(candidate).exists():
            return candidate
    return None


def describe_revision() -> str | None:
    """What tree this ran from. `-dirty` is the tell that a file was swapped in."""
    return _run(["git", "describe", "--tags", "--always", "--dirty"], cwd=ROOT)


def server_version(binary: str) -> str | None:
    return _run([binary, "--version"])


def _run(command: list[str], cwd: Path | None = None) -> str | None:
    try:
        finished = subprocess.run(command, cwd=cwd, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return finished.stdout.strip() or None if finished.returncode == 0 else None


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_port(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("nats-server did not start")


async def worker(url: str, group: str, members: int, messages: int, outfile: str, readyfile: str) -> None:
    from channels_nats import NatsChannelLayer

    layer = NatsChannelLayer(servers=url)
    channels = [await layer.new_channel() for _ in range(members)]
    for channel in channels:
        await layer.group_add(group, channel)
    Path(readyfile).touch()

    latencies: list[float] = []

    async def drain(channel: str) -> None:
        for _ in range(messages):
            message = await layer.receive(channel)
            latencies.append(time.time() - message["ts"])

    await asyncio.gather(*(drain(channel) for channel in channels))
    Path(outfile).write_text(json.dumps({"latencies": latencies, "done": time.time()}))
    await layer.close()


async def publish(url: str, group: str, messages: int, gap: float) -> float:
    from channels_nats import NatsChannelLayer

    layer = NatsChannelLayer(servers=url)
    started = time.time()
    for i in range(messages):
        await layer.group_send(group, {"type": "bench", "ts": time.time(), "i": i})
        await asyncio.sleep(gap)
    await layer.close()
    return started


async def in_memory(members: int, messages: int) -> dict:
    from channels.layers import InMemoryChannelLayer

    layer = InMemoryChannelLayer()
    channels = [await layer.new_channel() for _ in range(members)]
    for channel in channels:
        await layer.group_add("bench", channel)
    started = time.perf_counter()
    for i in range(messages):
        await layer.group_send("bench", {"type": "bench", "i": i})
        for channel in channels:
            await layer.receive(channel)
    elapsed = time.perf_counter() - started
    return {"group_send_ms": elapsed * 1000 / messages, "deliveries_per_s": members * messages / elapsed}


def percentile(values: list[float], p: float) -> float:
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--members", type=int, default=1000)
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--messages", type=int, default=50)
    parser.add_argument("--gap", type=float, default=0.01, help="seconds between publishes")
    parser.add_argument("--url", help="use a running nats-server instead of starting one")
    parser.add_argument("--worker", nargs=6, metavar=("URL", "GROUP", "MEMBERS", "MESSAGES", "OUT", "READY"))
    args = parser.parse_args(argv)

    if args.worker:
        url, group, members, messages, out, ready = args.worker
        asyncio.run(worker(url, group, int(members), int(messages), out, ready))
        return 0

    server = None
    server_release = None
    url = args.url
    if url is None:
        binary = find_nats_server()
        if binary is None:
            print("nats-server not found: set NATS_SERVER or --url", file=sys.stderr)
            return 2
        port = free_port()
        server = subprocess.Popen(
            [binary, "-a", "127.0.0.1", "-p", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        wait_for_port(port)
        server_release = server_version(binary)
        url = f"nats://127.0.0.1:{port}"

    tmp = Path(tempfile.mkdtemp(prefix="channels-nats-bench-"))
    per_process = args.members // args.processes
    procs, outs, readies = [], [], []
    try:
        for i in range(args.processes):
            out, ready = tmp / f"out-{i}.json", tmp / f"ready-{i}"
            outs.append(out)
            readies.append(ready)
            cmd = [
                sys.executable,
                "-m",
                "bench.fanout",
                "--worker",
                url,
                "bench",
                str(per_process),
                str(args.messages),
                str(out),
                str(ready),
            ]
            procs.append(subprocess.Popen(cmd, cwd=ROOT))
        deadline = time.time() + 120
        while not all(r.exists() for r in readies):
            if time.time() > deadline:
                raise RuntimeError("workers did not subscribe in time")
            time.sleep(0.05)

        started = asyncio.run(publish(url, "bench", args.messages, args.gap))
        for proc in procs:
            proc.wait(timeout=120)
        latencies: list[float] = []
        done = 0.0
        for out in outs:
            data = json.loads(out.read_text())
            latencies += data["latencies"]
            done = max(done, data["done"])
        deliveries = per_process * args.processes * args.messages
        wall = done - started
        nats_result = {
            "members": per_process * args.processes,
            "processes": args.processes,
            "messages": args.messages,
            "deliveries": deliveries,
            "delivered": len(latencies),
            "p50_ms": percentile(latencies, 50) * 1000,
            "p95_ms": percentile(latencies, 95) * 1000,
            "p99_ms": percentile(latencies, 99) * 1000,
            "max_ms": max(latencies) * 1000,
            "deliveries_per_s": len(latencies) / wall if wall > 0 else float("nan"),
            "wall_s": wall,
        }
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
        if server is not None:
            server.terminate()
            server.wait(timeout=5)

    memory_result = asyncio.run(in_memory(nats_result["members"], args.messages))

    import channels_nats

    measured = {
        "channels_nats": channels_nats.__version__,
        "revision": describe_revision() or "no git",
        "python": sys.version.split()[0],
        "nats_server": server_release,
    }

    n = nats_result
    print(f"NATS fan-out: {n['members']} members over {args.processes} processes, {args.messages} group_send")
    print(
        f"  channels-nats {measured['channels_nats']} ({measured['revision']}), "
        f"Python {measured['python']}, {measured['nats_server'] or 'server not started here'}"
    )
    print(
        f"  delivered {n['delivered']}/{n['deliveries']}  p50 {n['p50_ms']:.2f} ms  "
        f"p95 {n['p95_ms']:.2f} ms  p99 {n['p99_ms']:.2f} ms  max {n['max_ms']:.1f} ms"
    )
    print(f"  {n['deliveries_per_s']:,.0f} deliveries/s end to end (publish gap {args.gap * 1000:.0f} ms)")
    print(
        f"InMemory (1 process): group_send {memory_result['group_send_ms']:.2f} ms for {n['members']} members, "
        f"{memory_result['deliveries_per_s']:,.0f} deliveries/s"
    )

    results_dir = ROOT / "bench" / "results"
    results_dir.mkdir(exist_ok=True)
    path = results_dir / f"fanout-{time.strftime('%Y%m%d-%H%M%S')}.json"
    path.write_text(
        json.dumps(
            {
                "nats": nats_result,
                "in_memory": memory_result,
                "measured": measured,
            },
            indent=1,
        )
    )
    print(f"written: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
