"""Message encoding for the wire.

msgpack, and only msgpack. Channels messages may carry byte strings, which JSON
cannot represent, and the encoding is shared by every process on a layer: a
setting that has to match everywhere but nothing checks is a way to lose
messages silently, not an option worth offering.
"""

from __future__ import annotations

import typing as t

import msgpack


def dumps(message: dict[str, t.Any]) -> bytes:
    return t.cast(bytes, msgpack.packb(message, use_bin_type=True))


def loads(data: bytes) -> dict[str, t.Any]:
    return t.cast(dict[str, t.Any], msgpack.unpackb(data, raw=False))
