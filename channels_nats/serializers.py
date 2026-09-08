"""Message serializers. JSON by default; msgpack when bytes must survive."""

from __future__ import annotations

import json
import typing as t


class JSONSerializer:
    name = "json"

    def dumps(self, message: dict[str, t.Any]) -> bytes:
        return json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    def loads(self, data: bytes) -> dict[str, t.Any]:
        return json.loads(data)


class MsgPackSerializer:
    name = "msgpack"

    def __init__(self) -> None:
        try:
            import msgpack
        except ImportError as e:  # pragma: no cover
            raise ImportError(
                "serializer='msgpack' needs the msgpack package (pip install channels-nats[msgpack])"
            ) from e
        self._msgpack = msgpack

    def dumps(self, message: dict[str, t.Any]) -> bytes:
        return t.cast(bytes, self._msgpack.packb(message, use_bin_type=True))

    def loads(self, data: bytes) -> dict[str, t.Any]:
        return self._msgpack.unpackb(data, raw=False)


Serializer = t.Union[JSONSerializer, MsgPackSerializer]


def get_serializer(spec: str | Serializer) -> Serializer:
    if not isinstance(spec, str):
        return spec
    if spec == "json":
        return JSONSerializer()
    if spec == "msgpack":
        return MsgPackSerializer()
    raise ValueError(f"unknown serializer {spec!r}; use 'json' or 'msgpack'")
