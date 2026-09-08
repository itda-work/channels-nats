"""NATS-backed channel layer for Django Channels."""

from importlib.metadata import PackageNotFoundError, version

from .layer import NatsChannelLayer

try:
    __version__ = version("channels-nats")
except PackageNotFoundError:  # a source tree that was never installed
    __version__ = "unknown"

__all__ = ["NatsChannelLayer", "__version__"]
