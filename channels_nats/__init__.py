"""NATS-backed channel layer for Django Channels."""

from .layer import NatsChannelLayer

__version__ = "0.1.0"
__all__ = ["NatsChannelLayer", "__version__"]
