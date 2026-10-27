"""Fixes for nats-py defects that the layer cannot wait out, each gated to the
releases that have them.

A patch here replaces one nats-py method with upstream's own fix, and only where
two things hold at once: the installed nats-py is no newer than the last release
known to have the defect, and the method is still the exact code the patch was
written against. The version alone cannot say it -- a build of upstream's main
reports the same version as the release before it -- and the shape alone would
keep patching a release that changed the method for other reasons.

Each patch says where it goes and when to take it out.
"""

from __future__ import annotations

import logging
import re
from importlib.metadata import PackageNotFoundError, version

from nats.aio.client import Client

log = logging.getLogger("channels_nats")

#: The last nats-py release whose ``_process_pong`` has the defect (#41). Every
#: release up to it calls ``set_result()`` unconditionally; nats-io/nats.py#1031
#: (merge commit c0589b45, 2026-10-01) landed after its tag.
_PROCESS_PONG_DEFECTIVE_UP_TO = (2, 16, 0)

#: The names ``Client._process_pong`` uses in nats-py 2.16.0. Upstream's fix adds
#: ``done``; anything else means the method changed under us and is left alone.
_PROCESS_PONG_DEFECTIVE_NAMES = ("len", "_pongs", "pop", "set_result", "_pongs_received", "_pings_outstanding")

_MARK = "_channels_nats_patch"

_process_pong_outcome: str | None = None


def _release(text: str) -> tuple[int, ...] | None:
    """``2.16.0`` -> ``(2, 16, 0)``. A pre-release of the next version counts as
    that version: it is cut from a main that has the fix."""
    found = re.match(r"(\d+)\.(\d+)\.(\d+)", text)
    return tuple(int(part) for part in found.groups()) if found else None


def _installed_nats_py() -> str | None:
    try:
        return version("nats-py")
    except PackageNotFoundError:  # a source checkout on sys.path
        return None


async def _process_pong(self: Client) -> None:
    """nats-py's ``_process_pong`` with nats-io/nats.py#1031 applied.

    ``flush()`` puts a future on ``_pongs`` and waits for it. Cancelled -- by its
    caller, by its own ``timeout``, or by the layer's ``publish_deadline`` -- that
    future stays on the list, already done; the PONG that comes for it later made
    ``set_result()`` raise ``InvalidStateError``, and the read loop broke on it
    with no reconnect and no close (#41). The future is still popped: PONGs answer
    PINGs in order, and taking a cancelled one off the list instead would hand its
    PONG to the next flush, which would return before the server had seen what it
    was flushing (reproduced, #41).

    Remove when the nats-py floor in ``pyproject.toml`` is past 2.16.0.
    """
    if len(self._pongs) > 0:
        future = self._pongs.pop(0)
        if not future.done():
            future.set_result(True)
        self._pongs_received += 1
        self._pings_outstanding = 0


setattr(_process_pong, _MARK, True)


def patch_process_pong(client_cls: type = Client, installed: str | None = None) -> str:
    """Put upstream's ``_process_pong`` fix on ``client_cls`` if it needs it; say what was done.

    Idempotent: a class that already carries the patch is left as it is.
    """
    current = client_cls.__dict__.get("_process_pong")
    if current is None:
        return "skipped: no _process_pong"
    if getattr(current, _MARK, False):
        return "already applied"
    release = _release(installed) if installed is not None else None
    installed = installed or "(version unknown)"
    if release is not None and release > _PROCESS_PONG_DEFECTIVE_UP_TO:
        return f"not needed: nats-py {installed} is past {'.'.join(map(str, _PROCESS_PONG_DEFECTIVE_UP_TO))}"
    names = getattr(getattr(current, "__code__", None), "co_names", None)
    if names != _PROCESS_PONG_DEFECTIVE_NAMES:
        if names is not None and "done" in names:
            return f"not needed: nats-py {installed}'s _process_pong already checks future.done()"
        return f"skipped: nats-py {installed}'s _process_pong is not the code this patch replaces"
    client_cls._process_pong = _process_pong  # type: ignore[attr-defined]
    return "applied"


def apply() -> None:
    """Apply every patch to nats-py's ``Client`` once per process.

    Called from ``NatsChannelLayer.__init__``. The outcome goes to the
    ``channels_nats`` logger at debug, or as a warning when nats-py is in the
    defective range but its code is not what the patch expects -- then the defect
    may be there and nothing covers it.
    """
    global _process_pong_outcome
    if _process_pong_outcome is not None:
        return
    installed = _installed_nats_py()
    _process_pong_outcome = patch_process_pong(Client, installed)
    if _process_pong_outcome.startswith("skipped"):
        log.warning("channels_nats: nats-py patch for #41 %s", _process_pong_outcome)
    else:
        log.debug("channels_nats: nats-py patch for #41 %s (nats-py %s)", _process_pong_outcome, installed)
