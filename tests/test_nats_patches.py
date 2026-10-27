"""The gate on the layer's nats-py patches: which nats-py gets one, and only once."""

import logging

from nats.aio.client import Client

from channels_nats import NatsChannelLayer, _nats_patches


class _Defective:
    """``_process_pong`` exactly as nats-py 2.16.0 has it."""

    async def _process_pong(self) -> None:
        if len(self._pongs) > 0:
            future = self._pongs.pop(0)
            future.set_result(True)
            self._pongs_received += 1
            self._pings_outstanding = 0


class _Fixed:
    """``_process_pong`` as nats-io/nats.py#1031 left it."""

    async def _process_pong(self) -> None:
        if len(self._pongs) > 0:
            future = self._pongs.pop(0)
            if not future.done():
                future.set_result(True)
            self._pongs_received += 1
            self._pings_outstanding = 0


class _Rewritten:
    """A ``_process_pong`` the patch was not written against."""

    async def _process_pong(self) -> None:
        self._pongs.clear()


def _cls(base: type) -> type:
    return type(base.__name__, (base,), dict(base.__dict__))


def test_a_defective_release_is_patched_once():
    cls = _cls(_Defective)
    assert _nats_patches.patch_process_pong(cls, "2.16.0") == "applied"
    assert cls.__dict__["_process_pong"] is _nats_patches._process_pong
    assert _nats_patches.patch_process_pong(cls, "2.16.0") == "already applied"
    assert cls.__dict__["_process_pong"] is _nats_patches._process_pong


def test_an_unknown_version_with_the_defective_code_is_patched():
    cls = _cls(_Defective)
    assert _nats_patches.patch_process_pong(cls, None) == "applied"


def test_the_fixed_code_under_the_same_version_is_left_alone():
    """A build of upstream's main after #1031 still calls itself 2.16.0."""
    cls = _cls(_Fixed)
    original = cls.__dict__["_process_pong"]
    assert _nats_patches.patch_process_pong(cls, "2.16.0").startswith("not needed")
    assert cls.__dict__["_process_pong"] is original


def test_a_later_release_is_left_alone_whatever_its_code():
    for version in ("2.17.0", "2.16.1", "3.0.0rc1"):
        cls = _cls(_Defective)
        original = cls.__dict__["_process_pong"]
        assert _nats_patches.patch_process_pong(cls, version).startswith("not needed"), version
        assert cls.__dict__["_process_pong"] is original


def test_code_the_patch_does_not_know_is_left_alone():
    cls = _cls(_Rewritten)
    original = cls.__dict__["_process_pong"]
    assert _nats_patches.patch_process_pong(cls, "2.16.0").startswith("skipped")
    assert cls.__dict__["_process_pong"] is original


def test_the_installed_nats_py_gets_what_its_code_calls_for():
    """On a release with the defect the patch is on; on a fixed build it is not.

    In this repository's lock (nats-py 2.16.0) the first holds; against a build of
    upstream's c0589b45 the second.
    """
    NatsChannelLayer()
    NatsChannelLayer()  # a second layer in the same process changes nothing
    outcome = _nats_patches._process_pong_outcome
    assert outcome is not None and not outcome.startswith("skipped"), outcome
    patched = Client.__dict__["_process_pong"] is _nats_patches._process_pong
    assert patched == (outcome == "applied"), outcome
    if not patched:
        assert "done" in Client._process_pong.__code__.co_names


def test_the_outcome_is_logged_at_debug(caplog, monkeypatch):
    monkeypatch.setattr(_nats_patches, "_process_pong_outcome", None)
    with caplog.at_level(logging.DEBUG, logger="channels_nats"):
        _nats_patches.apply()
    assert any("nats-py patch for #41" in record.getMessage() for record in caplog.records)
