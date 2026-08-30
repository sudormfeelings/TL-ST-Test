"""Bounded, pre-body recovery policy for Viewer-owned Core cache playback."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from pyrogram.errors import MessageIdInvalid, MessageIdsEmpty

from Backend.core_client import (
    CacheHealthOutcome,
    CacheHealthResult,
    ReplicaAvailability,
    SourceAvailability,
)
from Backend.helper.exceptions import FileNotFound


_DEFINITE_CACHE_LOSS = (FileNotFound, MessageIdInvalid, MessageIdsEmpty)
_RECOVERABLE_OUTCOMES = {
    CacheHealthOutcome.REPLICA_MISSING,
    CacheHealthOutcome.REPLICA_DEGRADED,
    CacheHealthOutcome.REPLICA_INVALID_SHAPE,
}
_BROKEN_AVAILABILITY = {
    ReplicaAvailability.MISSING,
    ReplicaAvailability.DEGRADED,
}


def is_definite_cache_loss(error: BaseException) -> bool:
    return isinstance(error, _DEFINITE_CACHE_LOSS)


def health_allows_recovery(result: CacheHealthResult) -> bool:
    return (
        not result.retryable
        and result.outcome in _RECOVERABLE_OUTCOMES
        and result.resulting_availability in _BROKEN_AVAILABILITY
        and result.source_availability is SourceAvailability.AVAILABLE
    )


def new_recovery_idempotency_key() -> str:
    return f"stream-recovery-{uuid4()}"


@dataclass(slots=True)
class PlaybackRecoveryBudget:
    health_attempted: bool = False
    recovery_attempted: bool = False
    locator_refreshed: bool = False
    playback_retried: bool = False

    def begin_health(self) -> None:
        self._begin("health_attempted")

    def begin_recovery(self) -> None:
        self._begin("recovery_attempted")

    def begin_locator_refresh(self) -> None:
        self._begin("locator_refreshed")

    def begin_playback_retry(self) -> None:
        self._begin("playback_retried")

    def _begin(self, field: str) -> None:
        if getattr(self, field):
            raise RuntimeError("Playback recovery budget exhausted")
        setattr(self, field, True)


def discard_cached_file_properties(streamer, parts) -> None:
    """Force Core playback metadata preflight to observe deleted destinations."""
    cache = getattr(streamer, "_file_id_cache", None)
    if not isinstance(cache, dict):
        return
    for part in parts:
        cache.pop((int(part.chat_id), int(part.msg_id)), None)
