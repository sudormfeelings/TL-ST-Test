from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from .models import (
    CopyFailureReport,
    DestinationManifest,
    DestinationPart,
    PlaybackManifest,
    PlaybackPart,
    SourceManifest,
    SourcePart,
)


LOGGER = logging.getLogger("m0.telegram")
T = TypeVar("T")


class CopyPartialFailure(RuntimeError):
    def __init__(self, report: CopyFailureReport, manifest: DestinationManifest):
        super().__init__(f"COPY_PARTIAL_FAILURE failed_part={report.failed_part}")
        self.report = report
        self.manifest = manifest


class DestinationVerificationError(RuntimeError):
    pass


def _media_from_message(message: Any) -> Any | None:
    return getattr(message, "document", None) or getattr(message, "video", None)


def _message_exists(message: Any) -> bool:
    return message is not None and not bool(getattr(message, "empty", False))


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (OSError, TimeoutError, asyncio.TimeoutError)):
        return True
    return type(exc).__name__ in {
        "FloodWait",
        "InternalServerError",
        "ServiceUnavailable",
        "Timeout",
    }


class TelegramBroker:
    """Small central-bot copy adapter used only by the Milestone 0 spike."""

    def __init__(self, client: Any, *, attempts: int = 3, max_retry_delay_seconds: float = 10):
        if attempts < 1:
            raise ValueError("attempts must be at least one")
        self.client = client
        self.attempts = attempts
        self.max_retry_delay_seconds = max(0.0, max_retry_delay_seconds)

    async def _bounded_call(self, operation: Callable[[], Awaitable[T]], *, label: str) -> T:
        for attempt in range(1, self.attempts + 1):
            try:
                return await operation()
            except Exception as exc:
                if not _is_transient(exc) or attempt >= self.attempts:
                    raise
                flood_seconds = float(getattr(exc, "value", 0) or 0)
                delay = min(max(flood_seconds, 0.25 * (2 ** (attempt - 1))), self.max_retry_delay_seconds)
                LOGGER.warning(
                    "[M0 COPY] transient %s during %s; retry=%s/%s delay=%.2fs",
                    type(exc).__name__,
                    label,
                    attempt,
                    self.attempts - 1,
                    delay,
                )
                await asyncio.sleep(delay)
        raise AssertionError("bounded retry loop terminated unexpectedly")

    async def _validate_source_part(self, part: SourcePart, source_topic_id: int) -> None:
        async def get_message():
            return await self.client.get_messages(part.source_chat_id, part.source_message_id)

        message = await self._bounded_call(get_message, label=f"source lookup part={part.index}")
        if not _message_exists(message):
            raise ValueError(f"source part {part.index} does not exist")
        if _media_from_message(message) is None:
            raise ValueError(f"source part {part.index} is not a document or video")
        actual_topic = getattr(message, "message_thread_id", None)
        if actual_topic != source_topic_id:
            raise ValueError(
                f"source part {part.index} belongs to topic {actual_topic}, expected {source_topic_id}"
            )

    async def copy_part(
        self,
        source_chat_id: int,
        source_message_id: int,
        destination_chat_id: int,
        destination_topic_id: int,
    ) -> int:
        async def copy_message():
            return await self.client.copy_message(
                chat_id=destination_chat_id,
                from_chat_id=source_chat_id,
                message_id=source_message_id,
                message_thread_id=destination_topic_id,
            )

        copied = await self._bounded_call(copy_message, label=f"copy source_msg={source_message_id}")
        destination_message_id = int(getattr(copied, "id", 0) or 0)
        if destination_message_id <= 0:
            raise RuntimeError("Telegram copy returned no destination message ID")
        actual_topic = getattr(copied, "message_thread_id", None)
        if actual_topic != destination_topic_id:
            raise RuntimeError(
                f"copied message landed in topic {actual_topic}, expected {destination_topic_id}"
            )
        return destination_message_id

    async def copy_manifest(
        self,
        source_manifest: SourceManifest,
        *,
        source_topic_id: int,
        destination_chat_id: int,
        destination_topic_id: int,
    ) -> DestinationManifest:
        copied_parts: list[DestinationPart] = []
        for part in source_manifest.parts:
            try:
                await self._validate_source_part(part, source_topic_id)
                destination_message_id = await self.copy_part(
                    source_chat_id=part.source_chat_id,
                    source_message_id=part.source_message_id,
                    destination_chat_id=destination_chat_id,
                    destination_topic_id=destination_topic_id,
                )
                copied_parts.append(
                    DestinationPart(
                        index=part.index,
                        destination_chat_id=destination_chat_id,
                        destination_message_id=destination_message_id,
                    )
                )
                LOGGER.info(
                    "[M0 COPY] source_chat=%s source_msg=%s -> destination_msg=%s part=%s",
                    part.source_chat_id,
                    part.source_message_id,
                    destination_message_id,
                    part.index,
                )
            except Exception as exc:
                report = CopyFailureReport(
                    successful_parts=tuple(item.index for item in copied_parts),
                    failed_part=part.index,
                    error_type=type(exc).__name__,
                )
                partial = DestinationManifest(
                    logical_name=source_manifest.logical_name,
                    destination_topic_id=destination_topic_id,
                    parts=tuple(copied_parts),
                    complete=False,
                    failure=report,
                )
                LOGGER.error(
                    "[M0 COPY] COPY_PARTIAL_FAILURE successful_parts=%s failed_part=%s error_type=%s",
                    list(report.successful_parts),
                    report.failed_part,
                    report.error_type,
                )
                raise CopyPartialFailure(report, partial) from exc

        LOGGER.info("[M0 COPY] completed parts=%s", len(copied_parts))
        return DestinationManifest(
            logical_name=source_manifest.logical_name,
            destination_topic_id=destination_topic_id,
            parts=tuple(copied_parts),
        )


async def verify_destination_manifest(client: Any, manifest: DestinationManifest) -> PlaybackManifest:
    """Resolve destination messages exclusively through the viewer user session."""
    if not manifest.complete:
        raise DestinationVerificationError("destination manifest is incomplete")

    playback_parts: list[PlaybackPart] = []
    for part in manifest.parts:
        try:
            message = await client.get_messages(part.destination_chat_id, part.destination_message_id)
        except Exception as exc:
            raise DestinationVerificationError(
                f"viewer could not resolve destination part {part.index}: {type(exc).__name__}"
            ) from exc
        if not _message_exists(message):
            raise DestinationVerificationError(f"destination part {part.index} does not exist")
        actual_topic = getattr(message, "message_thread_id", None)
        if actual_topic != manifest.destination_topic_id:
            raise DestinationVerificationError(
                f"destination part {part.index} belongs to topic {actual_topic}, "
                f"expected {manifest.destination_topic_id}"
            )
        media = _media_from_message(message)
        if media is None:
            raise DestinationVerificationError(f"destination part {part.index} is not a document or video")
        size = int(getattr(media, "file_size", 0) or 0)
        if size <= 0:
            raise DestinationVerificationError(f"destination part {part.index} has no streamable bytes")
        playback_parts.append(
            PlaybackPart(
                index=part.index,
                chat_id=part.destination_chat_id,
                message_id=part.destination_message_id,
                size=size,
            )
        )
        LOGGER.info("[M0 VERIFY] destination part=%s size=%s", part.index, size)

    return PlaybackManifest(logical_name=manifest.logical_name, parts=tuple(playback_parts))
