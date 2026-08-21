"""Typed integration seam between TL-Core cache locators and local media playback."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit
from uuid import UUID

from Backend.config import TLCore
from Backend.core_client import TLCoreClient, ValidatedCacheLocator


class CorePlaybackNotConfigured(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CoreVirtualPart:
    part_number: int
    chat_id: int
    msg_id: int
    size_bytes: int


@dataclass(frozen=True, slots=True)
class VirtualMediaDescriptor:
    source_id: UUID
    cache_replica_id: UUID
    chat_id: int
    thread_id: int
    total_size_bytes: int
    parts: tuple[CoreVirtualPart, ...]

    def as_resolver_payload(self) -> list[dict[str, int]]:
        return [
            {
                "chat_id": part.chat_id,
                "msg_id": part.msg_id,
                "part_number": part.part_number,
                "size_bytes": part.size_bytes,
            }
            for part in self.parts
        ]


def locator_to_virtual_media(locator: ValidatedCacheLocator) -> VirtualMediaDescriptor:
    ordered = tuple(sorted(locator.parts, key=lambda part: part.part_number))
    if locator.chat_id == 0 or locator.thread_id <= 0 or locator.total_size_bytes <= 0:
        raise ValueError("Invalid Viewer Cache topology")
    if [part.part_number for part in ordered] != list(range(1, len(ordered) + 1)):
        raise ValueError("Invalid Viewer Cache part ordering")
    if not ordered:
        raise ValueError("Viewer Cache locator has no parts")
    if any(part.telegram_message_id <= 0 or part.size_bytes <= 0 for part in ordered):
        raise ValueError("Invalid Viewer Cache part")
    if len({part.telegram_message_id for part in ordered}) != len(ordered):
        raise ValueError("Duplicate Viewer Cache message")
    if sum(part.size_bytes for part in ordered) != locator.total_size_bytes:
        raise ValueError("Viewer Cache size mismatch")

    parts = tuple(
        CoreVirtualPart(
            part_number=part.part_number,
            chat_id=locator.chat_id,
            msg_id=part.telegram_message_id,
            size_bytes=part.size_bytes,
        )
        for part in ordered
    )
    if any(part.chat_id != locator.chat_id for part in parts):
        raise ValueError("Viewer Cache chat mismatch")
    return VirtualMediaDescriptor(
        source_id=locator.source_id,
        cache_replica_id=locator.cache_replica_id,
        chat_id=locator.chat_id,
        thread_id=locator.thread_id,
        total_size_bytes=locator.total_size_bytes,
        parts=parts,
    )


def build_core_stream_url(base_url: str, source_id: UUID | str) -> str:
    parsed_source_id = UUID(str(source_id))
    normalized = base_url.strip().rstrip("/")
    parsed_url = urlsplit(normalized)
    if (
        parsed_url.scheme not in {"http", "https"}
        or not parsed_url.netloc
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise ValueError("base_url must be a credential-free http(s) URL")
    return f"{normalized}/stream/core/{parsed_source_id}"


_core_client: TLCoreClient | None = None


def get_core_client() -> TLCoreClient:
    global _core_client
    if _core_client is not None:
        return _core_client
    if not TLCore.BASE_URL or not TLCore.CREDENTIAL:
        raise CorePlaybackNotConfigured("Core playback is not configured")
    try:
        _core_client = TLCoreClient.from_config()
    except ValueError:
        raise CorePlaybackNotConfigured("Core playback configuration is invalid") from None
    return _core_client


async def close_core_client() -> None:
    global _core_client
    client, _core_client = _core_client, None
    if client is not None:
        await client.aclose()
