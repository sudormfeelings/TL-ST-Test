from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


class ManifestValidationError(ValueError):
    """Raised when a disposable M0 manifest is malformed or unsafe to stream."""


def _required_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ManifestValidationError(f"{key} must be an integer")
    return value


def _validate_message_id(value: int, key: str) -> None:
    if value <= 0:
        raise ManifestValidationError(f"{key} must be greater than zero")


def _ordered(parts: Iterable[Any], *, allow_empty: bool = False) -> tuple[Any, ...]:
    ordered = tuple(sorted(parts, key=lambda part: part.index))
    if not ordered and not allow_empty:
        raise ManifestValidationError("manifest must contain at least one part")
    indexes = [part.index for part in ordered]
    if indexes != list(range(len(indexes))):
        raise ManifestValidationError("part indexes must be unique and contiguous from zero")
    return ordered


@dataclass(frozen=True)
class SourcePart:
    index: int
    source_chat_id: int
    source_message_id: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ManifestValidationError("part index cannot be negative")
        _validate_message_id(self.source_message_id, "source_message_id")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourcePart":
        return cls(
            index=_required_int(data, "index"),
            source_chat_id=_required_int(data, "source_chat_id"),
            source_message_id=_required_int(data, "source_message_id"),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "index": self.index,
            "source_chat_id": self.source_chat_id,
            "source_message_id": self.source_message_id,
        }


@dataclass(frozen=True)
class SourceManifest:
    logical_name: str
    parts: tuple[SourcePart, ...]

    def __post_init__(self) -> None:
        if not self.logical_name.strip():
            raise ManifestValidationError("logical_name cannot be empty")
        object.__setattr__(self, "parts", _ordered(self.parts))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceManifest":
        raw_parts = data.get("parts")
        if not isinstance(raw_parts, list):
            raise ManifestValidationError("parts must be a list")
        return cls(
            logical_name=str(data.get("logical_name") or "").strip(),
            parts=tuple(SourcePart.from_dict(part) for part in raw_parts),
        )

    def to_dict(self) -> dict[str, Any]:
        return {"logical_name": self.logical_name, "parts": [part.to_dict() for part in self.parts]}


@dataclass(frozen=True)
class DestinationPart:
    index: int
    destination_chat_id: int
    destination_message_id: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ManifestValidationError("part index cannot be negative")
        _validate_message_id(self.destination_message_id, "destination_message_id")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DestinationPart":
        return cls(
            index=_required_int(data, "index"),
            destination_chat_id=_required_int(data, "destination_chat_id"),
            destination_message_id=_required_int(data, "destination_message_id"),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "index": self.index,
            "destination_chat_id": self.destination_chat_id,
            "destination_message_id": self.destination_message_id,
        }


@dataclass(frozen=True)
class CopyFailureReport:
    successful_parts: tuple[int, ...]
    failed_part: int
    error_type: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "COPY_PARTIAL_FAILURE",
            "successful_parts": list(self.successful_parts),
            "failed_part": self.failed_part,
            "error_type": self.error_type,
        }


@dataclass(frozen=True)
class DestinationManifest:
    logical_name: str
    destination_topic_id: int
    parts: tuple[DestinationPart, ...]
    complete: bool = True
    failure: CopyFailureReport | None = None

    def __post_init__(self) -> None:
        if not self.logical_name.strip():
            raise ManifestValidationError("logical_name cannot be empty")
        object.__setattr__(self, "parts", _ordered(self.parts, allow_empty=not self.complete))
        if self.complete and self.failure is not None:
            raise ManifestValidationError("a complete destination manifest cannot contain a failure")
        if not self.complete and self.failure is None:
            raise ManifestValidationError("an incomplete destination manifest requires failure details")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DestinationManifest":
        raw_parts = data.get("parts")
        if not isinstance(raw_parts, list):
            raise ManifestValidationError("parts must be a list")
        complete = data.get("complete", True)
        if not isinstance(complete, bool):
            raise ManifestValidationError("complete must be a boolean")
        raw_failure = data.get("failure")
        failure = None
        if raw_failure is not None:
            if not isinstance(raw_failure, dict):
                raise ManifestValidationError("failure must be an object")
            successful = raw_failure.get("successful_parts")
            if not isinstance(successful, list) or any(isinstance(x, bool) or not isinstance(x, int) for x in successful):
                raise ManifestValidationError("failure.successful_parts must be an integer list")
            failure = CopyFailureReport(
                successful_parts=tuple(successful),
                failed_part=_required_int(raw_failure, "failed_part"),
                error_type=str(raw_failure.get("error_type") or "UnknownError"),
            )
        return cls(
            logical_name=str(data.get("logical_name") or "").strip(),
            destination_topic_id=_required_int(data, "destination_topic_id"),
            parts=tuple(DestinationPart.from_dict(part) for part in raw_parts),
            complete=complete,
            failure=failure,
        )

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "logical_name": self.logical_name,
            "destination_topic_id": self.destination_topic_id,
            "complete": self.complete,
            "parts": [part.to_dict() for part in self.parts],
        }
        if self.failure is not None:
            result["failure"] = self.failure.to_dict()
        return result


@dataclass(frozen=True)
class PlaybackPart:
    index: int
    chat_id: int
    message_id: int
    size: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ManifestValidationError("part index cannot be negative")
        _validate_message_id(self.message_id, "message_id")
        if self.size <= 0:
            raise ManifestValidationError("playback part size must be greater than zero")


@dataclass(frozen=True)
class PlaybackManifest:
    logical_name: str
    parts: tuple[PlaybackPart, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "parts", _ordered(self.parts))

    @property
    def virtual_size(self) -> int:
        return sum(part.size for part in self.parts)


def calculate_virtual_size(parts: Iterable[PlaybackPart]) -> int:
    return sum(part.size for part in parts)


def cross_part_slices(parts: Iterable[PlaybackPart], start: int, end: int) -> list[tuple[int, int, int]]:
    """Return (part index, local start, local end), inclusive, for a virtual range."""
    ordered = _ordered(parts)
    virtual_size = calculate_virtual_size(ordered)
    if start < 0 or end < start or end >= virtual_size:
        raise ManifestValidationError("virtual range is outside the playback manifest")
    slices: list[tuple[int, int, int]] = []
    cumulative = 0
    for part in ordered:
        part_end = cumulative + part.size - 1
        if part_end >= start and cumulative <= end:
            slices.append((part.index, max(start, cumulative) - cumulative, min(end, part_end) - cumulative))
        cumulative = part_end + 1
    return slices
