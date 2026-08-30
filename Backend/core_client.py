"""Small authenticated TL-Core delivery client for local Stream orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import math
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx

from Backend.config import (
    DEFAULT_TL_CORE_RECOVERY_TIMEOUT_SECONDS,
    MAX_TL_CORE_RECOVERY_TIMEOUT_SECONDS,
    TLCore,
)


DEFAULT_CORE_TIMEOUT_SECONDS = 10.0


class CoreErrorCode(str, Enum):
    INVALID_CREDENTIAL = "INVALID_CREDENTIAL"
    INSTALLATION_REVOKED = "INSTALLATION_REVOKED"
    ACCESS_REVOKED = "ACCESS_REVOKED"
    CORE_SUSPENDED = "CORE_SUSPENDED"
    STREAM_REQUIRED = "STREAM_REQUIRED"
    STREAM_STORAGE_NOT_CONFIGURED = "STREAM_STORAGE_NOT_CONFIGURED"
    SOURCE_NOT_FOUND = "SOURCE_NOT_FOUND"
    SOURCE_NOT_PUBLISHED = "SOURCE_NOT_PUBLISHED"
    SOURCE_NOT_AVAILABLE = "SOURCE_NOT_AVAILABLE"
    CACHE_NOT_READY = "CACHE_NOT_READY"
    CACHE_NOT_FOUND = "CACHE_NOT_FOUND"
    CACHE_INVALID = "CACHE_INVALID"
    CACHE_STORAGE_NOT_CONFIGURED = "CACHE_STORAGE_NOT_CONFIGURED"
    CACHE_STORAGE_INVALID = "CACHE_STORAGE_INVALID"
    CACHE_MATERIALIZATION_IN_PROGRESS = "CACHE_MATERIALIZATION_IN_PROGRESS"
    CACHE_RECONCILIATION_REQUIRED = "CACHE_RECONCILIATION_REQUIRED"
    CACHE_MATERIALIZATION_FAILED = "CACHE_MATERIALIZATION_FAILED"
    CACHE_COPY_SOURCE_MISSING = "CACHE_COPY_SOURCE_MISSING"
    CACHE_COPY_FORBIDDEN = "CACHE_COPY_FORBIDDEN"
    CACHE_COPY_UNAVAILABLE = "CACHE_COPY_UNAVAILABLE"
    CACHE_HEALTH_UNCERTAIN = "CACHE_HEALTH_UNCERTAIN"
    CACHE_HEALTH_RECONCILIATION_FAILED = "CACHE_HEALTH_RECONCILIATION_FAILED"
    RECOVERY_SOURCE_UNAVAILABLE = "RECOVERY_SOURCE_UNAVAILABLE"
    RECOVERY_IN_PROGRESS = "RECOVERY_IN_PROGRESS"
    RECOVERY_RECONCILIATION_REQUIRED = "RECOVERY_RECONCILIATION_REQUIRED"
    IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"
    STREAM_READ_UNAVAILABLE = "STREAM_READ_UNAVAILABLE"
    CORE_UNAVAILABLE = "CORE_UNAVAILABLE"
    CORE_REQUEST_FAILED = "CORE_REQUEST_FAILED"
    INVALID_CORE_RESPONSE = "INVALID_CORE_RESPONSE"


class CoreClientError(RuntimeError):
    """Sanitized Stream-local representation of a Core failure."""

    def __init__(self, code: CoreErrorCode) -> None:
        self.code = code
        super().__init__(code.value)

    def __repr__(self) -> str:
        return f"CoreClientError(code={self.code.value!r})"


@dataclass(frozen=True, slots=True)
class StreamCapabilities:
    cache_materialization: bool
    multipart: bool
    http_range: bool


@dataclass(frozen=True, slots=True)
class CoreBootstrap:
    chat_id: int
    thread_id: int
    capabilities: StreamCapabilities


@dataclass(frozen=True, slots=True)
class CacheLocatorPart:
    part_number: int
    telegram_message_id: int
    size_bytes: int


@dataclass(frozen=True, slots=True)
class ValidatedCacheLocator:
    source_id: UUID
    cache_replica_id: UUID
    chat_id: int
    thread_id: int
    total_size_bytes: int
    parts: tuple[CacheLocatorPart, ...]


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    source_id: UUID
    cache_replica_id: UUID
    status: str
    expected_part_count: int
    parts: tuple[CacheLocatorPart, ...]


class CacheHealthOutcome(str, Enum):
    REPLICA_HEALTHY = "REPLICA_HEALTHY"
    REPLICA_DEGRADED = "REPLICA_DEGRADED"
    REPLICA_MISSING = "REPLICA_MISSING"
    REPLICA_INVALID_SHAPE = "REPLICA_INVALID_SHAPE"
    RECONCILIATION_RETRYABLE = "RECONCILIATION_RETRYABLE"
    RECONCILIATION_FAILED = "RECONCILIATION_FAILED"


class ReplicaAvailability(str, Enum):
    UNKNOWN = "UNKNOWN"
    AVAILABLE = "AVAILABLE"
    DEGRADED = "DEGRADED"
    MISSING = "MISSING"
    UNREACHABLE = "UNREACHABLE"


class SourceAvailability(str, Enum):
    UNKNOWN = "UNKNOWN"
    AVAILABLE = "AVAILABLE"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class CacheHealthResult:
    source_id: UUID
    cache_replica_id: UUID
    outcome: CacheHealthOutcome
    previous_availability: ReplicaAvailability
    resulting_availability: ReplicaAvailability
    source_availability: SourceAvailability
    retryable: bool


@dataclass(frozen=True, slots=True)
class CacheRecoveryResult:
    materialization_id: UUID
    source_id: UUID
    cache_replica_id: UUID
    status: str
    idempotent: bool
    recovered: bool
    reused: bool
    expected_part_count: int
    parts: tuple[CacheLocatorPart, ...]


class PresentationContainer(str, Enum):
    MKV = "MKV"
    MP4 = "MP4"
    AVI = "AVI"
    TS = "TS"
    M2TS = "M2TS"
    WEBM = "WEBM"


class PresentationResolution(str, Enum):
    UHD_2160P = "2160P"
    FHD_1080P = "1080P"
    HD_720P = "720P"
    SD_480P = "480P"


class PresentationVideoCodec(str, Enum):
    AV1 = "AV1"
    HEVC = "HEVC"
    AVC = "AVC"


class PresentationHdrFormat(str, Enum):
    HDR = "HDR"
    HDR10 = "HDR10"
    HDR10_PLUS = "HDR10_PLUS"
    DOLBY_VISION = "DOLBY_VISION"


class PresentationAudioCodec(str, Enum):
    AAC = "AAC"
    AC3 = "AC3"
    EAC3 = "EAC3"
    DTS = "DTS"
    DTS_HD = "DTS_HD"
    DTS_HD_MA = "DTS_HD_MA"
    TRUEHD = "TRUEHD"
    FLAC = "FLAC"
    OPUS = "OPUS"


class PresentationSourceType(str, Enum):
    WEB_DL = "WEB_DL"
    WEBRIP = "WEBRIP"
    BLURAY = "BLURAY"
    BDRIP = "BDRIP"
    REMUX = "REMUX"
    HDTV = "HDTV"


@dataclass(frozen=True, slots=True)
class DiscoveredSourcePresentation:
    release_name: str | None
    container: PresentationContainer | None
    resolution: PresentationResolution | None
    video_codec: PresentationVideoCodec | None
    video_profile: str | None
    hdr_format: PresentationHdrFormat | None
    audio_codec: PresentationAudioCodec | None
    audio_channels: str | None
    audio_layout: str | None
    audio_languages: tuple[str, ...] | None
    subtitle_languages: tuple[str, ...] | None
    edition: str | None
    release_group: str | None
    source_type: PresentationSourceType | None
    is_remux: bool | None
    is_hdr: bool | None
    is_dolby_vision: bool | None
    is_dual_audio: bool | None
    is_multi_audio: bool | None


@dataclass(frozen=True, slots=True)
class DiscoveredSource:
    source_id: UUID
    original_filename: str
    total_size_bytes: int
    expected_part_count: int
    created_at: datetime
    published_at: datetime | None
    presentation: DiscoveredSourcePresentation | None = None


def _positive_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return value


def _integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return value


def _uuid(value: Any) -> UUID:
    if not isinstance(value, str):
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    try:
        return UUID(value)
    except ValueError:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE) from None


def _object(value: Any, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return value


def _boolean(value: Any) -> bool:
    if not isinstance(value, bool):
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return value


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return value


def _string(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return value


def _nullable_string(value: Any) -> str | None:
    return None if value is None else _string(value)


def _nullable_boolean(value: Any) -> bool | None:
    return None if value is None else _boolean(value)


def _nullable_enum(value: Any, enum_type: type[Enum]):
    if value is None:
        return None
    if not isinstance(value, str):
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    try:
        return enum_type(value)
    except ValueError:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE) from None


def _nullable_strings(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return tuple(_string(item) for item in value)


_PRESENTATION_KEYS = {
    "release_name",
    "container",
    "resolution",
    "video_codec",
    "video_profile",
    "hdr_format",
    "audio_codec",
    "audio_channels",
    "audio_layout",
    "audio_languages",
    "subtitle_languages",
    "edition",
    "release_group",
    "source_type",
    "is_remux",
    "is_hdr",
    "is_dolby_vision",
    "is_dual_audio",
    "is_multi_audio",
}


def _source_presentation(value: Any) -> DiscoveredSourcePresentation | None:
    if value is None:
        return None
    item = _object(value, _PRESENTATION_KEYS)
    return DiscoveredSourcePresentation(
        release_name=_nullable_string(item["release_name"]),
        container=_nullable_enum(item["container"], PresentationContainer),
        resolution=_nullable_enum(item["resolution"], PresentationResolution),
        video_codec=_nullable_enum(item["video_codec"], PresentationVideoCodec),
        video_profile=_nullable_string(item["video_profile"]),
        hdr_format=_nullable_enum(item["hdr_format"], PresentationHdrFormat),
        audio_codec=_nullable_enum(item["audio_codec"], PresentationAudioCodec),
        audio_channels=_nullable_string(item["audio_channels"]),
        audio_layout=_nullable_string(item["audio_layout"]),
        audio_languages=_nullable_strings(item["audio_languages"]),
        subtitle_languages=_nullable_strings(item["subtitle_languages"]),
        edition=_nullable_string(item["edition"]),
        release_group=_nullable_string(item["release_group"]),
        source_type=_nullable_enum(item["source_type"], PresentationSourceType),
        is_remux=_nullable_boolean(item["is_remux"]),
        is_hdr=_nullable_boolean(item["is_hdr"]),
        is_dolby_vision=_nullable_boolean(item["is_dolby_vision"]),
        is_dual_audio=_nullable_boolean(item["is_dual_audio"]),
        is_multi_audio=_nullable_boolean(item["is_multi_audio"]),
    )


def _datetime(value: Any, *, nullable: bool = False) -> datetime | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE) from None
    if parsed.tzinfo is None:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return parsed


def _parts(value: Any, expected_count: int) -> tuple[CacheLocatorPart, ...]:
    if not isinstance(value, list) or len(value) != expected_count:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    parsed: list[CacheLocatorPart] = []
    for raw_part in value:
        part = _object(raw_part, {"part_number", "telegram_message_id", "size_bytes"})
        parsed.append(
            CacheLocatorPart(
                part_number=_positive_int(part["part_number"]),
                telegram_message_id=_positive_int(part["telegram_message_id"]),
                size_bytes=_positive_int(part["size_bytes"]),
            )
        )
    parsed.sort(key=lambda item: item.part_number)
    if [item.part_number for item in parsed] != list(range(1, expected_count + 1)):
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    if len({item.telegram_message_id for item in parsed}) != expected_count:
        raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
    return tuple(parsed)


class TLCoreClient:
    """Connection-reusing client for the narrow M6A delivery contract."""

    def __init__(
        self,
        base_url: str,
        credential: str,
        *,
        timeout_seconds: float = DEFAULT_CORE_TIMEOUT_SECONDS,
        recovery_timeout_seconds: float = DEFAULT_TL_CORE_RECOVERY_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        normalized_url = base_url.strip().rstrip("/")
        parsed_url = urlsplit(normalized_url)
        if (
            parsed_url.scheme not in {"http", "https"}
            or not parsed_url.netloc
            or parsed_url.username is not None
            or parsed_url.password is not None
            or parsed_url.query
            or parsed_url.fragment
        ):
            raise ValueError("TL_CORE_BASE_URL must be an http(s) URL without credentials, query, or fragment")
        if not credential.strip():
            raise ValueError("TL_CORE_CREDENTIAL is required")
        if timeout_seconds <= 0:
            raise ValueError("Core timeout must be positive")
        if (
            not math.isfinite(recovery_timeout_seconds)
            or recovery_timeout_seconds <= 0
            or recovery_timeout_seconds > MAX_TL_CORE_RECOVERY_TIMEOUT_SECONDS
        ):
            raise ValueError(
                "Core recovery timeout must be positive, finite, and no more than "
                f"{MAX_TL_CORE_RECOVERY_TIMEOUT_SECONDS:g} seconds"
            )

        normalized_credential = credential.strip()
        self._base_url = normalized_url
        self._recovery_timeout_seconds = recovery_timeout_seconds
        self._client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {normalized_credential}"},
            timeout=httpx.Timeout(timeout_seconds),
            transport=transport,
        )
        self._bootstrap: CoreBootstrap | None = None

    @classmethod
    def from_config(cls) -> "TLCoreClient":
        return cls(
            TLCore.BASE_URL,
            TLCore.CREDENTIAL,
            recovery_timeout_seconds=TLCore.RECOVERY_TIMEOUT_SECONDS,
        )

    async def __aenter__(self) -> "TLCoreClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        retry_uncertain_post: bool = False,
        request_timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key is not None else None
        attempts = 2 if retry_uncertain_post else 1
        for attempt in range(attempts):
            try:
                request_options = {}
                if request_timeout_seconds is not None:
                    request_options["timeout"] = httpx.Timeout(request_timeout_seconds)
                response = await self._client.request(
                    method,
                    f"{self._base_url}{path}",
                    headers=headers,
                    json=json,
                    **request_options,
                )
            except httpx.TransportError:
                if attempt + 1 < attempts:
                    continue
                raise CoreClientError(CoreErrorCode.CORE_UNAVAILABLE) from None

            if response.is_error:
                raise self._response_error(response)
            try:
                payload = response.json()
            except ValueError:
                raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE) from None
            if not isinstance(payload, dict):
                raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
            return payload
        raise CoreClientError(CoreErrorCode.CORE_UNAVAILABLE)

    @staticmethod
    def _response_error(response: httpx.Response) -> CoreClientError:
        try:
            payload = response.json()
            raw_code = payload["detail"]["code"]
            return CoreClientError(CoreErrorCode(raw_code))
        except (KeyError, TypeError, ValueError):
            if response.status_code == 401:
                return CoreClientError(CoreErrorCode.INVALID_CREDENTIAL)
            return CoreClientError(CoreErrorCode.CORE_REQUEST_FAILED)

    async def bootstrap(self) -> CoreBootstrap:
        if self._bootstrap is not None:
            return self._bootstrap
        payload = _object(
            await self._request_json("GET", "/api/v1/stream/bootstrap"),
            {"api_version", "viewer", "telegram_cache", "capabilities"},
        )
        if payload["api_version"] != "v1":
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        viewer = _object(payload["viewer"], {"storage_ready"})
        if _boolean(viewer["storage_ready"]) is not True:
            raise CoreClientError(CoreErrorCode.STREAM_STORAGE_NOT_CONFIGURED)
        cache = _object(payload["telegram_cache"], {"chat_id", "thread_id"})
        capabilities = _object(
            payload["capabilities"],
            {"cache_materialization", "multipart", "http_range"},
        )
        bootstrap = CoreBootstrap(
            chat_id=_integer(cache["chat_id"]),
            thread_id=_positive_int(cache["thread_id"]),
            capabilities=StreamCapabilities(
                cache_materialization=_boolean(capabilities["cache_materialization"]),
                multipart=_boolean(capabilities["multipart"]),
                http_range=_boolean(capabilities["http_range"]),
            ),
        )
        if bootstrap.chat_id == 0:
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        self._bootstrap = bootstrap
        return bootstrap

    async def get_cache(self, source_id: UUID | str) -> ValidatedCacheLocator:
        requested_source_id = _uuid(str(source_id))
        topology = await self.bootstrap()
        payload = _object(
            await self._request_json("GET", f"/api/v1/stream/cache/{requested_source_id}"),
            {"source_id", "cache_replica_id", "status", "content", "cache"},
        )
        returned_source_id = _uuid(payload["source_id"])
        if returned_source_id != requested_source_id or payload["status"] != "READY":
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        content = _object(payload["content"], {"media_id", "media_type", "episode_id"})
        _uuid(content["media_id"])
        if not isinstance(content["media_type"], str) or not content["media_type"]:
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        if content["episode_id"] is not None:
            _uuid(content["episode_id"])
        cache = _object(payload["cache"], {"expected_part_count", "total_size_bytes", "parts"})
        expected_count = _positive_int(cache["expected_part_count"])
        total_size = _positive_int(cache["total_size_bytes"])
        parts = _parts(cache["parts"], expected_count)
        if sum(part.size_bytes for part in parts) != total_size:
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        return ValidatedCacheLocator(
            source_id=returned_source_id,
            cache_replica_id=_uuid(payload["cache_replica_id"]),
            chat_id=topology.chat_id,
            thread_id=topology.thread_id,
            total_size_bytes=total_size,
            parts=parts,
        )

    async def materialize_cache(
        self,
        source_id: UUID | str,
        idempotency_key: str,
    ) -> MaterializationResult:
        requested_source_id = _uuid(str(source_id))
        if not idempotency_key:
            raise ValueError("idempotency_key is required")
        payload = _object(
            await self._request_json(
                "POST",
                "/api/v1/cache/materialize",
                json={"source_id": str(requested_source_id)},
                idempotency_key=idempotency_key,
                retry_uncertain_post=True,
            ),
            {
                "materialization_id",
                "source_id",
                "cache_replica_id",
                "status",
                "idempotent",
                "materialized",
                "expected_part_count",
                "cache",
            },
        )
        _uuid(payload["materialization_id"])
        returned_source_id = _uuid(payload["source_id"])
        if returned_source_id != requested_source_id:
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        if payload["status"] != "READY":
            raise CoreClientError(CoreErrorCode.CACHE_MATERIALIZATION_FAILED)
        _boolean(payload["idempotent"])
        _boolean(payload["materialized"])
        expected_count = _positive_int(payload["expected_part_count"])
        cache = _object(payload["cache"], {"parts"})
        return MaterializationResult(
            source_id=returned_source_id,
            cache_replica_id=_uuid(payload["cache_replica_id"]),
            status="READY",
            expected_part_count=expected_count,
            parts=_parts(cache["parts"], expected_count),
        )

    async def check_cache_health(self, source_id: UUID | str) -> CacheHealthResult:
        requested_source_id = _uuid(str(source_id))
        payload = _object(
            await self._request_json(
                "POST",
                "/api/v1/cache/health",
                json={"source_id": str(requested_source_id)},
            ),
            {
                "source_id",
                "cache_replica_id",
                "outcome",
                "previous_availability",
                "resulting_availability",
                "source_availability",
                "retryable",
            },
        )
        returned_source_id = _uuid(payload["source_id"])
        if returned_source_id != requested_source_id:
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        outcome = _nullable_enum(payload["outcome"], CacheHealthOutcome)
        previous = _nullable_enum(payload["previous_availability"], ReplicaAvailability)
        resulting = _nullable_enum(payload["resulting_availability"], ReplicaAvailability)
        source_availability = _nullable_enum(payload["source_availability"], SourceAvailability)
        if None in {outcome, previous, resulting, source_availability}:
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        retryable = _boolean(payload["retryable"])
        if retryable is not (outcome is CacheHealthOutcome.RECONCILIATION_RETRYABLE):
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        return CacheHealthResult(
            source_id=returned_source_id,
            cache_replica_id=_uuid(payload["cache_replica_id"]),
            outcome=outcome,
            previous_availability=previous,
            resulting_availability=resulting,
            source_availability=source_availability,
            retryable=retryable,
        )

    async def recover_cache(
        self,
        source_id: UUID | str,
        idempotency_key: str,
    ) -> CacheRecoveryResult:
        requested_source_id = _uuid(str(source_id))
        if not idempotency_key:
            raise ValueError("idempotency_key is required")
        payload = _object(
            await self._request_json(
                "POST",
                "/api/v1/cache/recover",
                json={"source_id": str(requested_source_id)},
                idempotency_key=idempotency_key,
                request_timeout_seconds=self._recovery_timeout_seconds,
            ),
            {
                "materialization_id",
                "source_id",
                "cache_replica_id",
                "status",
                "idempotent",
                "recovered",
                "reused",
                "expected_part_count",
                "cache",
            },
        )
        returned_source_id = _uuid(payload["source_id"])
        if returned_source_id != requested_source_id or payload["status"] != "READY":
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        expected_count = _positive_int(payload["expected_part_count"])
        cache = _object(payload["cache"], {"parts"})
        recovered = _boolean(payload["recovered"])
        reused = _boolean(payload["reused"])
        if recovered is reused:
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        return CacheRecoveryResult(
            materialization_id=_uuid(payload["materialization_id"]),
            source_id=returned_source_id,
            cache_replica_id=_uuid(payload["cache_replica_id"]),
            status="READY",
            idempotent=_boolean(payload["idempotent"]),
            recovered=recovered,
            reused=reused,
            expected_part_count=expected_count,
            parts=_parts(cache["parts"], expected_count),
        )

    async def discover_sources(
        self,
        requested_identity: dict[str, Any],
        *,
        include_presentation: bool = False,
    ) -> tuple[DiscoveredSource, ...]:
        path = "/api/v1/stream/sources"
        if include_presentation:
            path += "?include_presentation=true"
        payload = _object(
            await self._request_json(
                "POST",
                path,
                json=requested_identity,
            ),
            {"requested", "canonical", "sources"},
        )
        requested = _object(
            payload["requested"],
            {"raw_id", "provider", "namespace", "external_id", "episode"},
        )
        _string(requested["provider"])
        _string(requested["namespace"])
        _string(requested["external_id"])
        if requested["raw_id"] is not None and not isinstance(requested["raw_id"], str):
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        if requested["episode"] is not None:
            episode = _object(
                requested["episode"],
                {"kind", "season_number", "episode_number", "absolute_number"},
            )
            kind = _string(episode["kind"])
            if kind == "SEASON_EPISODE":
                _nonnegative_int(episode["season_number"])
                _positive_int(episode["episode_number"])
                if episode["absolute_number"] is not None:
                    raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
            elif kind == "ABSOLUTE":
                _positive_int(episode["absolute_number"])
                if episode["season_number"] is not None or episode["episode_number"] is not None:
                    raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
            else:
                raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)

        canonical = _object(payload["canonical"], {"media_id", "media_type", "episode_id"})
        _uuid(canonical["media_id"])
        _string(canonical["media_type"])
        if canonical["episode_id"] is not None:
            _uuid(canonical["episode_id"])

        raw_sources = payload["sources"]
        if not isinstance(raw_sources, list):
            raise CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        sources: list[DiscoveredSource] = []
        for raw_source in raw_sources:
            source_keys = {
                "source_id",
                "original_filename",
                "total_size_bytes",
                "expected_part_count",
                "created_at",
                "published_at",
            }
            if include_presentation:
                source_keys.add("presentation")
            source = _object(
                raw_source,
                source_keys,
            )
            sources.append(
                DiscoveredSource(
                    source_id=_uuid(source["source_id"]),
                    original_filename=_string(source["original_filename"]),
                    total_size_bytes=_positive_int(source["total_size_bytes"]),
                    expected_part_count=_positive_int(source["expected_part_count"]),
                    created_at=_datetime(source["created_at"]),
                    published_at=_datetime(source["published_at"], nullable=True),
                    presentation=(
                        _source_presentation(source["presentation"])
                        if include_presentation
                        else None
                    ),
                )
            )
        return tuple(sources)

    async def ensure_cache(
        self,
        source_id: UUID | str,
        *,
        idempotency_key: str | None = None,
    ) -> ValidatedCacheLocator:
        requested_source_id = _uuid(str(source_id))
        try:
            return await self.get_cache(requested_source_id)
        except CoreClientError as exc:
            if exc.code is not CoreErrorCode.CACHE_NOT_READY:
                raise

        key = idempotency_key or str(uuid4())
        materialized = await self.materialize_cache(requested_source_id, key)
        if materialized.status != "READY":
            raise CoreClientError(CoreErrorCode.CACHE_MATERIALIZATION_FAILED)
        return await self.get_cache(requested_source_id)
