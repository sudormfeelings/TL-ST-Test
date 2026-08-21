"""Small authenticated TL-Core delivery client for local Stream orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx

from Backend.config import TLCore


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
    CACHE_INVALID = "CACHE_INVALID"
    CACHE_STORAGE_NOT_CONFIGURED = "CACHE_STORAGE_NOT_CONFIGURED"
    CACHE_STORAGE_INVALID = "CACHE_STORAGE_INVALID"
    CACHE_MATERIALIZATION_IN_PROGRESS = "CACHE_MATERIALIZATION_IN_PROGRESS"
    CACHE_RECONCILIATION_REQUIRED = "CACHE_RECONCILIATION_REQUIRED"
    CACHE_MATERIALIZATION_FAILED = "CACHE_MATERIALIZATION_FAILED"
    CACHE_COPY_SOURCE_MISSING = "CACHE_COPY_SOURCE_MISSING"
    CACHE_COPY_FORBIDDEN = "CACHE_COPY_FORBIDDEN"
    CACHE_COPY_UNAVAILABLE = "CACHE_COPY_UNAVAILABLE"
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
        timeout_seconds: float = 10.0,
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

        normalized_credential = credential.strip()
        self._base_url = normalized_url
        self._client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {normalized_credential}"},
            timeout=httpx.Timeout(timeout_seconds),
            transport=transport,
        )
        self._bootstrap: CoreBootstrap | None = None

    @classmethod
    def from_config(cls) -> "TLCoreClient":
        return cls(TLCore.BASE_URL, TLCore.CREDENTIAL)

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
    ) -> dict[str, Any]:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key is not None else None
        attempts = 2 if retry_uncertain_post else 1
        for attempt in range(attempts):
            try:
                response = await self._client.request(
                    method,
                    f"{self._base_url}{path}",
                    headers=headers,
                    json=json,
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
