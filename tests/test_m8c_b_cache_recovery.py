import asyncio
import inspect
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pyrogram.errors import FloodWait, MessageIdInvalid, MessageIdsEmpty, SessionExpired

from Backend.config import (
    DEFAULT_TL_CORE_RECOVERY_TIMEOUT_SECONDS,
    MAX_TL_CORE_RECOVERY_TIMEOUT_SECONDS,
    TLCore,
    _bounded_float_env,
)
from Backend.core_client import (
    CacheHealthOutcome,
    CacheHealthResult,
    CacheLocatorPart,
    CoreClientError,
    CoreErrorCode,
    ReplicaAvailability,
    SourceAvailability,
    TLCoreClient,
    ValidatedCacheLocator,
)
from Backend.core_recovery import (
    PlaybackRecoveryBudget,
    health_allows_recovery,
    is_definite_cache_loss,
)
from Backend.fastapi.routes import stremio_routes, stream_routes
from Backend.helper.custom_dl import ByteStreamer
from Backend.helper.exceptions import FileNotFound


SOURCE_ID = UUID("11111111-1111-1111-1111-111111111111")
OLD_REPLICA_ID = UUID("22222222-2222-2222-2222-222222222222")
NEW_REPLICA_ID = UUID("33333333-3333-3333-3333-333333333333")
CHAT_ID = -100123456


def cache_locator(*, fresh=False):
    base = 1900 if fresh else 900
    return ValidatedCacheLocator(
        source_id=SOURCE_ID,
        cache_replica_id=NEW_REPLICA_ID if fresh else OLD_REPLICA_ID,
        chat_id=CHAT_ID,
        thread_id=77,
        total_size_bytes=9,
        parts=(
            CacheLocatorPart(1, base + 1, 4),
            CacheLocatorPart(2, base + 2, 5),
        ),
    )


def health_result(
    outcome=CacheHealthOutcome.REPLICA_MISSING,
    resulting=ReplicaAvailability.MISSING,
    source_availability=SourceAvailability.AVAILABLE,
    retryable=False,
):
    return CacheHealthResult(
        source_id=SOURCE_ID,
        cache_replica_id=OLD_REPLICA_ID,
        outcome=outcome,
        previous_availability=ReplicaAvailability.AVAILABLE,
        resulting_availability=resulting,
        source_availability=source_availability,
        retryable=retryable,
    )


def health_payload(**overrides):
    payload = {
        "source_id": str(SOURCE_ID),
        "cache_replica_id": str(OLD_REPLICA_ID),
        "outcome": "REPLICA_HEALTHY",
        "previous_availability": "AVAILABLE",
        "resulting_availability": "AVAILABLE",
        "source_availability": "AVAILABLE",
        "retryable": False,
    }
    payload.update(overrides)
    return payload


def recovery_payload(**overrides):
    payload = {
        "materialization_id": "44444444-4444-4444-4444-444444444444",
        "source_id": str(SOURCE_ID),
        "cache_replica_id": str(NEW_REPLICA_ID),
        "status": "READY",
        "idempotent": False,
        "recovered": True,
        "reused": False,
        "expected_part_count": 2,
        "cache": {
            "parts": [
                {"part_number": 1, "telegram_message_id": 1901, "size_bytes": 4},
                {"part_number": 2, "telegram_message_id": 1902, "size_bytes": 5},
            ]
        },
    }
    payload.update(overrides)
    return payload


class CoreRecoveryClientTests(unittest.IsolatedAsyncioTestCase):
    async def _call(self, callback, **client_options):
        requests = []

        def handler(request):
            requests.append(request)
            return callback(request)

        client = TLCoreClient(
            "https://core.example.test",
            "private-credential",
            transport=httpx.MockTransport(handler),
            **client_options,
        )
        return client, requests

    @staticmethod
    def _timeout(request):
        return request.extensions["timeout"]

    async def test_normal_locator_and_health_keep_short_timeout(self):
        def response(request):
            if request.url.path.endswith("/bootstrap"):
                payload = {
                    "api_version": "v1",
                    "viewer": {"storage_ready": True},
                    "telegram_cache": {"chat_id": CHAT_ID, "thread_id": 77},
                    "capabilities": {
                        "cache_materialization": True,
                        "multipart": True,
                        "http_range": False,
                    },
                }
            elif request.url.path.startswith("/api/v1/stream/cache/"):
                payload = {
                    "source_id": str(SOURCE_ID),
                    "cache_replica_id": str(OLD_REPLICA_ID),
                    "status": "READY",
                    "content": {
                        "media_id": "55555555-5555-5555-5555-555555555555",
                        "media_type": "MOVIE",
                        "episode_id": None,
                    },
                    "cache": {
                        "expected_part_count": 2,
                        "total_size_bytes": 9,
                        "parts": [
                            {"part_number": 1, "telegram_message_id": 901, "size_bytes": 4},
                            {"part_number": 2, "telegram_message_id": 902, "size_bytes": 5},
                        ],
                    },
                }
            elif request.url.path == "/api/v1/cache/materialize":
                payload = {
                    "materialization_id": "66666666-6666-6666-6666-666666666666",
                    "source_id": str(SOURCE_ID),
                    "cache_replica_id": str(OLD_REPLICA_ID),
                    "status": "READY",
                    "idempotent": False,
                    "materialized": True,
                    "expected_part_count": 2,
                    "cache": {
                        "parts": [
                            {"part_number": 1, "telegram_message_id": 901, "size_bytes": 4},
                            {"part_number": 2, "telegram_message_id": 902, "size_bytes": 5},
                        ]
                    },
                }
            else:
                payload = health_payload()
            return httpx.Response(200, request=request, json=payload)

        client, requests = await self._call(
            response,
            timeout_seconds=7,
            recovery_timeout_seconds=75,
        )
        try:
            await client.get_cache(SOURCE_ID)
            await client.check_cache_health(SOURCE_ID)
            await client.materialize_cache(SOURCE_ID, "materialize-key")
        finally:
            await client.aclose()
        self.assertEqual(len(requests), 4)
        for request in requests:
            self.assertEqual(set(self._timeout(request).values()), {7})

    async def test_recovery_alone_uses_dedicated_longer_timeout(self):
        client, requests = await self._call(
            lambda request: httpx.Response(201, request=request, json=recovery_payload()),
            timeout_seconds=7,
            recovery_timeout_seconds=75,
        )
        try:
            result = await client.recover_cache(SOURCE_ID, "one-recovery-key")
        finally:
            await client.aclose()
        self.assertEqual(result.status, "READY")
        self.assertEqual(len(requests), 1)
        self.assertEqual(set(self._timeout(requests[0]).values()), {75})
        self.assertGreater(self._timeout(requests[0])["read"], 10)

    async def test_recovery_timeout_is_one_sanitized_ambiguous_attempt(self):
        def timeout(request):
            raise httpx.ReadTimeout("private timeout detail", request=request)

        client, requests = await self._call(
            timeout,
            recovery_timeout_seconds=75,
        )
        try:
            with self.assertRaises(CoreClientError) as caught:
                await client.recover_cache(SOURCE_ID, "only-key")
        finally:
            await client.aclose()
        self.assertEqual(caught.exception.code, CoreErrorCode.CORE_UNAVAILABLE)
        self.assertNotIn("private", str(caught.exception))
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].headers["Idempotency-Key"], "only-key")
        self.assertEqual(set(self._timeout(requests[0]).values()), {75})

    async def test_recovery_timeout_configuration_is_positive_finite_and_bounded(self):
        for invalid in (0, -1, float("inf"), float("nan"), 301):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    TLCoreClient(
                        "https://core.example.test",
                        "private-credential",
                        recovery_timeout_seconds=invalid,
                    )
        self.assertEqual(DEFAULT_TL_CORE_RECOVERY_TIMEOUT_SECONDS, 90)
        self.assertEqual(MAX_TL_CORE_RECOVERY_TIMEOUT_SECONDS, 300)

    async def test_invalid_recovery_timeout_environment_values_fall_back(self):
        for invalid in ("", "0", "-1", "nan", "inf", "301", "not-a-number"):
            with self.subTest(invalid=invalid), patch.dict(
                "os.environ", {"TEST_RECOVERY_TIMEOUT": invalid}
            ):
                self.assertEqual(
                    _bounded_float_env("TEST_RECOVERY_TIMEOUT", 90, 300),
                    90,
                )
        with patch.dict("os.environ", {"TEST_RECOVERY_TIMEOUT": "75"}):
            self.assertEqual(_bounded_float_env("TEST_RECOVERY_TIMEOUT", 90, 300), 75)

    async def test_from_config_passes_only_recovery_timeout_override(self):
        with (
            patch.object(TLCore, "BASE_URL", "https://core.example.test"),
            patch.object(TLCore, "CREDENTIAL", "private-credential"),
            patch.object(TLCore, "RECOVERY_TIMEOUT_SECONDS", 75),
        ):
            client = TLCoreClient.from_config()
        try:
            self.assertEqual(client._client.timeout.read, 10)
            self.assertEqual(client._recovery_timeout_seconds, 75)
        finally:
            await client.aclose()

    async def test_health_endpoint_body_headers_and_healthy_parse(self):
        client, requests = await self._call(
            lambda request: httpx.Response(200, request=request, json=health_payload())
        )
        try:
            result = await client.check_cache_health(SOURCE_ID)
        finally:
            await client.aclose()
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].method, "POST")
        self.assertEqual(requests[0].url.path, "/api/v1/cache/health")
        self.assertEqual(__import__("json").loads(requests[0].content), {"source_id": str(SOURCE_ID)})
        self.assertNotIn("Idempotency-Key", requests[0].headers)
        self.assertEqual(result.outcome, CacheHealthOutcome.REPLICA_HEALTHY)

    async def test_missing_and_retryable_health_responses_parse(self):
        payloads = (
            health_payload(outcome="REPLICA_MISSING", resulting_availability="MISSING"),
            health_payload(outcome="RECONCILIATION_RETRYABLE", retryable=True),
        )
        for payload in payloads:
            with self.subTest(payload=payload):
                client, _ = await self._call(
                    lambda request, payload=payload: httpx.Response(200, request=request, json=payload)
                )
                try:
                    result = await client.check_cache_health(SOURCE_ID)
                finally:
                    await client.aclose()
                self.assertEqual(result.retryable, payload["retryable"])

    async def test_health_rejects_topology_and_malformed_contracts(self):
        invalid = []
        topology = health_payload()
        topology["chat_id"] = CHAT_ID
        invalid.append(topology)
        invalid.append(health_payload(outcome="UNKNOWN_OUTCOME"))
        invalid.append(health_payload(retryable=True))
        invalid.append(health_payload(source_id=str(NEW_REPLICA_ID)))
        for payload in invalid:
            with self.subTest(payload=payload):
                client, _ = await self._call(
                    lambda request, payload=payload: httpx.Response(200, request=request, json=payload)
                )
                try:
                    with self.assertRaises(CoreClientError) as caught:
                        await client.check_cache_health(SOURCE_ID)
                finally:
                    await client.aclose()
                self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CORE_RESPONSE)

    async def test_recovery_endpoint_key_body_and_destination_response(self):
        client, requests = await self._call(
            lambda request: httpx.Response(201, request=request, json=recovery_payload())
        )
        try:
            result = await client.recover_cache(SOURCE_ID, "stream-recovery-stable")
        finally:
            await client.aclose()
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].url.path, "/api/v1/cache/recover")
        self.assertEqual(requests[0].headers["Idempotency-Key"], "stream-recovery-stable")
        self.assertEqual(__import__("json").loads(requests[0].content), {"source_id": str(SOURCE_ID)})
        self.assertEqual(result.cache_replica_id, NEW_REPLICA_ID)
        self.assertEqual([part.telegram_message_id for part in result.parts], [1901, 1902])

    async def test_recovery_rejects_origin_topology_and_malformed_response(self):
        invalid = []
        origin = recovery_payload()
        origin["origin_chat_id"] = -999
        invalid.append(origin)
        invalid.append(recovery_payload(status="PENDING"))
        invalid.append(recovery_payload(source_id=str(NEW_REPLICA_ID)))
        invalid.append(recovery_payload(expected_part_count=3))
        invalid.append(recovery_payload(recovered=True, reused=True))
        invalid.append(recovery_payload(recovered=False, reused=False))
        for payload in invalid:
            with self.subTest(payload=payload):
                client, _ = await self._call(
                    lambda request, payload=payload: httpx.Response(200, request=request, json=payload)
                )
                try:
                    with self.assertRaises(CoreClientError) as caught:
                        await client.recover_cache(SOURCE_ID, "stable-key")
                finally:
                    await client.aclose()
                self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CORE_RESPONSE)


class RecoveryPolicyTests(unittest.TestCase):
    def test_only_documented_definite_message_loss_is_classified(self):
        for error in (FileNotFound("deleted"), MessageIdInvalid(), MessageIdsEmpty()):
            with self.subTest(error=type(error).__name__):
                self.assertTrue(is_definite_cache_loss(error))
        for error in (
            TimeoutError(), ConnectionError(), OSError(), FloodWait(1), SessionExpired(),
            RuntimeError(), asyncio.CancelledError(), CoreClientError(CoreErrorCode.CORE_UNAVAILABLE),
        ):
            with self.subTest(error=type(error).__name__):
                self.assertFalse(is_definite_cache_loss(error))

    def test_health_policy_requires_definite_broken_cache_and_available_source(self):
        self.assertTrue(health_allows_recovery(health_result()))
        self.assertTrue(health_allows_recovery(health_result(
            CacheHealthOutcome.REPLICA_DEGRADED, ReplicaAvailability.DEGRADED
        )))
        self.assertTrue(health_allows_recovery(health_result(
            CacheHealthOutcome.REPLICA_INVALID_SHAPE, ReplicaAvailability.DEGRADED
        )))
        refused = (
            health_result(CacheHealthOutcome.REPLICA_HEALTHY, ReplicaAvailability.AVAILABLE),
            health_result(CacheHealthOutcome.RECONCILIATION_RETRYABLE, ReplicaAvailability.AVAILABLE, retryable=True),
            health_result(CacheHealthOutcome.RECONCILIATION_FAILED, ReplicaAvailability.AVAILABLE),
            health_result(source_availability=SourceAvailability.UNAVAILABLE),
        )
        for result in refused:
            with self.subTest(result=result):
                self.assertFalse(health_allows_recovery(result))

    def test_budget_rejects_every_duplicate_phase(self):
        budget = PlaybackRecoveryBudget()
        for method_name in (
            "begin_health", "begin_recovery", "begin_locator_refresh", "begin_playback_retry"
        ):
            method = getattr(budget, method_name)
            method()
            with self.assertRaises(RuntimeError):
                method()


class TelegramMetadataPrivacyTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_lookup_logging_omits_raw_telegram_exception_text(self):
        streamer = object.__new__(ByteStreamer)
        streamer.client = SimpleNamespace(
            get_messages=AsyncMock(side_effect=RuntimeError("private-telegram-detail"))
        )
        streamer.expose_locator_metadata = False
        streamer._file_id_cache = {}
        with patch("Backend.helper.pyro.LOGGER") as logger:
            with self.assertRaises(RuntimeError):
                await streamer.get_file_properties(CHAT_ID, 901)
        rendered = " ".join(str(value) for call in logger.method_calls for value in call.args)
        self.assertNotIn("private-telegram-detail", rendered)
        self.assertIn("RuntimeError", rendered)


class FakeCoreClient:
    def __init__(self, *, health=None, fresh=None, recover_error=None, locator_error=None):
        self.initial = cache_locator()
        self.health = health if health is not None else health_result()
        self.fresh = fresh if fresh is not None else cache_locator(fresh=True)
        self.recover_error = recover_error
        self.locator_error = locator_error
        self.ensure_calls = []
        self.health_calls = []
        self.recover_calls = []
        self.get_calls = []

    async def ensure_cache(self, source_id):
        self.ensure_calls.append(source_id)
        return self.initial

    async def check_cache_health(self, source_id):
        self.health_calls.append(source_id)
        if isinstance(self.health, BaseException):
            raise self.health
        return self.health

    async def recover_cache(self, source_id, key):
        self.recover_calls.append((source_id, key))
        if self.recover_error is not None:
            raise self.recover_error
        return SimpleNamespace(status="READY")

    async def get_cache(self, source_id):
        self.get_calls.append(source_id)
        if self.locator_error is not None:
            raise self.locator_error
        return self.fresh


class FakeRecoveryStreamer:
    def __init__(self, *, missing=(), errors=None, stale=()):
        self._file_id_cache = {}
        self.missing = set(missing)
        self.errors = errors or {}
        self.property_calls = []
        self.byte_calls = []
        self.files = {
            (CHAT_ID, 901): SimpleNamespace(data=b"ABCD", file_size=4, file_name="cache.mkv.001", mime_type="video/x-matroska"),
            (CHAT_ID, 902): SimpleNamespace(data=b"EFGHI", file_size=5, file_name="cache.mkv.002", mime_type="video/x-matroska"),
            (CHAT_ID, 1901): SimpleNamespace(data=b"ABCD", file_size=4, file_name="cache.mkv.001", mime_type="video/x-matroska"),
            (CHAT_ID, 1902): SimpleNamespace(data=b"EFGHI", file_size=5, file_name="cache.mkv.002", mime_type="video/x-matroska"),
        }
        for key in stale:
            self._file_id_cache[key] = self.files[key]

    async def get_file_properties(self, chat_id, message_id):
        key = (chat_id, message_id)
        self.property_calls.append(key)
        if key in self._file_id_cache:
            return self._file_id_cache[key]
        if key in self.errors:
            raise self.errors[key]
        if key in self.missing:
            raise FileNotFound("destination missing")
        item = self.files[key]
        self._file_id_cache[key] = item
        return item

    async def prefetch_stream(
        self, *, file_id, offset, first_part_cut, last_part_cut, part_count,
        chunk_size, chat_id, message_id, **_kwargs,
    ):
        self.byte_calls.append((chat_id, message_id, offset, part_count))

        async def generate():
            for sequence in range(part_count):
                start = offset + sequence * chunk_size
                chunk = file_id.data[start:start + chunk_size]
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif sequence == 0:
                    yield chunk[first_part_cut:]
                elif sequence == part_count - 1:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk

        return generate()


class RecoveryRouteTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(stream_routes.router)
        self.client = TestClient(app)

    def request(self, core, streamer, *, range_value=None, method="GET"):
        headers = {"Range": range_value} if range_value else {}
        with (
            patch.object(stream_routes, "get_core_client", return_value=core),
            patch.object(stream_routes, "_get_core_userbot_streamer", return_value=streamer),
            patch.object(stream_routes, "select_best_client", side_effect=AssertionError("bot forbidden")),
        ):
            return self.client.request(method, f"/stream/core/{SOURCE_ID}", headers=headers)

    def test_healthy_playback_has_zero_recovery_control_calls_and_range_unchanged(self):
        core, streamer = FakeCoreClient(), FakeRecoveryStreamer()
        response = self.request(core, streamer, range_value="bytes=2-6")
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"CDEFG")
        self.assertEqual(response.headers["content-range"], "bytes 2-6/9")
        self.assertEqual(response.headers["content-length"], "5")
        self.assertEqual(core.health_calls, [])
        self.assertEqual(core.recover_calls, [])
        self.assertEqual(core.get_calls, [])

    def test_head_preserves_range_headers_without_body_or_recovery(self):
        core, streamer = FakeCoreClient(), FakeRecoveryStreamer()
        response = self.request(core, streamer, range_value="bytes=2-6", method="HEAD")
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"")
        self.assertEqual(response.headers["content-range"], "bytes 2-6/9")
        self.assertEqual(response.headers["content-length"], "5")
        self.assertEqual(core.health_calls, [])
        self.assertEqual(core.recover_calls, [])
        self.assertEqual(core.get_calls, [])

    def test_definite_missing_recovers_once_refreshes_once_and_retries_fresh_multipart(self):
        core = FakeCoreClient()
        old_keys = {(CHAT_ID, 901), (CHAT_ID, 902)}
        streamer = FakeRecoveryStreamer(missing=old_keys, stale=old_keys)
        response = self.request(core, streamer, range_value="bytes=2-6")
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"CDEFG")
        self.assertEqual(response.headers["content-range"], "bytes 2-6/9")
        self.assertEqual(response.headers["content-length"], "5")
        self.assertEqual(core.health_calls, [SOURCE_ID])
        self.assertEqual(core.get_calls, [SOURCE_ID])
        self.assertEqual(len(core.recover_calls), 1)
        recovery_source, recovery_key = core.recover_calls[0]
        self.assertEqual(recovery_source, SOURCE_ID)
        self.assertRegex(recovery_key, r"^stream-recovery-[0-9a-f-]{36}$")
        self.assertEqual(
            streamer.property_calls,
            [(CHAT_ID, 901), (CHAT_ID, 1901), (CHAT_ID, 1902)],
        )
        self.assertEqual([call[:2] for call in streamer.byte_calls], [(CHAT_ID, 1901), (CHAT_ID, 1902)])

    def test_later_multipart_part_loss_triggers_only_one_full_recovery_sequence(self):
        core = FakeCoreClient()
        streamer = FakeRecoveryStreamer(missing={(CHAT_ID, 902)})
        response = self.request(core, streamer, range_value="bytes=3-5")
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"DEF")
        self.assertEqual(response.headers["content-range"], "bytes 3-5/9")
        self.assertEqual(response.headers["content-length"], "3")
        self.assertEqual(core.health_calls, [SOURCE_ID])
        self.assertEqual(len(core.recover_calls), 1)
        self.assertEqual(core.get_calls, [SOURCE_ID])
        self.assertEqual(
            streamer.property_calls,
            [(CHAT_ID, 901), (CHAT_ID, 902), (CHAT_ID, 1901), (CHAT_ID, 1902)],
        )

    def test_health_disagreement_retryable_or_failure_never_recovers(self):
        cases = (
            health_result(CacheHealthOutcome.REPLICA_HEALTHY, ReplicaAvailability.AVAILABLE),
            health_result(CacheHealthOutcome.RECONCILIATION_RETRYABLE, ReplicaAvailability.AVAILABLE, retryable=True),
            CoreClientError(CoreErrorCode.CORE_UNAVAILABLE),
        )
        for health in cases:
            with self.subTest(health=health):
                core = FakeCoreClient(health=health)
                response = self.request(core, FakeRecoveryStreamer(missing={(CHAT_ID, 901)}))
                self.assertEqual(response.status_code, 503)
                self.assertEqual(len(core.health_calls), 1)
                self.assertEqual(core.recover_calls, [])
                self.assertEqual(core.get_calls, [])

    def test_uncertain_telegram_failures_never_call_health(self):
        errors = (
            TimeoutError("secret timeout"), ConnectionError("secret connection"),
            OSError("secret io"), FloodWait(1), SessionExpired(), RuntimeError("secret unknown"),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__):
                core = FakeCoreClient()
                streamer = FakeRecoveryStreamer(errors={(CHAT_ID, 901): error})
                response = self.request(core, streamer)
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json(), {"detail": "Viewer Cache media is unavailable"})
                self.assertNotIn("secret", response.text)
                self.assertEqual(core.health_calls, [])
                self.assertEqual(core.recover_calls, [])

    def test_malformed_range_is_rejected_before_missing_cache_recovery(self):
        core = FakeCoreClient()
        streamer = FakeRecoveryStreamer(missing={(CHAT_ID, 901)})
        response = self.request(core, streamer, range_value="bytes=99-")
        self.assertEqual(response.status_code, 416)
        self.assertEqual(core.health_calls, [])
        self.assertEqual(core.recover_calls, [])
        self.assertEqual(streamer.property_calls, [])

    def test_recovery_or_locator_failure_stops_without_second_attempt(self):
        cases = (
            FakeCoreClient(recover_error=CoreClientError(CoreErrorCode.CACHE_COPY_UNAVAILABLE)),
            FakeCoreClient(locator_error=CoreClientError(CoreErrorCode.CORE_UNAVAILABLE)),
        )
        for core in cases:
            with self.subTest(core=core):
                response = self.request(core, FakeRecoveryStreamer(missing={(CHAT_ID, 901)}))
                self.assertEqual(response.status_code, 503)
                self.assertEqual(len(core.health_calls), 1)
                self.assertEqual(len(core.recover_calls), 1)
                self.assertLessEqual(len(core.get_calls), 1)

    def test_recovery_timeout_has_no_second_key_locator_or_playback_retry(self):
        core = FakeCoreClient(
            recover_error=CoreClientError(CoreErrorCode.CORE_UNAVAILABLE)
        )
        streamer = FakeRecoveryStreamer(missing={(CHAT_ID, 901)})
        response = self.request(core, streamer, range_value="bytes=0-0")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json(), {"detail": "Viewer Cache media is unavailable"}
        )
        self.assertEqual(core.health_calls, [SOURCE_ID])
        self.assertEqual(len(core.recover_calls), 1)
        self.assertEqual(len({key for _source, key in core.recover_calls}), 1)
        self.assertEqual(core.get_calls, [])
        self.assertEqual(streamer.byte_calls, [])

    def test_invalid_recovery_response_stops_before_locator_refresh(self):
        core = FakeCoreClient(
            recover_error=CoreClientError(CoreErrorCode.INVALID_CORE_RESPONSE)
        )
        response = self.request(
            core,
            FakeRecoveryStreamer(missing={(CHAT_ID, 901)}),
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(len(core.recover_calls), 1)
        self.assertEqual(core.get_calls, [])

    def test_retry_missing_exhausts_budget_without_second_control_sequence(self):
        core = FakeCoreClient()
        streamer = FakeRecoveryStreamer(missing={(CHAT_ID, 901), (CHAT_ID, 1901)})
        response = self.request(core, streamer)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(len(core.health_calls), 1)
        self.assertEqual(len(core.recover_calls), 1)
        self.assertEqual(len(core.get_calls), 1)

    def test_subsequent_request_uses_repaired_locator_without_new_recovery(self):
        core = FakeCoreClient()
        streamer = FakeRecoveryStreamer(missing={(CHAT_ID, 901)})
        first = self.request(core, streamer, range_value="bytes=0-0")
        core.initial = core.fresh
        second = self.request(core, streamer, range_value="bytes=0-0")
        self.assertEqual(first.content, b"A")
        self.assertEqual(second.content, b"A")
        self.assertEqual(len(core.health_calls), 1)
        self.assertEqual(len(core.recover_calls), 1)
        self.assertEqual(len(core.get_calls), 1)

    def test_recovery_is_pre_body_only_and_never_wraps_generator(self):
        route_source = inspect.getsource(stream_routes.core_cache_stream_handler)
        self.assertLess(route_source.index("check_cache_health"), route_source.index("StreamingResponse"))
        generator_source = inspect.getsource(stream_routes.virtual_stream_generator)
        self.assertNotIn("check_cache_health", generator_source)
        self.assertNotIn("recover_cache", generator_source)


class RecoveryBoundaryRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_materialization_and_listing_do_not_use_recovery(self):
        ensure_source = inspect.getsource(TLCoreClient.ensure_cache)
        self.assertIn("materialize_cache", ensure_source)
        self.assertNotIn("check_cache_health", ensure_source)
        self.assertNotIn("recover_cache", ensure_source)

        fake = SimpleNamespace(
            discover_sources=AsyncMock(return_value=()),
            check_cache_health=AsyncMock(),
            recover_cache=AsyncMock(),
        )
        with patch.object(stremio_routes, "get_core_client", return_value=fake):
            self.assertEqual(await stremio_routes._core_streams_for("movie", "tt0133093"), [])
        fake.discover_sources.assert_awaited_once()
        fake.check_cache_health.assert_not_awaited()
        fake.recover_cache.assert_not_awaited()

    async def test_public_url_and_playback_authority_remain_source_only(self):
        route_source = inspect.getsource(stream_routes.core_cache_stream_handler).lower()
        self.assertNotIn("origin", route_source)
        self.assertNotIn("replica_id", route_source)
        self.assertNotIn("chat_id", route_source)
        self.assertNotIn("message_id", route_source)


if __name__ == "__main__":
    unittest.main()
