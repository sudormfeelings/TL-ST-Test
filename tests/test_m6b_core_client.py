import inspect
import io
import logging
import unittest
from dataclasses import fields
from uuid import UUID, uuid4

import httpx

from Backend.core_client import (
    CoreClientError,
    CoreErrorCode,
    TLCoreClient,
    ValidatedCacheLocator,
)


SOURCE_ID = UUID("11111111-1111-1111-1111-111111111111")
REPLICA_ID = UUID("22222222-2222-2222-2222-222222222222")
MEDIA_ID = UUID("33333333-3333-3333-3333-333333333333")
MATERIALIZATION_ID = UUID("44444444-4444-4444-4444-444444444444")
CREDENTIAL = "private-stream-installation-credential"


def bootstrap_payload():
    return {
        "api_version": "v1",
        "viewer": {"storage_ready": True},
        "telegram_cache": {"chat_id": -100123456, "thread_id": 77},
        "capabilities": {
            "cache_materialization": True,
            "multipart": True,
            "http_range": False,
        },
    }


def locator_payload(parts=None, *, expected=2, total=30):
    return {
        "source_id": str(SOURCE_ID),
        "cache_replica_id": str(REPLICA_ID),
        "status": "READY",
        "content": {
            "media_id": str(MEDIA_ID),
            "media_type": "MOVIE",
            "episode_id": None,
        },
        "cache": {
            "expected_part_count": expected,
            "total_size_bytes": total,
            "parts": parts if parts is not None else [
                {"part_number": 1, "telegram_message_id": 901, "size_bytes": 10},
                {"part_number": 2, "telegram_message_id": 902, "size_bytes": 20},
            ],
        },
    }


def materialization_payload():
    return {
        "materialization_id": str(MATERIALIZATION_ID),
        "source_id": str(SOURCE_ID),
        "cache_replica_id": str(REPLICA_ID),
        "status": "READY",
        "idempotent": False,
        "materialized": True,
        "expected_part_count": 2,
        "cache": {
            "parts": [
                {"part_number": 1, "telegram_message_id": 901, "size_bytes": 10},
                {"part_number": 2, "telegram_message_id": 902, "size_bytes": 20},
            ]
        },
    }


def error_response(request, code, status=409):
    return httpx.Response(status, request=request, json={"detail": {"code": code}})


class CoreClientTests(unittest.IsolatedAsyncioTestCase):
    async def _client(self, handler):
        return TLCoreClient(
            "https://core.example.test",
            CREDENTIAL,
            transport=httpx.MockTransport(handler),
        )

    async def test_bearer_credential_is_header_only_and_not_in_url(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, request=request, json=bootstrap_payload())

        client = await self._client(handler)
        try:
            await client.bootstrap()
        finally:
            await client.aclose()

        self.assertEqual(requests[0].headers["Authorization"], f"Bearer {CREDENTIAL}")
        self.assertNotIn(CREDENTIAL, str(requests[0].url))

    async def test_credential_is_not_logged_or_rendered_in_errors(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root = logging.getLogger()
        root.addHandler(handler)

        def failing(request):
            return httpx.Response(500, request=request, text=f"internal {CREDENTIAL}")

        client = await self._client(failing)
        try:
            with self.assertRaises(CoreClientError) as caught:
                await client.bootstrap()
        finally:
            await client.aclose()
            root.removeHandler(handler)
        self.assertEqual(caught.exception.code, CoreErrorCode.CORE_REQUEST_FAILED)
        rendered = stream.getvalue() + str(caught.exception) + repr(caught.exception)
        self.assertNotIn(CREDENTIAL, rendered)
        self.assertNotIn("internal", rendered)

    async def test_bootstrap_parses_own_cache_topology_and_capabilities(self):
        client = await self._client(
            lambda request: httpx.Response(200, request=request, json=bootstrap_payload())
        )
        try:
            result = await client.bootstrap()
        finally:
            await client.aclose()
        self.assertEqual((result.chat_id, result.thread_id), (-100123456, 77))
        self.assertTrue(result.capabilities.cache_materialization)
        self.assertTrue(result.capabilities.multipart)
        self.assertFalse(result.capabilities.http_range)

    async def test_bootstrap_missing_storage_is_rejected(self):
        payload = bootstrap_payload()
        payload["viewer"]["storage_ready"] = False
        client = await self._client(
            lambda request: httpx.Response(200, request=request, json=payload)
        )
        try:
            with self.assertRaises(CoreClientError) as caught:
                await client.bootstrap()
        finally:
            await client.aclose()
        self.assertEqual(caught.exception.code, CoreErrorCode.STREAM_STORAGE_NOT_CONFIGURED)

    async def test_bootstrap_missing_chat_or_thread_is_rejected(self):
        for key in ("chat_id", "thread_id"):
            with self.subTest(key=key):
                payload = bootstrap_payload()
                payload["telegram_cache"][key] = 0
                client = await self._client(
                    lambda request, payload=payload: httpx.Response(200, request=request, json=payload)
                )
                try:
                    with self.assertRaises(CoreClientError) as caught:
                        await client.bootstrap()
                finally:
                    await client.aclose()
                self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CORE_RESPONSE)

    async def test_ready_cache_returns_without_materialization(self):
        methods = []

        def handler(request):
            methods.append(request.method)
            payload = bootstrap_payload() if request.url.path.endswith("/bootstrap") else locator_payload()
            return httpx.Response(200, request=request, json=payload)

        client = await self._client(handler)
        try:
            locator = await client.ensure_cache(SOURCE_ID)
        finally:
            await client.aclose()
        self.assertEqual(methods, ["GET", "GET"])
        self.assertEqual(locator.chat_id, -100123456)

    async def test_cache_not_ready_materializes_once_then_gets_locator(self):
        paths = []
        cache_gets = 0

        def handler(request):
            nonlocal cache_gets
            paths.append((request.method, request.url.path))
            if request.url.path.endswith("/bootstrap"):
                return httpx.Response(200, request=request, json=bootstrap_payload())
            if request.method == "POST":
                return httpx.Response(201, request=request, json=materialization_payload())
            cache_gets += 1
            if cache_gets == 1:
                return error_response(request, "CACHE_NOT_READY", 404)
            return httpx.Response(200, request=request, json=locator_payload())

        client = await self._client(handler)
        try:
            locator = await client.ensure_cache(SOURCE_ID, idempotency_key="stable-key")
        finally:
            await client.aclose()
        self.assertEqual(sum(method == "POST" for method, _ in paths), 1)
        self.assertEqual(cache_gets, 2)
        self.assertEqual(locator.cache_replica_id, REPLICA_ID)

    async def test_uncertain_post_retry_reuses_idempotency_key(self):
        post_keys = []
        post_calls = 0

        def handler(request):
            nonlocal post_calls
            if request.url.path.endswith("/bootstrap"):
                return httpx.Response(200, request=request, json=bootstrap_payload())
            if request.method == "POST":
                post_calls += 1
                post_keys.append(request.headers["Idempotency-Key"])
                if post_calls == 1:
                    raise httpx.ConnectError("uncertain", request=request)
                return httpx.Response(200, request=request, json=materialization_payload())
            return error_response(request, "CACHE_NOT_READY", 404) if post_calls == 0 else httpx.Response(
                200, request=request, json=locator_payload()
            )

        client = await self._client(handler)
        try:
            await client.ensure_cache(SOURCE_ID, idempotency_key="same-logical-attempt")
        finally:
            await client.aclose()
        self.assertEqual(post_keys, ["same-logical-attempt", "same-logical-attempt"])

    async def test_generated_idempotency_key_is_uuid(self):
        keys = []
        post_seen = False

        def handler(request):
            nonlocal post_seen
            if request.url.path.endswith("/bootstrap"):
                return httpx.Response(200, request=request, json=bootstrap_payload())
            if request.method == "POST":
                post_seen = True
                keys.append(request.headers["Idempotency-Key"])
                return httpx.Response(201, request=request, json=materialization_payload())
            if not post_seen:
                return error_response(request, "CACHE_NOT_READY", 404)
            return httpx.Response(200, request=request, json=locator_payload())

        client = await self._client(handler)
        try:
            await client.ensure_cache(SOURCE_ID)
        finally:
            await client.aclose()
        self.assertEqual(str(UUID(keys[0])), keys[0])

    async def test_unrelated_core_errors_never_materialize(self):
        for code in (
            "SOURCE_NOT_FOUND",
            "SOURCE_NOT_PUBLISHED",
            "CACHE_INVALID",
            "INVALID_CREDENTIAL",
            "ACCESS_REVOKED",
            "STREAM_REQUIRED",
            "CACHE_RECONCILIATION_REQUIRED",
        ):
            with self.subTest(code=code):
                methods = []

                def handler(request, code=code):
                    methods.append(request.method)
                    if request.url.path.endswith("/bootstrap"):
                        return httpx.Response(200, request=request, json=bootstrap_payload())
                    return error_response(request, code, 401 if code == "INVALID_CREDENTIAL" else 409)

                client = await self._client(handler)
                try:
                    with self.assertRaises(CoreClientError) as caught:
                        await client.ensure_cache(SOURCE_ID)
                finally:
                    await client.aclose()
                self.assertEqual(caught.exception.code.value, code)
                self.assertNotIn("POST", methods)

    async def test_bare_401_maps_to_invalid_credential(self):
        client = await self._client(
            lambda request: httpx.Response(401, request=request, text="not JSON")
        )
        try:
            with self.assertRaises(CoreClientError) as caught:
                await client.bootstrap()
        finally:
            await client.aclose()
        self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CREDENTIAL)

    async def test_materialization_must_report_ready(self):
        payload = materialization_payload()
        payload["status"] = "FAILED"
        client = await self._client(
            lambda request: httpx.Response(200, request=request, json=payload)
        )
        try:
            with self.assertRaises(CoreClientError) as caught:
                await client.materialize_cache(SOURCE_ID, "key")
        finally:
            await client.aclose()
        self.assertEqual(caught.exception.code, CoreErrorCode.CACHE_MATERIALIZATION_FAILED)

    async def _assert_invalid_locator(self, payload):
        def handler(request):
            response_payload = bootstrap_payload() if request.url.path.endswith("/bootstrap") else payload
            return httpx.Response(200, request=request, json=response_payload)

        client = await self._client(handler)
        try:
            with self.assertRaises(CoreClientError) as caught:
                await client.get_cache(SOURCE_ID)
        finally:
            await client.aclose()
        self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CORE_RESPONSE)

    async def test_multipart_parts_are_sorted_by_part_number(self):
        payload = locator_payload(parts=[
            {"part_number": 2, "telegram_message_id": 902, "size_bytes": 20},
            {"part_number": 1, "telegram_message_id": 901, "size_bytes": 10},
        ])

        def handler(request):
            return httpx.Response(
                200,
                request=request,
                json=bootstrap_payload() if request.url.path.endswith("/bootstrap") else payload,
            )

        client = await self._client(handler)
        try:
            locator = await client.get_cache(SOURCE_ID)
        finally:
            await client.aclose()
        self.assertEqual([part.part_number for part in locator.parts], [1, 2])

    async def test_missing_part_is_rejected(self):
        await self._assert_invalid_locator(locator_payload(parts=[
            {"part_number": 1, "telegram_message_id": 901, "size_bytes": 10},
        ]))

    async def test_duplicate_part_number_is_rejected(self):
        await self._assert_invalid_locator(locator_payload(parts=[
            {"part_number": 1, "telegram_message_id": 901, "size_bytes": 10},
            {"part_number": 1, "telegram_message_id": 902, "size_bytes": 20},
        ]))

    async def test_non_positive_part_size_is_rejected(self):
        await self._assert_invalid_locator(locator_payload(parts=[
            {"part_number": 1, "telegram_message_id": 901, "size_bytes": 0},
            {"part_number": 2, "telegram_message_id": 902, "size_bytes": 30},
        ]))

    async def test_total_size_mismatch_is_rejected(self):
        await self._assert_invalid_locator(locator_payload(total=31))

    async def test_duplicate_destination_message_id_is_rejected(self):
        await self._assert_invalid_locator(locator_payload(parts=[
            {"part_number": 1, "telegram_message_id": 901, "size_bytes": 10},
            {"part_number": 2, "telegram_message_id": 901, "size_bytes": 20},
        ]))

    async def test_unexpected_origin_field_is_rejected(self):
        payload = locator_payload()
        payload["origin_chat_id"] = -100999
        await self._assert_invalid_locator(payload)

    def test_internal_locator_contains_no_origin_or_uploader_fields(self):
        names = {field.name for field in fields(ValidatedCacheLocator)}
        self.assertEqual(
            names,
            {"source_id", "cache_replica_id", "chat_id", "thread_id", "total_size_bytes", "parts"},
        )
        for forbidden in (
            "origin_replica_id",
            "origin_chat_id",
            "origin_thread_id",
            "uploader_chat_id",
            "uploader_thread_id",
            "uploader_message_id",
        ):
            self.assertNotIn(forbidden, names)

    def test_core_client_has_no_topic_scan_or_origin_lookup(self):
        source = inspect.getsource(TLCoreClient).lower()
        for forbidden in (
            "get_forum_topics",
            "get_chat_history",
            "search_messages",
            "upload topic",
            "control topic",
            "origin_chat",
            "uploader_chat",
        ):
            self.assertNotIn(forbidden, source)

    def test_existing_local_streaming_modules_still_import(self):
        import Backend.helper.custom_dl  # noqa: F401
        import Backend.helper.virtual_dl  # noqa: F401


if __name__ == "__main__":
    unittest.main()
