import inspect
import os
import subprocess
import sys
import unittest
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

import Backend.core_playback as core_playback
import Backend.fastapi.routes.stream_routes as stream_routes
from Backend.core_client import CacheLocatorPart, CoreClientError, CoreErrorCode, ValidatedCacheLocator
from Backend.core_playback import (
    CorePlaybackNotConfigured,
    VirtualMediaDescriptor,
    build_core_stream_url,
    close_core_client,
    get_core_client,
    locator_to_virtual_media,
)


SOURCE_ID = UUID("11111111-1111-1111-1111-111111111111")
REPLICA_ID = UUID("22222222-2222-2222-2222-222222222222")


def locator(parts=None, total=9):
    return ValidatedCacheLocator(
        source_id=SOURCE_ID,
        cache_replica_id=REPLICA_ID,
        chat_id=-100123456,
        thread_id=77,
        total_size_bytes=total,
        parts=parts if parts is not None else (
            CacheLocatorPart(1, 901, 4),
            CacheLocatorPart(2, 902, 5),
        ),
    )


class FakeCoreClient:
    def __init__(self, result=None, error=None):
        self.result = result or locator()
        self.error = error
        self.calls = []

    async def ensure_cache(self, source_id):
        self.calls.append(source_id)
        if self.error is not None:
            raise self.error
        return self.result


class FakeViewerStreamer:
    def __init__(self, files=None):
        self.files = files or {
            (-100123456, 901): SimpleNamespace(
                data=b"ABCD", file_size=4, file_name="cache.mkv.001", mime_type="video/x-matroska"
            ),
            (-100123456, 902): SimpleNamespace(
                data=b"EFGHI", file_size=5, file_name="cache.mkv.002", mime_type="video/x-matroska"
            ),
        }
        self.property_calls = []
        self.byte_calls = []

    async def get_file_properties(self, chat_id, message_id):
        self.property_calls.append((chat_id, message_id))
        return self.files[(chat_id, message_id)]

    async def prefetch_stream(
        self,
        *,
        file_id,
        offset,
        first_part_cut,
        last_part_cut,
        part_count,
        chunk_size,
        chat_id,
        message_id,
        **_kwargs,
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


class CorePlaybackRouteTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(stream_routes.router)
        self.client = TestClient(app)

    def _request(self, path, *, core=None, streamer=None, headers=None, method="GET"):
        core = core or FakeCoreClient()
        streamer = streamer or FakeViewerStreamer()
        with (
            patch.object(stream_routes, "get_core_client", return_value=core),
            patch.object(stream_routes, "_get_core_userbot_streamer", return_value=streamer),
            patch.object(stream_routes, "select_best_client", side_effect=AssertionError("bot selection forbidden")),
        ):
            response = self.client.request(method, path, headers=headers or {})
        return response, core, streamer

    def test_valid_uuid_route_calls_ensure_cache(self):
        response, core, _ = self._request(f"/stream/core/{SOURCE_ID}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(core.calls, [SOURCE_ID])

    def test_malformed_uuid_is_rejected_before_core_call(self):
        core = FakeCoreClient()
        response, _, _ = self._request("/stream/core/not-a-uuid", core=core)
        self.assertEqual(response.status_code, 422)
        self.assertEqual(core.calls, [])

    def test_core_url_contains_only_source_playback_identity(self):
        url = build_core_stream_url("http://127.0.0.1:8000", SOURCE_ID)
        self.assertEqual(url, f"http://127.0.0.1:8000/stream/core/{SOURCE_ID}")
        for forbidden in ("chat", "thread", "message", "credential", "session", "token"):
            self.assertNotIn(forbidden, url.lower())
        with self.assertRaises(ValueError):
            build_core_stream_url("https://secret@core.example", SOURCE_ID)

    def test_core_playback_uses_viewer_user_streamer_not_bot_pool(self):
        viewer_streamer = FakeViewerStreamer()
        response, _, used = self._request(
            f"/stream/core/{SOURCE_ID}",
            streamer=viewer_streamer,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIs(used, viewer_streamer)
        self.assertEqual(
            viewer_streamer.property_calls,
            [(-100123456, 901), (-100123456, 902)],
        )

    def test_legacy_indexed_playback_selection_remains_bot_based(self):
        source = inspect.getsource(stream_routes.media_streamer)
        virtual_source = inspect.getsource(stream_routes.virtual_media_streamer)
        self.assertIn("select_best_client", source)
        self.assertIn("multi_clients[index]", source)
        self.assertIn("select_best_client", virtual_source)
        self.assertIn("multi_clients[index]", virtual_source)

    def test_full_multipart_response_has_exact_virtual_length(self):
        response, _, streamer = self._request(f"/stream/core/{SOURCE_ID}")
        self.assertEqual(response.content, b"ABCDEFGHI")
        self.assertEqual(response.headers["content-length"], "9")
        self.assertEqual(response.headers["accept-ranges"], "bytes")
        self.assertEqual([call[:2] for call in streamer.byte_calls], [(-100123456, 901), (-100123456, 902)])

    def test_one_part_source_reads_correctly(self):
        one_part_locator = locator((CacheLocatorPart(1, 901, 4),), total=4)
        core = FakeCoreClient(one_part_locator)
        streamer = FakeViewerStreamer({
            (-100123456, 901): SimpleNamespace(
                data=b"ABCD", file_size=4, file_name="single.mp4", mime_type="video/mp4"
            )
        })
        response, _, _ = self._request(f"/stream/core/{SOURCE_ID}", core=core, streamer=streamer)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"ABCD")

    def test_middle_range_within_one_part(self):
        response, _, streamer = self._request(
            f"/stream/core/{SOURCE_ID}", headers={"Range": "bytes=5-7"}
        )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"FGH")
        self.assertEqual(response.headers["content-range"], "bytes 5-7/9")
        self.assertEqual(response.headers["content-length"], "3")
        self.assertEqual([call[:2] for call in streamer.byte_calls], [(-100123456, 902)])

    def test_range_crossing_part_boundary(self):
        response, _, streamer = self._request(
            f"/stream/core/{SOURCE_ID}", headers={"Range": "bytes=2-6"}
        )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"CDEFG")
        self.assertEqual(response.headers["content-range"], "bytes 2-6/9")
        self.assertEqual(response.headers["content-length"], "5")
        self.assertEqual([call[:2] for call in streamer.byte_calls], [(-100123456, 901), (-100123456, 902)])

    def test_final_and_suffix_ranges(self):
        for range_value, expected in (("bytes=7-", b"HI"), ("bytes=-2", b"HI")):
            with self.subTest(range=range_value):
                response, _, _ = self._request(
                    f"/stream/core/{SOURCE_ID}", headers={"Range": range_value}
                )
                self.assertEqual(response.status_code, 206)
                self.assertEqual(response.content, expected)
                self.assertEqual(response.headers["content-range"], "bytes 7-8/9")
                self.assertEqual(response.headers["content-length"], "2")

    def test_unsatisfiable_range_preserves_existing_416_behavior(self):
        response, _, streamer = self._request(
            f"/stream/core/{SOURCE_ID}", headers={"Range": "bytes=99-"}
        )
        self.assertEqual(response.status_code, 416)
        self.assertEqual(response.headers["content-range"], "bytes */9")
        self.assertEqual(streamer.byte_calls, [])

    def test_head_returns_headers_without_media_bytes(self):
        response, _, streamer = self._request(
            f"/stream/core/{SOURCE_ID}", headers={"Range": "bytes=2-6"}, method="HEAD"
        )
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"")
        self.assertEqual(response.headers["content-range"], "bytes 2-6/9")
        self.assertEqual(streamer.byte_calls, [])

    def test_core_errors_are_sanitized(self):
        expectations = {
            CoreErrorCode.SOURCE_NOT_FOUND: (404, "Core source is unavailable"),
            CoreErrorCode.SOURCE_NOT_PUBLISHED: (404, "Core source is unavailable"),
            CoreErrorCode.CACHE_INVALID: (503, "Core cache is temporarily unavailable"),
            CoreErrorCode.CACHE_RECONCILIATION_REQUIRED: (503, "Core cache is temporarily unavailable"),
            CoreErrorCode.CACHE_MATERIALIZATION_FAILED: (503, "Core cache is temporarily unavailable"),
            CoreErrorCode.INVALID_CREDENTIAL: (503, "Core playback access is unavailable"),
            CoreErrorCode.ACCESS_REVOKED: (503, "Core playback access is unavailable"),
        }
        for code, (status, detail) in expectations.items():
            with self.subTest(code=code):
                response, _, streamer = self._request(
                    f"/stream/core/{SOURCE_ID}",
                    core=FakeCoreClient(error=CoreClientError(code)),
                )
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.json(), {"detail": detail})
                self.assertNotIn("credential", response.text.lower())
                self.assertEqual(streamer.property_calls, [])

    def test_missing_core_configuration_fails_cleanly(self):
        with patch.object(
            stream_routes,
            "get_core_client",
            side_effect=CorePlaybackNotConfigured("secret internal configuration detail"),
        ):
            response = self.client.get(f"/stream/core/{SOURCE_ID}")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "Core playback is not configured"})
        self.assertNotIn("secret", response.text)

    def test_missing_destination_message_has_no_origin_or_bot_fallback(self):
        streamer = FakeViewerStreamer()
        del streamer.files[(-100123456, 902)]
        with (
            patch.object(stream_routes, "get_core_client", return_value=FakeCoreClient()),
            patch.object(stream_routes, "_get_core_userbot_streamer", return_value=streamer),
            patch.object(stream_routes, "select_best_client", side_effect=AssertionError("bot fallback forbidden")),
        ):
            response = self.client.get(f"/stream/core/{SOURCE_ID}")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {"detail": "Viewer Cache media is unavailable"})
        self.assertNotIn("chat", response.text.lower())
        self.assertNotIn("message", response.text.lower())

    def test_resolved_size_change_fails_before_streaming(self):
        streamer = FakeViewerStreamer()
        streamer.files[(-100123456, 902)].file_size = 6
        response, _, streamer = self._request(f"/stream/core/{SOURCE_ID}", streamer=streamer)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(streamer.byte_calls, [])

    def test_route_has_no_topic_scan_history_search_or_file_write(self):
        source = inspect.getsource(stream_routes.core_cache_stream_handler).lower()
        for forbidden in (
            "get_forum_topics",
            "get_chat_history",
            "search_messages",
            "open(",
            "write(",
            "download_media",
            "multi_clients",
            "select_best_client",
            "streambot",
        ):
            self.assertNotIn(forbidden, source)

    def test_core_streamer_is_viewer_only_and_hides_locator_diagnostics(self):
        sentinel_client = object()
        sentinel_streamer = SimpleNamespace(client=sentinel_client)
        previous = stream_routes._core_userbot_streamer
        stream_routes._core_userbot_streamer = None
        try:
            with (
                patch.object(stream_routes.botmod, "Userbot", sentinel_client),
                patch.object(stream_routes, "ByteStreamer", return_value=sentinel_streamer) as factory,
            ):
                self.assertIs(stream_routes._get_core_userbot_streamer(), sentinel_streamer)
                self.assertIs(stream_routes._get_core_userbot_streamer(), sentinel_streamer)
            factory.assert_called_once_with(
                sentinel_client,
                stream_routes.USERBOT_CLIENT_INDEX,
                log_stats=False,
                expose_locator_metadata=False,
            )
        finally:
            stream_routes._core_userbot_streamer = previous


class LocatorAdapterTests(unittest.TestCase):
    def test_locator_translates_to_ordered_viewer_cache_parts(self):
        descriptor = locator_to_virtual_media(locator(parts=(
            CacheLocatorPart(2, 902, 5),
            CacheLocatorPart(1, 901, 4),
        )))
        self.assertEqual([part.part_number for part in descriptor.parts], [1, 2])
        self.assertEqual([part.chat_id for part in descriptor.parts], [-100123456, -100123456])
        self.assertEqual([part.msg_id for part in descriptor.parts], [901, 902])
        self.assertEqual([part.size_bytes for part in descriptor.parts], [4, 5])
        self.assertEqual(descriptor.total_size_bytes, 9)
        self.assertEqual(
            descriptor.as_resolver_payload(),
            [
                {"chat_id": -100123456, "msg_id": 901, "part_number": 1, "size_bytes": 4},
                {"chat_id": -100123456, "msg_id": 902, "part_number": 2, "size_bytes": 5},
            ],
        )

    def test_adapter_rejects_missing_duplicate_and_size_mismatch(self):
        invalid = (
            locator((CacheLocatorPart(2, 902, 9),), total=9),
            locator((CacheLocatorPart(1, 901, 4), CacheLocatorPart(1, 902, 5))),
            locator(total=10),
            locator((CacheLocatorPart(1, 901, 4), CacheLocatorPart(2, 901, 5))),
        )
        for item in invalid:
            with self.subTest(locator=item):
                with self.assertRaises(ValueError):
                    locator_to_virtual_media(item)

    def test_descriptor_has_no_origin_coordinates(self):
        names = {field.name for field in fields(VirtualMediaDescriptor)}
        self.assertEqual(
            names,
            {"source_id", "cache_replica_id", "chat_id", "thread_id", "total_size_bytes", "parts"},
        )
        self.assertTrue({"origin_chat_id", "uploader_chat_id", "origin_message_id"}.isdisjoint(names))


class CoreClientLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        await close_core_client()

    async def test_absent_core_config_does_not_break_legacy_import(self):
        await close_core_client()
        with patch.object(core_playback.TLCore, "BASE_URL", ""), patch.object(core_playback.TLCore, "CREDENTIAL", ""):
            with self.assertRaises(CorePlaybackNotConfigured):
                get_core_client()
        self.assertTrue(callable(stream_routes.stream_handler))

    async def test_full_app_imports_with_core_config_absent(self):
        environment = os.environ.copy()
        environment.pop("TL_CORE_BASE_URL", None)
        environment.pop("TL_CORE_CREDENTIAL", None)
        repository_root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import Backend.fastapi.main as main; "
                    "assert any(route.path == '/stream/core/{source_id}' "
                    "for route in main.stream_router.routes); "
                    "print('CORE_ABSENT_APP_IMPORT_PASS')"
                ),
            ],
            cwd=repository_root,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CORE_ABSENT_APP_IMPORT_PASS", result.stdout)

    async def test_core_client_is_reused_and_closed(self):
        await close_core_client()

        class FakeReusableClient:
            def __init__(self):
                self.closed = 0

            async def aclose(self):
                self.closed += 1

        fake = FakeReusableClient()
        with (
            patch.object(core_playback.TLCore, "BASE_URL", "https://core.example"),
            patch.object(core_playback.TLCore, "CREDENTIAL", "test-credential"),
            patch.object(core_playback.TLCoreClient, "from_config", return_value=fake) as factory,
        ):
            self.assertIs(get_core_client(), fake)
            self.assertIs(get_core_client(), fake)
            factory.assert_called_once_with()
            await close_core_client()
        self.assertEqual(fake.closed, 1)


class CoreStreamerPrivacyTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_message_warning_does_not_include_destination_locator(self):
        streamer = object.__new__(stream_routes.ByteStreamer)
        streamer.client = object()
        streamer._file_id_cache = {}
        streamer.expose_locator_metadata = False
        with (
            patch("Backend.helper.custom_dl.get_file_ids", new=AsyncMock(return_value=None)),
            patch("Backend.helper.custom_dl.LOGGER.warning") as warning,
        ):
            with self.assertRaises(Exception):
                await streamer.get_file_properties(-100123456, 901)
        rendered = " ".join(str(value) for call in warning.call_args_list for value in call.args)
        self.assertNotIn("-100123456", rendered)
        self.assertNotIn("901", rendered)


if __name__ == "__main__":
    unittest.main()
