import asyncio
import os
import subprocess
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID

import httpx
from starlette.requests import Request

from Backend.core_client import CoreClientError, CoreErrorCode, DiscoveredSource, TLCoreClient
from Backend.core_discovery import core_requested_identity, core_source_stream
from Backend.core_playback import CorePlaybackNotConfigured
from Backend.fastapi.routes import stremio_routes


SOURCE_A = UUID("11111111-1111-1111-1111-111111111111")
SOURCE_B = UUID("22222222-2222-2222-2222-222222222222")
CREDENTIAL = "private-core-credential"


def requested_response(*, episode=None):
    return {
        "raw_id": "tt0133093" if episode is None else "tt0944947:1:1",
        "provider": "IMDB",
        "namespace": "MOVIE" if episode is None else "SERIES",
        "external_id": "tt0133093" if episode is None else "tt0944947",
        "episode": episode,
    }


def source_payload(source_id=SOURCE_A, filename="release.mkv"):
    return {
        "source_id": str(source_id),
        "original_filename": filename,
        "total_size_bytes": 1536,
        "expected_part_count": 2,
        "created_at": "2026-08-28T01:02:03Z",
        "published_at": "2026-08-28T01:03:04+00:00",
    }


def discovery_payload(sources=None):
    return {
        "requested": requested_response(),
        "canonical": {
            "media_id": "33333333-3333-3333-3333-333333333333",
            "media_type": "MOVIE",
            "episode_id": None,
        },
        "sources": [source_payload()] if sources is None else sources,
    }


class IdentityAdapterTests(unittest.TestCase):
    def test_imdb_movie_identity_is_exact(self):
        self.assertEqual(
            core_requested_identity("movie", "tt0133093"),
            {
                "provider": "IMDB",
                "namespace": "MOVIE",
                "external_id": "tt0133093",
                "raw_requested_id": "tt0133093",
                "media_type_hint": "MOVIE",
            },
        )

    def test_imdb_episode_preserves_coordinates(self):
        payload = core_requested_identity("series", "tt0944947:2:7")
        self.assertEqual(payload["external_id"], "tt0944947")
        self.assertEqual(
            payload["episode"],
            {
                "kind": "SEASON_EPISODE",
                "season_number": 2,
                "episode_number": 7,
                "absolute_number": None,
            },
        )
        special = core_requested_identity("series", "tt0944947:0:1")
        self.assertEqual(special["episode"]["season_number"], 0)

    def test_kitsu_series_and_absolute_episode_are_supported(self):
        series = core_requested_identity("series", "kitsu:123")
        episode = core_requested_identity("series", "kitsu:123:9")
        self.assertEqual(series["external_id"], "123")
        self.assertNotIn("episode", series)
        self.assertEqual(episode["episode"]["absolute_number"], 9)
        self.assertEqual(episode["episode"]["kind"], "ABSOLUTE")

    def test_malformed_or_nondeterministic_identity_does_not_guess(self):
        for media_type, stremio_id in (
            ("movie", "not-imdb"),
            ("movie", "tt0133093:1:1"),
            ("series", "tt0944947:1:0"),
            ("movie", "tt0000000"),
            ("series", "kitsu:123:1:2"),
            ("series", "kitsu:not-a-number"),
        ):
            with self.subTest(stremio_id=stremio_id):
                self.assertIsNone(core_requested_identity(media_type, stremio_id))


class CoreDiscoveryClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_discovery_posts_once_with_bearer_and_preserves_order(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(
                200,
                request=request,
                json=discovery_payload(
                    [source_payload(SOURCE_A, "same.mkv"), source_payload(SOURCE_B, "same.mkv")]
                ),
            )

        client = TLCoreClient(
            "https://core.example.test",
            CREDENTIAL,
            transport=httpx.MockTransport(handler),
        )
        requested = core_requested_identity("movie", "tt0133093")
        try:
            sources = await client.discover_sources(requested)
        finally:
            await client.aclose()

        self.assertEqual([item.source_id for item in sources], [SOURCE_A, SOURCE_B])
        self.assertEqual(requests[0].method, "POST")
        self.assertEqual(requests[0].url.path, "/api/v1/stream/sources")
        self.assertEqual(requests[0].headers["Authorization"], f"Bearer {CREDENTIAL}")
        self.assertNotIn(CREDENTIAL, str(requests[0].url))
        self.assertEqual(__import__("json").loads(requests[0].content), requested)

    async def test_strict_response_rejects_extra_missing_or_malformed_fields(self):
        bad_payloads = []
        extra = discovery_payload()
        extra["sources"][0]["replica_id"] = str(SOURCE_B)
        bad_payloads.append(extra)
        missing = discovery_payload()
        del missing["sources"][0]["created_at"]
        bad_payloads.append(missing)
        malformed = discovery_payload()
        malformed["sources"][0]["source_id"] = "not-a-uuid"
        bad_payloads.append(malformed)
        naive_time = discovery_payload()
        naive_time["sources"][0]["created_at"] = "2026-08-28T01:02:03"
        bad_payloads.append(naive_time)

        for payload in bad_payloads:
            with self.subTest(payload=payload):
                client = TLCoreClient(
                    "https://core.example.test",
                    CREDENTIAL,
                    transport=httpx.MockTransport(
                        lambda request, payload=payload: httpx.Response(200, request=request, json=payload)
                    ),
                )
                try:
                    with self.assertRaises(CoreClientError) as caught:
                        await client.discover_sources({})
                finally:
                    await client.aclose()
                self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CORE_RESPONSE)


class SourcePresentationTests(unittest.TestCase):
    def test_entry_contains_only_viewer_safe_presentation_and_source_url(self):
        source = DiscoveredSource(
            SOURCE_A,
            "release-name.mkv",
            1536,
            2,
            datetime.now(timezone.utc),
            None,
        )
        entry = core_source_stream(source, "https://stream.example.test")
        self.assertEqual(set(entry), {"name", "title", "url"})
        self.assertEqual(entry["name"], "TL Core")
        self.assertIn("release-name.mkv", entry["title"])
        self.assertIn("1.50 KB", entry["title"])
        self.assertEqual(entry["url"], f"https://stream.example.test/stream/core/{SOURCE_A}")
        rendered = repr(entry)
        for forbidden in (CREDENTIAL, "chat_id", "thread_id", "message_id", "replica_id", "publisher"):
            self.assertNotIn(forbidden, rendered)


class RouteIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_app_imports_with_core_config_present(self):
        environment = os.environ.copy()
        environment["TL_CORE_BASE_URL"] = "https://core.example.test"
        environment["TL_CORE_CREDENTIAL"] = "startup-smoke-placeholder"
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import Backend.fastapi.main as main; "
                    "assert any(route.path == '/stremio/{token}/stream/{media_type}/{id}.json' "
                    "for route in main.stremio_router.routes); "
                    "assert any(route.path == '/stream/core/{source_id}' "
                    "for route in main.stream_router.routes)"
                ),
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    async def test_core_helper_calls_discovery_only_and_preserves_duplicate_filenames(self):
        now = datetime.now(timezone.utc)
        fake = SimpleNamespace(
            discover_sources=AsyncMock(
                return_value=(
                    DiscoveredSource(SOURCE_A, "same.mkv", 100, 1, now, now),
                    DiscoveredSource(SOURCE_B, "same.mkv", 200, 1, now, now),
                )
            ),
            ensure_cache=AsyncMock(),
            materialize_cache=AsyncMock(),
        )
        settings = SimpleNamespace(base_url="https://stream.example.test")
        with (
            patch.object(stremio_routes, "get_core_client", return_value=fake),
            patch.object(stremio_routes.SettingsManager, "current", return_value=settings),
        ):
            entries = await stremio_routes._core_streams_for("movie", "tt0133093")
        self.assertEqual([entry["url"].rsplit("/", 1)[-1] for entry in entries], [str(SOURCE_A), str(SOURCE_B)])
        fake.discover_sources.assert_awaited_once()
        fake.ensure_cache.assert_not_awaited()
        fake.materialize_cache.assert_not_awaited()

    async def test_absent_core_config_and_zero_sources_are_clean_noops(self):
        with patch.object(
            stremio_routes,
            "get_core_client",
            side_effect=CorePlaybackNotConfigured("not configured"),
        ):
            self.assertEqual(await stremio_routes._core_streams_for("movie", "tt0133093"), [])

        fake = SimpleNamespace(discover_sources=AsyncMock(return_value=()))
        with patch.object(stremio_routes, "get_core_client", return_value=fake):
            self.assertEqual(await stremio_routes._core_streams_for("movie", "tt0133093"), [])
        fake.discover_sources.assert_awaited_once()

    async def test_malformed_id_skips_core_and_core_failure_is_sanitized(self):
        with patch.object(stremio_routes, "get_core_client") as factory:
            self.assertEqual(await stremio_routes._core_streams_for("movie", "bad-id"), [])
            factory.assert_not_called()

        fake = SimpleNamespace(
            discover_sources=AsyncMock(
                side_effect=CoreClientError(CoreErrorCode.CORE_UNAVAILABLE)
            )
        )
        with (
            patch.object(stremio_routes, "get_core_client", return_value=fake),
            patch.object(stremio_routes.LOGGER, "warning") as warning,
        ):
            self.assertEqual(await stremio_routes._core_streams_for("movie", "tt0133093"), [])
        self.assertNotIn(CREDENTIAL, repr(warning.call_args))
        self.assertNotIn("traceback", repr(warning.call_args).lower())

    async def test_legacy_and_core_entries_coexist_without_core_reranking(self):
        request = Request({
            "type": "http",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "scheme": "http",
            "server": ("testserver", 80),
        })
        settings = SimpleNamespace(
            subscription=False,
            base_url="https://stream.example.test",
            http_proxy_url="",
            mediaflow_proxy=False,
            mediaflow_password="",
            show_proxy_and_non_proxy_both=False,
        )
        legacy = {
            "telegram": [{"id": "legacy-id", "name": "legacy.mkv", "quality": "1080p", "size": "1 GB"}]
        }
        core_entries = [
            {"name": "TL Core", "title": "first.mkv", "url": f"https://stream.example.test/stream/core/{SOURCE_A}"},
            {"name": "TL Core", "title": "second.mkv", "url": f"https://stream.example.test/stream/core/{SOURCE_B}"},
        ]
        with (
            patch.object(stremio_routes, "record_client", new=AsyncMock()),
            patch.object(stremio_routes.SettingsManager, "current", return_value=settings),
            patch.object(stremio_routes, "_title_allowed", new=AsyncMock(return_value=True)),
            patch.object(stremio_routes.db, "get_media_details", new=AsyncMock(return_value=legacy)),
            patch.object(stremio_routes, "_core_streams_for", new=AsyncMock(return_value=core_entries)),
        ):
            result = await stremio_routes.get_streams(
                "addon-token",
                "movie",
                "tt0133093",
                request,
                {"config": {}},
            )
            await asyncio.sleep(0)
        self.assertEqual(len(result["streams"]), 3)
        self.assertIn("/dl/addon-token/legacy-id/", result["streams"][0]["url"])
        self.assertEqual(result["streams"][1:], core_entries)


if __name__ == "__main__":
    unittest.main()
