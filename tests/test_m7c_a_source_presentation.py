import inspect
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import UUID

import httpx

from Backend.core_client import (
    CoreClientError,
    CoreErrorCode,
    DiscoveredSource,
    DiscoveredSourcePresentation,
    PresentationAudioCodec,
    PresentationHdrFormat,
    PresentationResolution,
    PresentationSourceType,
    PresentationVideoCodec,
    TLCoreClient,
)
from Backend.core_discovery import core_source_stream
from Backend.fastapi.routes import stremio_routes


SOURCE_A = UUID("11111111-1111-1111-1111-111111111111")
SOURCE_B = UUID("22222222-2222-2222-2222-222222222222")


def presentation_payload(**overrides):
    payload = {
        "release_name": "Movie.Release.2160p.WEB-DL.HEVC.HDR10Plus.EAC3-GROUP",
        "container": "MKV",
        "resolution": "2160P",
        "video_codec": "HEVC",
        "video_profile": None,
        "hdr_format": "HDR10_PLUS",
        "audio_codec": "EAC3",
        "audio_channels": "5.1",
        "audio_layout": None,
        "audio_languages": ["en", "ja"],
        "subtitle_languages": ["en", "th"],
        "edition": None,
        "release_group": "GROUP",
        "source_type": "WEB_DL",
        "is_remux": False,
        "is_hdr": True,
        "is_dolby_vision": False,
        "is_dual_audio": False,
        "is_multi_audio": True,
    }
    payload.update(overrides)
    return payload


def source_payload(source_id=SOURCE_A, *, presentation=...):
    payload = {
        "source_id": str(source_id),
        "original_filename": "fallback-original.mkv",
        "total_size_bytes": 12 * 1024**3,
        "expected_part_count": 2,
        "created_at": "2026-08-28T01:02:03Z",
        "published_at": "2026-08-28T01:03:04Z",
    }
    if presentation is not ...:
        payload["presentation"] = presentation
    return payload


def discovery_payload(sources):
    return {
        "requested": {
            "raw_id": "tt0133093",
            "provider": "IMDB",
            "namespace": "MOVIE",
            "external_id": "tt0133093",
            "episode": None,
        },
        "canonical": {
            "media_id": "33333333-3333-3333-3333-333333333333",
            "media_type": "MOVIE",
            "episode_id": None,
        },
        "sources": sources,
    }


def presentation(**overrides):
    values = {
        "release_name": None,
        "container": None,
        "resolution": None,
        "video_codec": None,
        "video_profile": None,
        "hdr_format": None,
        "audio_codec": None,
        "audio_channels": None,
        "audio_layout": None,
        "audio_languages": None,
        "subtitle_languages": None,
        "edition": None,
        "release_group": None,
        "source_type": None,
        "is_remux": None,
        "is_hdr": None,
        "is_dolby_vision": None,
        "is_dual_audio": None,
        "is_multi_audio": None,
    }
    values.update(overrides)
    return DiscoveredSourcePresentation(**values)


def source(source_id=SOURCE_A, *, filename="fallback-original.mkv", size=12 * 1024**3, parts=2, item=None):
    return DiscoveredSource(
        source_id=source_id,
        original_filename=filename,
        total_size_bytes=size,
        expected_part_count=parts,
        created_at=datetime.now(timezone.utc),
        published_at=None,
        presentation=item,
    )


class EnrichedDiscoveryDtoTests(unittest.IsolatedAsyncioTestCase):
    async def _discover(self, payload):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(200, request=request, json=payload)

        client = TLCoreClient(
            "https://core.example.test",
            "private-credential",
            transport=httpx.MockTransport(handler),
        )
        try:
            result = await client.discover_sources({}, include_presentation=True)
        finally:
            await client.aclose()
        return result, requests

    async def test_enriched_request_is_single_call_and_parses_public_dto(self):
        sources, requests = await self._discover(
            discovery_payload([source_payload(presentation=presentation_payload())])
        )
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].url.path, "/api/v1/stream/sources")
        self.assertEqual(requests[0].url.query, b"include_presentation=true")
        parsed = sources[0].presentation
        self.assertEqual(parsed.resolution, PresentationResolution.UHD_2160P)
        self.assertEqual(parsed.video_codec, PresentationVideoCodec.HEVC)
        self.assertEqual(parsed.hdr_format, PresentationHdrFormat.HDR10_PLUS)
        self.assertEqual(parsed.audio_codec, PresentationAudioCodec.EAC3)
        self.assertEqual(parsed.source_type, PresentationSourceType.WEB_DL)
        self.assertEqual(parsed.audio_languages, ("en", "ja"))

    async def test_null_presentation_is_accepted(self):
        sources, _ = await self._discover(
            discovery_payload([source_payload(presentation=None)])
        )
        self.assertIsNone(sources[0].presentation)

    async def test_unknown_or_forbidden_presentation_fields_are_rejected(self):
        for forbidden in ("unexpected", "replica_id", "chat_id", "telegram_message_id", "provenance"):
            with self.subTest(field=forbidden):
                raw = presentation_payload()
                raw[forbidden] = "forbidden"
                with self.assertRaises(CoreClientError) as caught:
                    await self._discover(discovery_payload([source_payload(presentation=raw)]))
                self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CORE_RESPONSE)

    async def test_invalid_enum_boolean_and_language_values_are_rejected(self):
        invalid = (
            presentation_payload(resolution="4320P"),
            presentation_payload(video_codec="MPEG2"),
            presentation_payload(hdr_format="UNKNOWN"),
            presentation_payload(source_type="CAM"),
            presentation_payload(is_hdr="true"),
            presentation_payload(audio_languages=["en", ""]),
        )
        for raw in invalid:
            with self.subTest(raw=raw):
                with self.assertRaises(CoreClientError) as caught:
                    await self._discover(discovery_payload([source_payload(presentation=raw)]))
                self.assertEqual(caught.exception.code, CoreErrorCode.INVALID_CORE_RESPONSE)


class PresentationDisplayTests(unittest.TestCase):
    def test_rich_presentation_is_scanable_and_keeps_size_parts_and_release(self):
        entry = core_source_stream(
            source(item=presentation(
                release_name="Movie.Release.Name",
                resolution=PresentationResolution.UHD_2160P,
                source_type=PresentationSourceType.WEB_DL,
                video_codec=PresentationVideoCodec.HEVC,
                hdr_format=PresentationHdrFormat.HDR10_PLUS,
                audio_codec=PresentationAudioCodec.EAC3,
                audio_channels="5.1",
                audio_languages=("en", "ja"),
                subtitle_languages=("th",),
                is_multi_audio=True,
            )),
            "https://stream.example.test",
        )
        self.assertEqual(entry["name"], "TL Core · 2160P")
        for expected in (
            "Movie.Release.Name", "2160P", "WEB-DL", "HEVC", "HDR10+",
            "EAC3 5.1", "Multi-Audio", "Audio: EN/JA", "Subs: TH", "12.00 GB", "2 parts",
        ):
            self.assertIn(expected, entry["title"])

    def test_partial_1080p_avc_and_dolby_vision_present_cleanly(self):
        item = presentation(
            resolution=PresentationResolution.FHD_1080P,
            video_codec=PresentationVideoCodec.AVC,
            hdr_format=PresentationHdrFormat.DOLBY_VISION,
        )
        entry = core_source_stream(source(item=item, parts=1), "https://stream.example.test")
        self.assertEqual(entry["name"], "TL Core · 1080P")
        self.assertIn("1080P · AVC · DV", entry["title"])
        self.assertNotIn("None", entry["title"])
        self.assertNotIn("parts", entry["title"])

    def test_typed_hdr_boolean_fallbacks_do_not_infer_from_filename(self):
        dv = core_source_stream(
            source(filename="plain.mkv", item=presentation(is_dolby_vision=True)),
            "https://stream.example.test",
        )
        hdr = core_source_stream(
            source(filename="plain.mkv", item=presentation(is_hdr=True)),
            "https://stream.example.test",
        )
        unknown = core_source_stream(
            source(filename="looks-like-HDR10.mkv", item=presentation()),
            "https://stream.example.test",
        )
        self.assertIn("\nDV\n", dv["title"])
        self.assertIn("\nHDR\n", hdr["title"])
        self.assertNotIn("\nHDR10\n", unknown["title"])

    def test_remux_is_displayed_once_without_bluray_duplication(self):
        entry = core_source_stream(
            source(item=presentation(source_type=PresentationSourceType.BLURAY, is_remux=True)),
            "https://stream.example.test",
        )
        self.assertEqual(entry["title"].count("REMUX"), 1)
        self.assertNotIn("BluRay", entry["title"])

    def test_audio_enum_labels_are_human_readable(self):
        cases = (
            (PresentationAudioCodec.DTS_HD, "DTS-HD 5.1"),
            (PresentationAudioCodec.DTS_HD_MA, "DTS-HD MA 5.1"),
            (PresentationAudioCodec.TRUEHD, "TRUEHD 7.1"),
        )
        for codec, expected in cases:
            with self.subTest(codec=codec):
                entry = core_source_stream(
                    source(item=presentation(audio_codec=codec, audio_channels=expected.rsplit(" ", 1)[-1])),
                    "https://stream.example.test",
                )
                self.assertIn(expected, entry["title"])

    def test_release_name_and_null_presentation_fallbacks(self):
        rich = core_source_stream(
            source(filename="original.mkv", item=presentation(release_name="Preferred.Release")),
            "https://stream.example.test",
        )
        fallback = core_source_stream(
            source(filename="original.mkv", size=1536, parts=1),
            "https://stream.example.test",
        )
        self.assertTrue(rich["title"].startswith("Preferred.Release\n"))
        self.assertNotIn("original.mkv", rich["title"])
        self.assertEqual(fallback["name"], "TL Core")
        self.assertEqual(fallback["title"], "original.mkv\n1.50 KB")

    def test_entry_exposes_only_safe_fields_and_source_id_authority(self):
        entry = core_source_stream(
            source(item=presentation(release_name="Safe.Release")),
            "https://stream.example.test",
        )
        self.assertEqual(set(entry), {"name", "title", "url"})
        self.assertEqual(entry["url"], f"https://stream.example.test/stream/core/{SOURCE_A}")
        rendered = repr(entry).lower()
        for forbidden in (
            "replica", "chat_id", "thread_id", "message_id", "credential",
            "publisher", "installation", "provenance", "confidence",
        ):
            self.assertNotIn(forbidden, rendered)


class PresentationRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_production_helper_requests_enriched_once_without_side_effects(self):
        fake = SimpleNamespace(
            discover_sources=AsyncMock(return_value=(source(),)),
            ensure_cache=AsyncMock(),
            materialize_cache=AsyncMock(),
        )
        settings = SimpleNamespace(base_url="https://stream.example.test")
        with (
            patch.object(stremio_routes, "get_core_client", return_value=fake),
            patch.object(stremio_routes.SettingsManager, "current", return_value=settings),
        ):
            entries = await stremio_routes._core_streams_for("movie", "tt0133093")
        self.assertEqual(len(entries), 1)
        fake.discover_sources.assert_awaited_once()
        self.assertTrue(fake.discover_sources.await_args.kwargs["include_presentation"])
        fake.ensure_cache.assert_not_awaited()
        fake.materialize_cache.assert_not_awaited()

    async def test_core_order_is_preserved_despite_resolution_filename_and_size(self):
        returned = (
            source(
                SOURCE_A,
                filename="z-last.mkv",
                size=9 * 1024**3,
                item=presentation(resolution=PresentationResolution.HD_720P),
            ),
            source(
                SOURCE_B,
                filename="a-first.mkv",
                size=1 * 1024**3,
                item=presentation(resolution=PresentationResolution.UHD_2160P),
            ),
        )
        fake = SimpleNamespace(discover_sources=AsyncMock(return_value=returned))
        settings = SimpleNamespace(base_url="https://stream.example.test")
        with (
            patch.object(stremio_routes, "get_core_client", return_value=fake),
            patch.object(stremio_routes.SettingsManager, "current", return_value=settings),
        ):
            entries = await stremio_routes._core_streams_for("movie", "tt0133093")
        self.assertEqual(
            [entry["url"].rsplit("/", 1)[-1] for entry in entries],
            [str(SOURCE_A), str(SOURCE_B)],
        )

    def test_stream_contains_no_local_ranking_or_filename_presentation_parser(self):
        route_source = inspect.getsource(stremio_routes._core_streams_for).lower()
        adapter_source = inspect.getsource(core_source_stream).lower()
        for forbidden in ("sorted(", ".sort(", "score", "best_source", "recommended_source", "ptn.parse"):
            self.assertNotIn(forbidden, route_source)
            self.assertNotIn(forbidden, adapter_source)


if __name__ == "__main__":
    unittest.main()
