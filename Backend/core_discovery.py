"""Side-effect-free Stremio identity and TL-Core Source presentation adapters."""

from __future__ import annotations

import re
from typing import Any

from Backend.core_client import (
    DiscoveredSource,
    DiscoveredSourcePresentation,
    PresentationAudioCodec,
    PresentationHdrFormat,
    PresentationSourceType,
)
from Backend.core_playback import build_core_stream_url


_IMDB_ID = re.compile(r"^tt[0-9]+$")
_POSITIVE_DECIMAL = re.compile(r"^[1-9][0-9]*$")
_NONNEGATIVE_DECIMAL = re.compile(r"^(0|[1-9][0-9]*)$")


def core_requested_identity(media_type: str, stremio_id: str) -> dict[str, Any] | None:
    """Return an exact M4C request or None when the Stremio ID is not deterministic."""
    parts = stremio_id.split(":")
    if parts[0].lower() == "kitsu":
        if media_type != "series" or len(parts) not in {2, 3}:
            return None
        if not _POSITIVE_DECIMAL.fullmatch(parts[1]):
            return None
        payload: dict[str, Any] = {
            "provider": "KITSU",
            "namespace": "SERIES",
            "external_id": parts[1],
            "raw_requested_id": stremio_id,
            "media_type_hint": "ANIME",
        }
        if len(parts) == 3:
            if not _POSITIVE_DECIMAL.fullmatch(parts[2]):
                return None
            payload["episode"] = {
                "kind": "ABSOLUTE",
                "season_number": None,
                "episode_number": None,
                "absolute_number": int(parts[2]),
            }
        return payload

    if not _IMDB_ID.fullmatch(parts[0]) or int(parts[0][2:]) <= 0:
        return None
    if media_type == "movie" and len(parts) == 1:
        return {
            "provider": "IMDB",
            "namespace": "MOVIE",
            "external_id": parts[0],
            "raw_requested_id": stremio_id,
            "media_type_hint": "MOVIE",
        }
    if media_type != "series":
        return None
    if len(parts) == 1:
        return {
            "provider": "IMDB",
            "namespace": "SERIES",
            "external_id": parts[0],
            "raw_requested_id": stremio_id,
            "media_type_hint": "SERIES",
        }
    if (
        len(parts) != 3
        or not _NONNEGATIVE_DECIMAL.fullmatch(parts[1])
        or not _POSITIVE_DECIMAL.fullmatch(parts[2])
    ):
        return None
    return {
        "provider": "IMDB",
        "namespace": "SERIES",
        "external_id": parts[0],
        "raw_requested_id": stremio_id,
        "episode": {
            "kind": "SEASON_EPISODE",
            "season_number": int(parts[1]),
            "episode_number": int(parts[2]),
            "absolute_number": None,
        },
        "media_type_hint": "SERIES",
    }


def core_source_stream(source: DiscoveredSource, public_base_url: str) -> dict[str, Any]:
    size = _format_size(source.total_size_bytes)
    part_suffix = f" · {source.expected_part_count} parts" if source.expected_part_count > 1 else ""
    presentation = source.presentation
    display_tokens = _presentation_tokens(presentation) if presentation is not None else []
    release_name = (
        presentation.release_name
        if presentation is not None and presentation.release_name is not None
        else source.original_filename
    )
    resolution = presentation.resolution.value if presentation is not None and presentation.resolution else None
    title_lines = [release_name]
    if display_tokens:
        title_lines.append(" · ".join(display_tokens))
    title_lines.append(f"{size}{part_suffix}")
    return {
        "name": f"TL Core · {resolution}" if resolution else "TL Core",
        "title": "\n".join(title_lines),
        "url": build_core_stream_url(public_base_url, source.source_id),
    }


_SOURCE_TYPE_LABELS = {
    PresentationSourceType.WEB_DL: "WEB-DL",
    PresentationSourceType.WEBRIP: "WEBRip",
    PresentationSourceType.BLURAY: "BluRay",
    PresentationSourceType.BDRIP: "BDRip",
    PresentationSourceType.REMUX: "REMUX",
    PresentationSourceType.HDTV: "HDTV",
}
_HDR_LABELS = {
    PresentationHdrFormat.HDR: "HDR",
    PresentationHdrFormat.HDR10: "HDR10",
    PresentationHdrFormat.HDR10_PLUS: "HDR10+",
    PresentationHdrFormat.DOLBY_VISION: "DV",
}
_AUDIO_LABELS = {
    PresentationAudioCodec.AAC: "AAC",
    PresentationAudioCodec.AC3: "AC3",
    PresentationAudioCodec.EAC3: "EAC3",
    PresentationAudioCodec.DTS: "DTS",
    PresentationAudioCodec.DTS_HD: "DTS-HD",
    PresentationAudioCodec.DTS_HD_MA: "DTS-HD MA",
    PresentationAudioCodec.TRUEHD: "TRUEHD",
    PresentationAudioCodec.FLAC: "FLAC",
    PresentationAudioCodec.OPUS: "OPUS",
}


def _presentation_tokens(presentation: DiscoveredSourcePresentation) -> list[str]:
    tokens: list[str] = []
    if presentation.resolution is not None:
        tokens.append(presentation.resolution.value)

    is_remux = presentation.is_remux is True or presentation.source_type is PresentationSourceType.REMUX
    if is_remux:
        tokens.append("REMUX")
    elif presentation.source_type is not None:
        tokens.append(_SOURCE_TYPE_LABELS[presentation.source_type])

    if presentation.video_codec is not None:
        video = presentation.video_codec.value
        if presentation.video_profile is not None:
            video += f" {presentation.video_profile}"
        tokens.append(video)

    if presentation.hdr_format is not None:
        tokens.append(_HDR_LABELS[presentation.hdr_format])
    elif presentation.is_dolby_vision is True:
        tokens.append("DV")
    elif presentation.is_hdr is True:
        tokens.append("HDR")

    if presentation.audio_codec is not None:
        audio = _AUDIO_LABELS[presentation.audio_codec]
        channel_detail = presentation.audio_channels or presentation.audio_layout
        if channel_detail is not None:
            audio += f" {channel_detail}"
        tokens.append(audio)
    elif presentation.audio_channels is not None:
        tokens.append(presentation.audio_channels)
    elif presentation.audio_layout is not None:
        tokens.append(presentation.audio_layout)

    if presentation.is_multi_audio is True:
        tokens.append("Multi-Audio")
    elif presentation.is_dual_audio is True:
        tokens.append("Dual Audio")
    if presentation.audio_languages:
        tokens.append(f"Audio: {'/'.join(language.upper() for language in presentation.audio_languages)}")
    if presentation.subtitle_languages:
        tokens.append(f"Subs: {'/'.join(language.upper() for language in presentation.subtitle_languages)}")
    return tokens


def _format_size(size_bytes: int) -> str:
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{int(value)} {unit}" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")
