"""Side-effect-free Stremio identity and TL-Core Source presentation adapters."""

from __future__ import annotations

import re
from typing import Any

from Backend.core_client import DiscoveredSource
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
    return {
        "name": "TL Core",
        "title": f"{source.original_filename}\n{size}{part_suffix}",
        "url": build_core_stream_url(public_base_url, source.source_id),
    }


def _format_size(size_bytes: int) -> str:
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{int(value)} {unit}" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")
