from __future__ import annotations

import logging
import mimetypes
import secrets
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse

from Backend.helper.custom_dl import ByteStreamer
from Backend.helper.http_range import build_stream_headers
from Backend.helper.http_range import parse_range_header
from Backend.helper.virtual_dl import resolve_virtual_parts, virtual_stream_generator

from .models import PlaybackManifest


LOGGER = logging.getLogger("m0.stream")
M0_VIEWER_CLIENT_INDEX = -1000
M0_STREMIO_TEST_ID = "tt1254207"


@dataclass
class StreamState:
    viewer_client: Any
    streamer: ByteStreamer
    playback: PlaybackManifest


async def _resolve_destination_parts(state: StreamState):
    payload = [
        {"chat_id": part.chat_id, "msg_id": part.message_id}
        for part in state.playback.parts
    ]
    try:
        parts, virtual_size = await resolve_virtual_parts(payload, state.streamer, prefix_100=False)
    except Exception as exc:
        raise HTTPException(
            status_code=424,
            detail=f"Destination playback part is unavailable: {type(exc).__name__}",
        ) from exc
    expected_sizes = [part.size for part in state.playback.parts]
    actual_sizes = [part["size"] for part in parts]
    if actual_sizes != expected_sizes or virtual_size != state.playback.virtual_size:
        raise HTTPException(status_code=409, detail="Destination parts changed after verification")
    return parts, virtual_size


def create_app(state: StreamState, *, port: int) -> FastAPI:
    app = FastAPI(title="M0 Telegram Copy/Stream Spike", docs_url=None, redoc_url=None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "HEAD"],
        allow_headers=["*"],
    )

    @app.api_route("/m0/stream", methods=["GET", "HEAD"])
    async def m0_stream(request: Request):
        parts, virtual_size = await _resolve_destination_parts(state)
        range_header = request.headers.get("Range", "")
        start, end = parse_range_header(range_header, virtual_size)
        requested_length = end - start + 1
        mime_type = mimetypes.guess_type(state.playback.logical_name)[0] or "application/octet-stream"
        headers, status = build_stream_headers(
            mime_type,
            state.playback.logical_name,
            requested_length,
            range_header,
            start,
            end,
            virtual_size,
        )
        LOGGER.info("[M0 STREAM] range=%s virtual_size=%s", range_header or "full", virtual_size)
        if request.method == "HEAD":
            return Response(status_code=status, headers=headers)

        body = virtual_stream_generator(
            parts=parts,
            start=start,
            end=end,
            chunk_size=1024 * 1024,
            streamer=state.streamer,
            client_index=M0_VIEWER_CLIENT_INDEX,
            request=request,
            meta={"title": state.playback.logical_name, "m0_spike": True},
            stream_id=secrets.token_hex(8),
            parallelism=1,
            prefetch_count=1,
        )
        return StreamingResponse(body, headers=headers, status_code=status, media_type=mime_type)

    @app.get("/m0/status")
    async def status():
        return {
            "ready": True,
            "logical_name": state.playback.logical_name,
            "parts": len(state.playback.parts),
            "part_sizes": [part.size for part in state.playback.parts],
            "virtual_size": state.playback.virtual_size,
            "source_messages_used_for_playback": False,
        }

    @app.get("/m0/stremio/manifest.json")
    async def stremio_manifest():
        return JSONResponse({
            "id": "org.telegram-stremio.m0-spike",
            "version": "0.0.1",
            "name": "M0 Telegram Copy Spike",
            "description": "Disposable developer-only destination streaming proof",
            "resources": [{
                "name": "stream",
                "types": ["movie"],
                "idPrefixes": [M0_STREMIO_TEST_ID],
            }],
            "types": ["movie"],
            "idPrefixes": [M0_STREMIO_TEST_ID],
            "catalogs": [],
        })

    @app.get("/m0/stremio/stream/{media_type}/{media_id}.json")
    async def stremio_stream(media_type: str, media_id: str):
        if media_type != "movie" or media_id != M0_STREMIO_TEST_ID:
            return {"streams": []}
        return {
            "streams": [{
                "name": "M0 Destination Cache",
                "description": f"Developer-only destination playback: {state.playback.logical_name}",
                "url": f"http://127.0.0.1:{port}/m0/stream",
                "behaviorHints": {
                    "notWebReady": True,
                    "filename": state.playback.logical_name,
                },
            }]
        }

    return app
