from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

from .config import M0Config, PACKAGE_DIR
from .manifest_store import load_destination_manifest, load_source_manifest, save_destination_manifest
from .routes import M0_VIEWER_CLIENT_INDEX, StreamState, create_app
from .telegram_broker import CopyPartialFailure, TelegramBroker, verify_destination_manifest
from .viewer_login import login_viewer_session


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("pyrogram").setLevel(logging.WARNING)
LOGGER = logging.getLogger("m0")


def _build_bot(config: M0Config):
    from pyrogram import Client

    return Client(
        "m0_central_bot",
        api_id=config.api_id,
        api_hash=config.api_hash,
        bot_token=config.bot_token,
        in_memory=True,
        no_updates=True,
    )


def _build_viewer(config: M0Config):
    from pyrogram import Client

    return Client(
        "m0_viewer",
        api_id=config.api_id,
        api_hash=config.api_hash,
        session_string=config.viewer_session,
        in_memory=True,
        no_updates=True,
        max_concurrent_transmissions=10,
    )


def _load_source(config: M0Config):
    if config.source_manifest_path is not None:
        return load_source_manifest(config.source_manifest_path)
    return config.source_manifest_from_ids()


async def copy_command(config: M0Config) -> int:
    source = _load_source(config)
    bot = _build_bot(config)
    await bot.start()
    try:
        broker = TelegramBroker(
            bot,
            attempts=config.copy_attempts,
            max_retry_delay_seconds=config.retry_max_delay_seconds,
        )
        try:
            destination = await broker.copy_manifest(
                source,
                source_topic_id=config.source_topic_id,
                destination_chat_id=config.destination_chat_id,
                destination_topic_id=config.destination_topic_id,
            )
        except CopyPartialFailure as exc:
            save_destination_manifest(config.destination_manifest_path, exc.manifest)
            print(json.dumps(exc.report.to_dict(), indent=2))
            print(f"Partial destination manifest: {config.destination_manifest_path}")
            return 2
        save_destination_manifest(config.destination_manifest_path, destination)
    finally:
        await bot.stop()

    viewer = _build_viewer(config)
    await viewer.start()
    try:
        playback = await verify_destination_manifest(viewer, destination)
    finally:
        await viewer.stop()
    print(
        json.dumps(
            {
                "status": "COPY_COMPLETE_AND_VIEWER_VERIFIED",
                "parts": len(playback.parts),
                "virtual_size": playback.virtual_size,
                "destination_manifest": str(config.destination_manifest_path),
            },
            indent=2,
        )
    )
    return 0


async def verify_command(config: M0Config) -> int:
    destination = load_destination_manifest(config.destination_manifest_path)
    viewer = _build_viewer(config)
    await viewer.start()
    try:
        playback = await verify_destination_manifest(viewer, destination)
    finally:
        await viewer.stop()
    print(json.dumps({"status": "VIEWER_VERIFIED", "parts": len(playback.parts), "virtual_size": playback.virtual_size}, indent=2))
    return 0


async def serve_command(config: M0Config) -> int:
    import uvicorn

    from Backend.pyrofork.bot import client_avg_mbps, client_dc_map, client_failures, work_loads
    from Backend.helper.custom_dl import ByteStreamer

    destination = load_destination_manifest(config.destination_manifest_path)
    viewer = _build_viewer(config)
    await viewer.start()
    work_loads[M0_VIEWER_CLIENT_INDEX] = 0
    client_failures[M0_VIEWER_CLIENT_INDEX] = 0
    client_avg_mbps[M0_VIEWER_CLIENT_INDEX] = 0.0
    try:
        client_dc_map[M0_VIEWER_CLIENT_INDEX] = await viewer.storage.dc_id()
        playback = await verify_destination_manifest(viewer, destination)
        streamer = ByteStreamer(viewer, M0_VIEWER_CLIENT_INDEX, log_stats=False)
        app = create_app(StreamState(viewer, streamer, playback), port=config.port)
        LOGGER.info("M0 viewer serving destination-only playback on http://%s:%s/m0/stream", config.host, config.port)
        server = uvicorn.Server(uvicorn.Config(app, host=config.host, port=config.port, log_level="info"))
        await server.serve()
    finally:
        await viewer.stop()
        work_loads.pop(M0_VIEWER_CLIENT_INDEX, None)
        client_failures.pop(M0_VIEWER_CLIENT_INDEX, None)
        client_avg_mbps.pop(M0_VIEWER_CLIENT_INDEX, None)
        client_dc_map.pop(M0_VIEWER_CLIENT_INDEX, None)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Milestone 0 Telegram copy/stream feasibility spike")
    parser.add_argument("--env-file", type=Path, help="Optional environment file (defaults to the spike .env)")
    parser.add_argument("command", choices=("login", "copy", "verify", "serve"))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "login":
        return asyncio.run(login_viewer_session(args.env_file or PACKAGE_DIR / ".env"))
    config = M0Config.from_env(args.env_file, require_copy=args.command == "copy")
    commands = {"copy": copy_command, "verify": verify_command, "serve": serve_command}
    return asyncio.run(commands[args.command](config))


if __name__ == "__main__":
    raise SystemExit(main())
