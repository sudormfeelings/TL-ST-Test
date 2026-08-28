from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .models import SourceManifest, SourcePart


PACKAGE_DIR = Path(__file__).resolve().parent


class ConfigError(ValueError):
    pass


def _required(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _required_int(name: str) -> int:
    try:
        return int(_required(name))
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


def _optional_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class M0Config:
    bot_token: str
    source_chat_id: int
    source_topic_id: int
    destination_chat_id: int
    destination_topic_id: int
    api_id: int
    api_hash: str
    viewer_session: str
    source_message_ids: tuple[int, ...]
    logical_name: str
    source_manifest_path: Path | None
    destination_manifest_path: Path
    copy_attempts: int
    retry_max_delay_seconds: float
    host: str
    port: int

    @classmethod
    def from_env(cls, env_file: str | Path | None = None, *, require_copy: bool = True) -> "M0Config":
        load_dotenv(Path(env_file) if env_file else PACKAGE_DIR / ".env", override=False)
        source_manifest_raw = (os.getenv("M0_SOURCE_MANIFEST") or "").strip()
        ids_raw = (os.getenv("M0_SOURCE_MESSAGE_IDS") or "").strip()
        try:
            source_ids = tuple(int(item.strip()) for item in ids_raw.split(",") if item.strip())
        except ValueError as exc:
            raise ConfigError("M0_SOURCE_MESSAGE_IDS must be a comma-separated integer list") from exc
        if require_copy and not source_manifest_raw and not source_ids:
            raise ConfigError("Set M0_SOURCE_MANIFEST or M0_SOURCE_MESSAGE_IDS")

        attempts = _optional_int("M0_COPY_ATTEMPTS", 3)
        if not 1 <= attempts <= 10:
            raise ConfigError("M0_COPY_ATTEMPTS must be between 1 and 10")
        try:
            retry_delay = float((os.getenv("M0_RETRY_MAX_DELAY_SECONDS") or "10").strip())
        except ValueError as exc:
            raise ConfigError("M0_RETRY_MAX_DELAY_SECONDS must be numeric") from exc
        if retry_delay < 0 or retry_delay > 60:
            raise ConfigError("M0_RETRY_MAX_DELAY_SECONDS must be between 0 and 60")
        host = (os.getenv("M0_HOST") or "127.0.0.1").strip()
        if host != "127.0.0.1":
            raise ConfigError("Milestone 0 must bind to 127.0.0.1")

        return cls(
            bot_token=_required("M0_BOT_TOKEN") if require_copy else (os.getenv("M0_BOT_TOKEN") or "").strip(),
            source_chat_id=_required_int("M0_SOURCE_CHAT_ID") if require_copy else _optional_int("M0_SOURCE_CHAT_ID", 0),
            source_topic_id=_required_int("M0_SOURCE_TOPIC_ID") if require_copy else _optional_int("M0_SOURCE_TOPIC_ID", 0),
            destination_chat_id=_required_int("M0_DESTINATION_CHAT_ID") if require_copy else _optional_int("M0_DESTINATION_CHAT_ID", 0),
            destination_topic_id=_required_int("M0_DESTINATION_CACHE_TOPIC_ID") if require_copy else _optional_int("M0_DESTINATION_CACHE_TOPIC_ID", 0),
            api_id=_required_int("M0_VIEWER_API_ID"),
            api_hash=_required("M0_VIEWER_API_HASH"),
            viewer_session=_required("M0_VIEWER_SESSION"),
            source_message_ids=source_ids,
            logical_name=(os.getenv("M0_LOGICAL_NAME") or "M0.Test.mkv").strip(),
            source_manifest_path=Path(source_manifest_raw) if source_manifest_raw else None,
            destination_manifest_path=Path(
                (os.getenv("M0_DESTINATION_MANIFEST") or str(PACKAGE_DIR / "destination_manifest.json")).strip()
            ),
            copy_attempts=attempts,
            retry_max_delay_seconds=retry_delay,
            host=host,
            port=_optional_int("M0_PORT", 8780),
        )

    def source_manifest_from_ids(self) -> SourceManifest:
        return SourceManifest(
            logical_name=self.logical_name,
            parts=tuple(
                SourcePart(index=index, source_chat_id=self.source_chat_id, source_message_id=message_id)
                for index, message_id in enumerate(self.source_message_ids)
            ),
        )
