from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .models import DestinationManifest, SourceManifest


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Manifest does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Manifest is not valid JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Manifest root must be an object: {path}")
    return value


def load_source_manifest(path: str | Path) -> SourceManifest:
    return SourceManifest.from_dict(_read_json(Path(path)))


def load_destination_manifest(path: str | Path) -> DestinationManifest:
    return DestinationManifest.from_dict(_read_json(Path(path)))


def save_destination_manifest(path: str | Path, manifest: DestinationManifest) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(manifest.to_dict(), indent=2, sort_keys=False) + "\n"
    fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, target)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
