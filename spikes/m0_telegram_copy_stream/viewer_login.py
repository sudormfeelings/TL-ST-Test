from __future__ import annotations

import os
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dotenv import dotenv_values


SESSION_KEY = "M0_VIEWER_SESSION"
_SESSION_LINE = re.compile(
    r"^(?P<prefix>\s*(?:export\s+)?M0_VIEWER_SESSION\s*=)"
    r"(?P<value>[^#\r\n]*?)(?P<comment>\s+#.*)?$"
)


class ViewerLoginError(RuntimeError):
    """A non-secret validation or login failure suitable for CLI handling."""


def _load_login_credentials(env_path: Path) -> tuple[int, str]:
    if not env_path.is_file():
        raise ViewerLoginError(f"M0 environment file not found: {env_path}")

    values = dotenv_values(env_path, interpolate=False)
    api_id_raw = (values.get("M0_VIEWER_API_ID") or "").strip()
    api_hash = (values.get("M0_VIEWER_API_HASH") or "").strip()
    if not api_id_raw:
        raise ViewerLoginError("M0_VIEWER_API_ID is required in the M0 .env file")
    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise ViewerLoginError("M0_VIEWER_API_ID must be an integer") from exc
    if api_id <= 0:
        raise ViewerLoginError("M0_VIEWER_API_ID must be a positive integer")
    if not api_hash:
        raise ViewerLoginError("M0_VIEWER_API_HASH is required in the M0 .env file")
    return api_id, api_hash


def _split_line(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n") or line.endswith("\r"):
        return line[:-1], line[-1]
    return line, ""


def update_session_value(env_text: str, session_string: str) -> str:
    """Replace the session assignment while preserving unrelated .env content."""

    if not session_string or "\n" in session_string or "\r" in session_string:
        raise ViewerLoginError("Telegram returned an invalid viewer session")

    output: list[str] = []
    replaced = False
    for line in env_text.splitlines(keepends=True):
        body, ending = _split_line(line)
        match = _SESSION_LINE.match(body)
        if match is None:
            output.append(line)
            continue
        if replaced:
            continue
        output.append(f"{match.group('prefix')}{session_string}{match.group('comment') or ''}{ending}")
        replaced = True

    if not replaced:
        if env_text and not env_text.endswith(("\n", "\r")):
            output.append(os.linesep)
        output.append(f"{SESSION_KEY}={session_string}{os.linesep}")
    return "".join(output)


def _has_existing_session(env_text: str) -> bool:
    for line in env_text.splitlines():
        match = _SESSION_LINE.match(line)
        if match is not None and match.group("value").strip():
            return True
    return False


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(content)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _default_client_factory(
    *,
    api_id: int,
    api_hash: str,
    session_string: str | None,
    purpose: str,
) -> Any:
    from pyrogram import Client

    options: dict[str, Any] = {
        "api_id": api_id,
        "api_hash": api_hash,
        "in_memory": True,
        "no_updates": True,
        "hide_password": True,
    }
    if session_string is not None:
        options["session_string"] = session_string
    return Client(f"m0_viewer_{purpose}", **options)


async def _start_with_secret_prompts_hidden(client: Any) -> None:
    """Use PyroFork's interactive flow while hiding codes as well as passwords."""

    import pyrogram.client as pyrogram_client

    original_ainput = pyrogram_client.ainput

    async def secure_ainput(prompt: str = "", *, hide: bool = False) -> str:
        normalized = prompt.casefold()
        is_secret = hide or "confirmation code" in normalized or "recovery code" in normalized
        return await original_ainput(prompt, hide=is_secret)

    pyrogram_client.ainput = secure_ainput
    try:
        await client.start()
    finally:
        pyrogram_client.ainput = original_ainput


def _safe_user_label(user: Any) -> str:
    username = getattr(user, "username", None)
    return f"@{username}" if username else "(no username)"


async def login_viewer_session(
    env_path: Path,
    *,
    client_factory: Callable[..., Any] = _default_client_factory,
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], None] = print,
) -> int:
    """Authorize, persist, and freshly verify the local M0 viewer session."""

    env_path = env_path.resolve()
    try:
        api_id, api_hash = _load_login_credentials(env_path)
        original_bytes = env_path.read_bytes()
        original_text = original_bytes.decode("utf-8")
    except (OSError, UnicodeError, ViewerLoginError) as exc:
        output_fn(f"Viewer login failed: {exc}")
        return 1

    if _has_existing_session(original_text):
        output_fn("A viewer session already exists.")
        try:
            replace = input_fn("Replace it? [y/N] ").strip().casefold() == "y"
        except (EOFError, KeyboardInterrupt):
            output_fn("Viewer session unchanged.")
            return 0
        if not replace:
            output_fn("Viewer session unchanged.")
            return 0

    try:
        login_client = client_factory(
            api_id=api_id,
            api_hash=api_hash,
            session_string=None,
            purpose="login",
        )
        await _start_with_secret_prompts_hidden(login_client)
        try:
            user = await login_client.get_me()
            session_string = await login_client.export_session_string()
            updated_text = update_session_value(original_text, session_string)
        finally:
            await login_client.stop()
    except (Exception, KeyboardInterrupt) as exc:
        output_fn(f"Viewer login failed ({type(exc).__name__}).")
        return 1

    try:
        _atomic_write(env_path, updated_text.encode("utf-8"))
        stored_session = (dotenv_values(env_path, interpolate=False).get(SESSION_KEY) or "").strip()
        if stored_session != session_string:
            raise ViewerLoginError("the saved viewer session could not be read back")
        verification_client = client_factory(
            api_id=api_id,
            api_hash=api_hash,
            session_string=stored_session,
            purpose="verification",
        )
        verification_open = False
        try:
            verification_authorized = bool(await verification_client.connect())
            verification_open = True
            if not verification_authorized:
                raise ViewerLoginError("the exported session was not authorized")
            verified_user = await verification_client.get_me()
            if int(verified_user.id) != int(user.id):
                raise ViewerLoginError("the verified Telegram user did not match")
        finally:
            if verification_open:
                await verification_client.disconnect()
    except (Exception, KeyboardInterrupt) as exc:
        try:
            _atomic_write(env_path, original_bytes)
        except OSError:
            output_fn("Viewer session verification failed and the previous .env could not be restored.")
            return 1
        output_fn(f"Viewer session verification failed ({type(exc).__name__}); the previous .env was restored.")
        return 1

    output_fn("Telegram login successful")
    output_fn(f"User: {_safe_user_label(user)}")
    output_fn(f"User ID: {int(user.id)}")
    output_fn("Viewer session saved and verified.")
    return 0
