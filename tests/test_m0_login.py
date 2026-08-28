import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from spikes.m0_telegram_copy_stream.viewer_login import (
    _start_with_secret_prompts_hidden,
    login_viewer_session,
    update_session_value,
)


class SessionValueUpdateTests(unittest.TestCase):
    def test_updates_empty_session_line(self):
        updated = update_session_value("M0_VIEWER_SESSION=\n", "new-session")
        self.assertEqual(updated, "M0_VIEWER_SESSION=new-session\n")

    def test_replaces_existing_session_value(self):
        updated = update_session_value("M0_VIEWER_SESSION=old-session\n", "new-session")
        self.assertEqual(updated, "M0_VIEWER_SESSION=new-session\n")

    def test_does_not_duplicate_session_key(self):
        original = "M0_VIEWER_SESSION=old-one\nM0_VIEWER_SESSION=old-two\n"
        updated = update_session_value(original, "new-session")
        self.assertEqual(updated.count("M0_VIEWER_SESSION="), 1)
        self.assertIn("M0_VIEWER_SESSION=new-session", updated)

    def test_preserves_unrelated_values_and_comments(self):
        original = (
            "# viewer credentials\n"
            "M0_VIEWER_API_ID=12345\n"
            "M0_VIEWER_API_HASH=private-hash\n"
            "M0_VIEWER_SESSION=  # managed locally\n"
            "\n"
            "# keep this comment\n"
            "M0_PORT=8780\n"
        )
        updated = update_session_value(original, "new-session")
        expected = original.replace(
            "M0_VIEWER_SESSION=  # managed locally",
            "M0_VIEWER_SESSION=new-session  # managed locally",
        )
        self.assertEqual(updated, expected)


class FakeLoginClient:
    def __init__(self, session: str, user_id: int = 123456789):
        self.session = session
        self.user = SimpleNamespace(id=user_id, username="m0viewer")
        self.stopped = False

    async def start(self):
        return self

    async def get_me(self):
        return self.user

    async def export_session_string(self):
        return self.session

    async def stop(self):
        self.stopped = True


class FakeVerificationClient:
    def __init__(self, *, user_id: int = 123456789, failure: Exception | None = None):
        self.user = SimpleNamespace(id=user_id, username="m0viewer")
        self.failure = failure
        self.disconnected = False

    async def connect(self):
        if self.failure is not None:
            raise self.failure
        return True

    async def get_me(self):
        return self.user

    async def disconnect(self):
        self.disconnected = True


class ViewerLoginTests(unittest.IsolatedAsyncioTestCase):
    def _write_env(self, directory: str, session: str = "") -> Path:
        path = Path(directory) / ".env"
        path.write_text(
            "# M0 local credentials\n"
            "M0_VIEWER_API_ID=12345\n"
            "M0_VIEWER_API_HASH=private-api-hash\n"
            f"M0_VIEWER_SESSION={session}\n"
            "M0_PORT=8780\n",
            encoding="utf-8",
        )
        return path

    async def test_declined_replacement_preserves_existing_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_env(directory, "existing-secret-session")
            original = path.read_bytes()
            factory_called = False
            output: list[str] = []

            def factory(**_kwargs):
                nonlocal factory_called
                factory_called = True
                raise AssertionError("Telegram client must not be constructed")

            result = await login_viewer_session(
                path,
                client_factory=factory,
                input_fn=lambda _prompt: "",
                output_fn=output.append,
            )

            self.assertEqual(result, 0)
            self.assertFalse(factory_called)
            self.assertEqual(path.read_bytes(), original)
            self.assertNotIn("existing-secret-session", "\n".join(output))

    async def test_login_code_and_password_prompts_are_hidden(self):
        import pyrogram.client as pyrogram_client

        prompt_calls: list[tuple[str, bool]] = []

        async def recording_ainput(prompt: str = "", *, hide: bool = False):
            prompt_calls.append((prompt, hide))
            return "local-entry"

        class PromptingClient:
            async def start(self):
                await pyrogram_client.ainput("Enter phone number: ")
                await pyrogram_client.ainput("Enter confirmation code: ")
                await pyrogram_client.ainput("Enter password: ", hide=True)

        original_ainput = pyrogram_client.ainput
        pyrogram_client.ainput = recording_ainput
        try:
            await _start_with_secret_prompts_hidden(PromptingClient())
        finally:
            pyrogram_client.ainput = original_ainput

        self.assertEqual(
            prompt_calls,
            [
                ("Enter phone number: ", False),
                ("Enter confirmation code: ", True),
                ("Enter password: ", True),
            ],
        )

    async def test_verification_failure_restores_previous_env_without_printing_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_env(directory)
            original = path.read_bytes()
            exported_session = "new-secret-session"
            output: list[str] = []

            def factory(*, purpose: str, **_kwargs):
                if purpose == "login":
                    return FakeLoginClient(exported_session)
                return FakeVerificationClient(failure=RuntimeError("secret-bearing internal failure"))

            result = await login_viewer_session(path, client_factory=factory, output_fn=output.append)

            rendered_output = "\n".join(output)
            self.assertEqual(result, 1)
            self.assertEqual(path.read_bytes(), original)
            self.assertNotIn(exported_session, rendered_output)
            self.assertNotIn("private-api-hash", rendered_output)
            self.assertNotIn("secret-bearing internal failure", rendered_output)
            self.assertNotIn("Telegram login successful", rendered_output)

    async def test_success_saves_verified_session_without_printing_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self._write_env(directory)
            exported_session = "new-secret-session"
            output: list[str] = []
            verification_clients: list[FakeVerificationClient] = []

            def factory(*, purpose: str, session_string: str | None, **_kwargs):
                if purpose == "login":
                    self.assertIsNone(session_string)
                    return FakeLoginClient(exported_session)
                self.assertEqual(session_string, exported_session)
                client = FakeVerificationClient()
                verification_clients.append(client)
                return client

            result = await login_viewer_session(path, client_factory=factory, output_fn=output.append)

            rendered_output = "\n".join(output)
            self.assertEqual(result, 0)
            self.assertIn("M0_VIEWER_SESSION=new-secret-session", path.read_text(encoding="utf-8"))
            self.assertEqual(path.read_text(encoding="utf-8").count("M0_VIEWER_SESSION="), 1)
            self.assertTrue(verification_clients[0].disconnected)
            self.assertIn("Telegram login successful", rendered_output)
            self.assertIn("User: @m0viewer", rendered_output)
            self.assertIn("User ID: 123456789", rendered_output)
            self.assertNotIn(exported_session, rendered_output)
            self.assertNotIn("private-api-hash", rendered_output)


if __name__ == "__main__":
    unittest.main()
