import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from Backend.config import Telegram
from Backend.helper.database import Database


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class DatabaseBoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_reused_streaming_modules_import_without_database_configuration(self):
        environment = os.environ.copy()
        environment["DATABASE"] = ""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import Backend.helper.custom_dl; import Backend.helper.virtual_dl",
            ],
            cwd=REPOSITORY_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    async def test_connect_still_rejects_missing_required_databases(self):
        with patch.object(Telegram, "DATABASE", []):
            database = Database()
        with self.assertRaisesRegex(ValueError, "At least 2 database URIs are required"):
            await database.connect()


if __name__ == "__main__":
    unittest.main()
