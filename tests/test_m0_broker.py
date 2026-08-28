import unittest
from types import SimpleNamespace

from spikes.m0_telegram_copy_stream.models import SourceManifest, SourcePart
from spikes.m0_telegram_copy_stream.telegram_broker import CopyPartialFailure, TelegramBroker


def source_message(topic_id=11):
    return SimpleNamespace(empty=False, message_thread_id=topic_id, document=SimpleNamespace(file_size=4), video=None)


class FakeCopyClient:
    def __init__(self, fail_on_source_id=None):
        self.fail_on_source_id = fail_on_source_id
        self.calls = []

    async def get_messages(self, chat_id, message_id):
        return source_message()

    async def copy_message(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["message_id"] == self.fail_on_source_id:
            raise ValueError("deliberate non-transient failure")
        return SimpleNamespace(id=kwargs["message_id"] + 400, message_thread_id=kwargs["message_thread_id"])


class TelegramBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def test_copy_uses_copy_message_and_explicit_destination_topic(self):
        client = FakeCopyClient()
        broker = TelegramBroker(client, attempts=2, max_retry_delay_seconds=0)
        source = SourceManifest("movie.mkv", (SourcePart(0, -1001, 101),))
        result = await broker.copy_manifest(
            source,
            source_topic_id=11,
            destination_chat_id=-2002,
            destination_topic_id=22,
        )
        self.assertTrue(result.complete)
        self.assertEqual(result.parts[0].destination_message_id, 501)
        self.assertEqual(client.calls[0]["message_thread_id"], 22)
        self.assertEqual(client.calls[0]["from_chat_id"], -1001)

    async def test_failure_reports_successful_prefix_and_failed_index(self):
        client = FakeCopyClient(fail_on_source_id=102)
        broker = TelegramBroker(client, attempts=3, max_retry_delay_seconds=0)
        source = SourceManifest("movie.mkv", (
            SourcePart(0, -1001, 101),
            SourcePart(1, -1001, 102),
            SourcePart(2, -1001, 103),
        ))
        with self.assertRaises(CopyPartialFailure) as caught:
            await broker.copy_manifest(
                source,
                source_topic_id=11,
                destination_chat_id=-2002,
                destination_topic_id=22,
            )
        self.assertEqual(caught.exception.report.successful_parts, (0,))
        self.assertEqual(caught.exception.report.failed_part, 1)
        self.assertFalse(caught.exception.manifest.complete)
        self.assertEqual(len(client.calls), 2)


if __name__ == "__main__":
    unittest.main()
