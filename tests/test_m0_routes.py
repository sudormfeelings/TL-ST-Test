import unittest
from types import SimpleNamespace

from fastapi.testclient import TestClient

from spikes.m0_telegram_copy_stream.models import PlaybackManifest, PlaybackPart
from spikes.m0_telegram_copy_stream.routes import StreamState, create_app


class FakeRouteStreamer:
    def __init__(self):
        self.files = {
            (-200, 50): SimpleNamespace(
                data=b"ABCD", file_size=4, file_name="movie.mkv.001", mime_type="application/octet-stream"
            ),
            (-200, 99): SimpleNamespace(
                data=b"EFGHI", file_size=5, file_name="movie.mkv.002", mime_type="application/octet-stream"
            ),
        }

    async def get_file_properties(self, chat_id, message_id):
        return self.files[(chat_id, message_id)]

    async def prefetch_stream(
        self,
        *,
        file_id,
        offset,
        first_part_cut,
        last_part_cut,
        part_count,
        chunk_size,
        **kwargs,
    ):
        async def generate():
            for sequence in range(part_count):
                chunk = file_id.data[offset + sequence * chunk_size:offset + (sequence + 1) * chunk_size]
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif sequence == 0:
                    yield chunk[first_part_cut:]
                elif sequence == part_count - 1:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk
        return generate()


class M0RouteTests(unittest.TestCase):
    def setUp(self):
        playback = PlaybackManifest("movie.mkv", (
            PlaybackPart(index=0, chat_id=-200, message_id=50, size=4),
            PlaybackPart(index=1, chat_id=-200, message_id=99, size=5),
        ))
        self.streamer = FakeRouteStreamer()
        self.client = TestClient(create_app(StreamState(object(), self.streamer, playback), port=8780))

    def test_normal_get_and_head(self):
        response = self.client.get("/m0/stream")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"ABCDEFGHI")
        self.assertEqual(response.headers["content-length"], "9")
        self.assertEqual(response.headers["accept-ranges"], "bytes")

        head = self.client.head("/m0/stream")
        self.assertEqual(head.status_code, 200)
        self.assertEqual(head.content, b"")
        self.assertEqual(head.headers["content-length"], "9")

    def test_cross_part_range_response_is_exact(self):
        response = self.client.get("/m0/stream", headers={"Range": "bytes=2-6"})
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, b"CDEFG")
        self.assertEqual(response.headers["content-range"], "bytes 2-6/9")
        self.assertEqual(response.headers["content-length"], "5")

    def test_invalid_range_and_missing_destination_part_fail_cleanly(self):
        invalid = self.client.get("/m0/stream", headers={"Range": "bytes=99-"})
        self.assertEqual(invalid.status_code, 416)
        self.assertEqual(invalid.headers["content-range"], "bytes */9")

        del self.streamer.files[(-200, 99)]
        missing = self.client.get("/m0/stream")
        self.assertEqual(missing.status_code, 424)
        self.assertIn("Destination playback part is unavailable", missing.json()["detail"])


if __name__ == "__main__":
    unittest.main()
