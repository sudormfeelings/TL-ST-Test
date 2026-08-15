import asyncio
import unittest
from types import SimpleNamespace

from fastapi import HTTPException

from Backend.helper.custom_dl import ByteStreamer
from Backend.helper.http_range import build_stream_headers, parse_range_header
from Backend.helper.virtual_dl import virtual_stream_generator
from Backend.pyrofork.bot import client_avg_mbps, client_failures, work_loads


class FakeByteStreamer:
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


class RangeTests(unittest.TestCase):
    def test_range_forms_and_invalid_range(self):
        self.assertEqual(parse_range_header("", 100), (0, 99))
        self.assertEqual(parse_range_header("bytes=10-19", 100), (10, 19))
        self.assertEqual(parse_range_header("bytes=90-", 100), (90, 99))
        self.assertEqual(parse_range_header("bytes=-10", 100), (90, 99))
        with self.assertRaises(HTTPException) as caught:
            parse_range_header("bytes=1000-", 100)
        self.assertEqual(caught.exception.status_code, 416)
        self.assertEqual(caught.exception.headers["Content-Range"], "bytes */100")

    def test_shared_headers_preserve_full_response_behavior(self):
        headers, status = build_stream_headers(
            "video/x-matroska", "movie.mkv", 100, "", 0, 99, 100
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Length"], "100")
        self.assertEqual(headers["Accept-Ranges"], "bytes")
        self.assertNotIn("Content-Range", headers)
        self.assertEqual(
            headers["Content-Disposition"],
            "inline; filename=\"movie.mkv\"; filename*=UTF-8''movie.mkv",
        )

    def test_shared_headers_preserve_partial_response_behavior(self):
        headers, status = build_stream_headers(
            "video/x-matroska", "movie.mkv", 10, "bytes=10-19", 10, 19, 100
        )
        self.assertEqual(status, 206)
        self.assertEqual(headers["Content-Range"], "bytes 10-19/100")
        self.assertEqual(headers["Content-Length"], "10")


class InMemorySession:
    def __init__(self, data: bytes):
        self.data = data
        self.requested_offsets = []

    async def send(self, request):
        self.requested_offsets.append(request.offset)
        return SimpleNamespace(bytes=self.data[request.offset:request.offset + request.limit])


class InMemoryByteStreamer(ByteStreamer):
    def __init__(self, data: bytes, client_index: int):
        # Avoid ByteStreamer's Telegram session-prewarm tasks; the inherited
        # prefetch_stream implementation below is the code under test.
        self.client = SimpleNamespace()
        self.client_index = client_index
        self.log_stats = False
        self._file_id_cache = {}
        self._session_lock = asyncio.Lock()
        self.session = InMemorySession(data)

    async def _get_media_session(self, file_id):
        return self.session

    @staticmethod
    async def _get_location(file_id):
        return object()


class VirtualGeneratorTests(unittest.IsolatedAsyncioTestCase):
    CLIENT_INDEX = -1001

    def setUp(self):
        work_loads[self.CLIENT_INDEX] = 0
        client_failures[self.CLIENT_INDEX] = 0
        client_avg_mbps[self.CLIENT_INDEX] = 0.0

    def tearDown(self):
        work_loads.pop(self.CLIENT_INDEX, None)
        client_failures.pop(self.CLIENT_INDEX, None)
        client_avg_mbps.pop(self.CLIENT_INDEX, None)

    async def collect(self, parts, start, end, chunk_size=3, streamer=None):
        streamer = streamer or FakeByteStreamer()
        generator = virtual_stream_generator(
            parts=parts,
            start=start,
            end=end,
            chunk_size=chunk_size,
            streamer=streamer,
            client_index=self.CLIENT_INDEX,
            request=None,
            meta=None,
            stream_id="test",
            parallelism=1,
            prefetch_count=1,
        )
        return b"".join([chunk async for chunk in generator])

    async def test_one_byte_range_at_zero_schedules_one_chunk(self):
        parts = [{
            "index": 0, "chat_id": -200, "msg_id": 50,
            "file_id": SimpleNamespace(data=b"ABCD"), "size": 4, "cum_start": 0,
        }]
        self.assertEqual(await self.collect(parts, 0, 0), b"A")

    async def test_cross_part_range_is_exact_logical_concatenation(self):
        parts = [
            {"index": 0, "chat_id": -200, "msg_id": 50, "file_id": SimpleNamespace(data=b"ABCD"), "size": 4, "cum_start": 0},
            {"index": 1, "chat_id": -200, "msg_id": 99, "file_id": SimpleNamespace(data=b"EFGHI"), "size": 5, "cum_start": 4},
        ]
        self.assertEqual(await self.collect(parts, 2, 6), b"CDEFG")

    async def test_fixed_chunk_boundaries_return_exact_bytes(self):
        chunk_size = 10
        data = bytes(range(50))
        cases = [
            ("zero byte", 0, 0, [0]),
            ("exact first chunk", 0, chunk_size - 1, [0]),
            ("first boundary inclusive", 0, chunk_size, [0, 10]),
            ("one byte around first boundary", chunk_size - 1, chunk_size, [0, 10]),
            ("exact second chunk", chunk_size, (2 * chunk_size) - 1, [10]),
            ("second boundary inclusive", chunk_size, 2 * chunk_size, [10, 20]),
            ("unaligned inside one chunk", 12, 17, [10]),
            ("unaligned across one boundary", 17, 23, [10, 20]),
            ("across multiple boundaries", 7, 34, [0, 10, 20, 30]),
        ]
        for label, start, end, expected_offsets in cases:
            with self.subTest(label=label, start=start, end=end):
                streamer = InMemoryByteStreamer(data, self.CLIENT_INDEX)
                file_id = SimpleNamespace(
                    data=data,
                    file_size=len(data),
                    dc_id=1,
                    local_id=None,
                    chat_id=-200,
                )
                parts = [{
                    "index": 0,
                    "chat_id": -200,
                    "msg_id": 50,
                    "file_id": file_id,
                    "size": len(data),
                    "cum_start": 0,
                }]
                returned = await self.collect(
                    parts,
                    start,
                    end,
                    chunk_size=chunk_size,
                    streamer=streamer,
                )
                self.assertEqual(returned, data[start:end + 1])
                self.assertEqual(streamer.session.requested_offsets, expected_offsets)


if __name__ == "__main__":
    unittest.main()
