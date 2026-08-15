import json
import tempfile
import unittest
from pathlib import Path

from spikes.m0_telegram_copy_stream.manifest_store import (
    load_destination_manifest,
    save_destination_manifest,
)
from spikes.m0_telegram_copy_stream.models import (
    CopyFailureReport,
    DestinationManifest,
    DestinationPart,
    ManifestValidationError,
    PlaybackManifest,
    PlaybackPart,
    SourceManifest,
    cross_part_slices,
)


class ManifestModelTests(unittest.TestCase):
    def test_source_parts_are_sorted_strictly_by_explicit_index(self):
        manifest = SourceManifest.from_dict({
            "logical_name": "movie.mkv",
            "parts": [
                {"index": 1, "source_chat_id": -1001, "source_message_id": 12},
                {"index": 0, "source_chat_id": -1001, "source_message_id": 11},
            ],
        })
        self.assertEqual([part.index for part in manifest.parts], [0, 1])
        self.assertEqual([part.source_message_id for part in manifest.parts], [11, 12])

    def test_duplicate_or_noncontiguous_indexes_are_rejected(self):
        with self.assertRaises(ManifestValidationError):
            SourceManifest.from_dict({
                "logical_name": "movie.mkv",
                "parts": [
                    {"index": 0, "source_chat_id": -1001, "source_message_id": 11},
                    {"index": 0, "source_chat_id": -1001, "source_message_id": 12},
                ],
            })

    def test_virtual_size_and_cross_part_offsets(self):
        playback = PlaybackManifest("movie.mkv", (
            PlaybackPart(index=0, chat_id=-200, message_id=50, size=10),
            PlaybackPart(index=1, chat_id=-200, message_id=99, size=7),
            PlaybackPart(index=2, chat_id=-200, message_id=60, size=3),
        ))
        self.assertEqual(playback.virtual_size, 20)
        self.assertEqual(cross_part_slices(playback.parts, 8, 12), [(0, 8, 9), (1, 0, 2)])

    def test_partial_copy_representation_round_trips(self):
        report = CopyFailureReport((0,), 1, "FloodWait")
        manifest = DestinationManifest(
            "movie.mkv",
            destination_topic_id=77,
            parts=(DestinationPart(0, -200, 501),),
            complete=False,
            failure=report,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "destination.json"
            save_destination_manifest(path, manifest)
            loaded = load_destination_manifest(path)
        self.assertFalse(loaded.complete)
        self.assertEqual(loaded.failure.to_dict()["status"], "COPY_PARTIAL_FAILURE")
        self.assertEqual(loaded.failure.successful_parts, (0,))

    def test_complete_manifest_cannot_be_empty(self):
        with self.assertRaises(ManifestValidationError):
            DestinationManifest("movie.mkv", destination_topic_id=77, parts=())


if __name__ == "__main__":
    unittest.main()
