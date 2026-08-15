# Milestone 0: Telegram Copy-to-Local-Stream Spike

This disposable spike tests one architecture boundary only:

```text
Uploader private Forum topic
  -> central bot copy_message(message_thread_id=Cache topic)
  -> viewer private Forum Cache topic
  -> viewer Telegram user session
  -> localhost seekable HTTP stream
```

The central bot copies messages but never downloads or proxies video bytes. Playback resolves only the copied destination message IDs. No MongoDB, production API, metadata provider, catalog, authentication, UI, or completed-file assembly is involved.

## Setup

1. Create or choose two private Telegram supergroups with Forum Topics enabled.
2. In the source group, create the `📤 Upload / Reupload` topic and place the test document parts there in logical order.
3. In the destination group, create the `💾 Cache / LSX` topic.
4. Create a central bot with BotFather. Add it as an administrator to both groups with permission to read and send/copy messages.
5. Add the viewer Telegram user account to the destination group. Source-group membership is needed only if you want that user to inspect the source manually; the streamer does not use it.
6. Obtain an API ID and API hash for the viewer account from `my.telegram.org`.
7. Copy `config.example.env` to `spikes/m0_telegram_copy_stream/.env` and fill in `M0_VIEWER_API_ID` and `M0_VIEWER_API_HASH`. Leave `M0_VIEWER_SESSION` empty.
8. From the repository root, authorize the viewer account locally:

   ```powershell
   .\.venv\Scripts\python.exe -m spikes.m0_telegram_copy_stream.run login
   ```

   Enter the viewer phone number, Telegram login code, and 2FA password (if requested) only in the local terminal. The command exports the authorized session directly into `M0_VIEWER_SESSION` in the spike `.env`, then verifies it with a fresh in-memory client. It never prints the session string.

The numeric topic ID is the topic's `message_thread_id`, not the supergroup ID. The bot-copy command validates both the source topic and the destination Cache topic.

## Environment variables

Required for `copy`:

| Variable | Purpose |
| --- | --- |
| `M0_BOT_TOKEN` | Central bot token. Used only for copy orchestration. |
| `M0_SOURCE_CHAT_ID` | Source private Forum supergroup ID, normally `-100...`. |
| `M0_SOURCE_TOPIC_ID` | Source Upload topic `message_thread_id`. |
| `M0_SOURCE_MESSAGE_IDS` | Comma-separated logical part order, unless `M0_SOURCE_MANIFEST` is used. |
| `M0_DESTINATION_CHAT_ID` | Viewer private Forum supergroup ID. |
| `M0_DESTINATION_CACHE_TOPIC_ID` | Destination Cache topic `message_thread_id`. |
| `M0_VIEWER_API_ID` | Telegram application API ID. Also used to initialize the bot client. |
| `M0_VIEWER_API_HASH` | Telegram application API hash. |
| `M0_VIEWER_SESSION` | Viewer user-session string written automatically by `login`. Do not fill or share it manually. |

Required for `verify` and `serve`:

- `M0_VIEWER_API_ID`
- `M0_VIEWER_API_HASH`
- `M0_VIEWER_SESSION`
- `M0_DESTINATION_MANIFEST` only when overriding its default path

The viewer commands intentionally do not require the bot token or any source-group setting.

Optional values:

| Variable | Default | Purpose |
| --- | --- | --- |
| `M0_LOGICAL_NAME` | `M0.Test.mkv` | Virtual output filename when message IDs come from the environment. |
| `M0_SOURCE_MANIFEST` | unset | Explicit source JSON fixture instead of `M0_SOURCE_MESSAGE_IDS`. |
| `M0_DESTINATION_MANIFEST` | `spikes/m0_telegram_copy_stream/destination_manifest.json` | Local copy result. |
| `M0_COPY_ATTEMPTS` | `3` | Bounded attempts per Telegram lookup/copy operation. |
| `M0_RETRY_MAX_DELAY_SECONDS` | `10` | Maximum delay between transient retries. |
| `M0_HOST` | `127.0.0.1` | Loopback bind address; non-loopback values are rejected. |
| `M0_PORT` | `8780` | Local server port. |

Credentials are never logged. Never share the spike `.env`, session string, API hash, login code, or 2FA password. The spike `.gitignore` excludes `.env`, local session files, and the generated destination manifest.

## Initialize the viewer session

With `M0_VIEWER_API_ID` and `M0_VIEWER_API_HASH` filled in, run:

```powershell
.\.venv\Scripts\python.exe -m spikes.m0_telegram_copy_stream.run login
```

PyroFork prompts locally for the viewer phone number, Telegram login code, and 2FA password when required. Login codes and passwords are entered without terminal echo. On success, the command writes `M0_VIEWER_SESSION` atomically, starts a fresh in-memory client from the stored value, calls `get_me()`, and disconnects. An existing session is preserved unless you explicitly answer `y` to the replacement prompt.

## Prepare test source

For the simplest fixture, set message IDs in their explicit logical order:

```env
M0_LOGICAL_NAME=Movie.Test.mkv
M0_SOURCE_MESSAGE_IDS=101,102,103
```

This becomes indexes `0,1,2`; destination message IDs are never used for ordering.

Alternatively, copy `source_manifest.example.json` to a local file, replace its sample values, and set:

```env
M0_SOURCE_MANIFEST=C:/private/m0_source_manifest.json
```

The source messages must be documents or videos in `M0_SOURCE_TOPIC_ID`. A single ID exercises the single-part case.

## Run copy

From the repository root, with the project dependencies installed:

```bash
python -m spikes.m0_telegram_copy_stream.run copy
```

Or use a separate environment file:

```bash
python -m spikes.m0_telegram_copy_stream.run --env-file C:/private/m0.env copy
```

The command performs these steps in order:

1. The central bot resolves each source message and checks its source topic/media.
2. It calls PyroFork `copy_message` with the configured destination `message_thread_id`.
3. It validates the returned destination topic and records the destination message ID under the original explicit index.
4. It atomically writes the destination JSON manifest.
5. It stops the bot and independently starts the viewer user session.
6. The viewer resolves every destination message, validates topic/media/size, and builds the in-memory playback manifest.

On part failure the command exits nonzero, saves an incomplete manifest, and prints `COPY_PARTIAL_FAILURE`, the successful indexes, failed index, and error type. Already-copied messages remain in the Cache topic; automatic rollback is intentionally deferred.

## Run viewer streamer

After a successful copy:

```bash
python -m spikes.m0_telegram_copy_stream.run verify
python -m spikes.m0_telegram_copy_stream.run serve
```

The server binds to `127.0.0.1:8780` by default. It verifies all destination parts before listening. On every request it resolves those destination messages again; a missing or changed part fails the request instead of silently producing corrupted ordering.

Inspect the destination-only state and part sizes:

```bash
curl http://127.0.0.1:8780/m0/status
```

## Verify with curl

Normal headers/HEAD:

```bash
curl -I http://127.0.0.1:8780/m0/stream
```

Normal GET without retaining the body:

```bash
curl -v http://127.0.0.1:8780/m0/stream -o /dev/null
```

One MiB Range:

```bash
curl -v \
  -H "Range: bytes=0-1048575" \
  http://127.0.0.1:8780/m0/stream \
  -o /dev/null
```

Expect `206 Partial Content`, the exact `Content-Range`, `Content-Length: 1048576` when the virtual file is at least that large, and `Accept-Ranges: bytes`.

Invalid/unsatisfiable Range:

```bash
curl -v -H "Range: bytes=999999999999-" http://127.0.0.1:8780/m0/stream -o /dev/null
```

Expect `416 Requested Range Not Satisfiable` and `Content-Range: bytes */<virtual_size>`.

## Cross-part verification

Read `part_sizes` from `/m0/status`. If part 0 has size `S`, request a range that takes 512 bytes from each side of the boundary:

```text
start = S - 512
end   = S + 511
Range: bytes=<start>-<end>
```

Example when part 0 is 2,000,000 bytes:

```bash
curl -v \
  -H "Range: bytes=1999488-2000511" \
  http://127.0.0.1:8780/m0/stream \
  -o boundary.bin
```

Expect status `206`, `Content-Length: 1024`, and exactly 1024 bytes on disk. For byte-level proof, fetch the last 512 bytes of destination part 0 and first 512 bytes of destination part 1 with a trusted Telegram test tool, concatenate those small samples, and compare their hash to `boundary.bin`. The automated generator test performs this exact logical-concatenation assertion with in-memory fixture bytes.

## Player verification

Start the server, then use either player:

```bash
mpv http://127.0.0.1:8780/m0/stream
```

```bash
vlc http://127.0.0.1:8780/m0/stream
```

Seek to roughly 25%, 50%, and 80%. Each seek should produce a new Range request and resume without downloading all preceding bytes.

The temporary Stremio proof endpoints are:

```text
http://127.0.0.1:8780/m0/stremio/manifest.json
http://127.0.0.1:8780/m0/stremio/stream/movie/m0:test.json
```

They expose one hardcoded stream response and no catalog or metadata provider. If the installed Stremio build supports opening a direct network URL, open `http://127.0.0.1:8780/m0/stream`; otherwise inspect/install the developer manifest and invoke the hardcoded `m0:test` stream through the local addon-development tooling. This endpoint is only a transport proof, not a usable addon.

## Manual acceptance checklist

### Single-part copy

Set one source message ID, run `copy`, and confirm in Telegram that the destination message exists under the configured Cache topic. The command must finish with `COPY_COMPLETE_AND_VIEWER_VERIFIED`.

### Multipart copy

Set at least three ordered IDs, run `copy`, and inspect `destination_manifest.json`. Confirm all parts exist with indexes `0,1,2`, regardless of their resulting destination IDs.

### Missing destination part

Delete one copied message from the Cache topic and restart `serve`. Startup must fail with a destination verification error. If it is deleted after startup, the next stream request must fail with a useful `424` response.

### Source independence

1. Complete copy and stop the viewer server.
2. Remove the viewer user's access to the source group, or otherwise make the source group unreachable to the viewer.
3. For an even stronger configuration check, remove `M0_BOT_TOKEN`, `M0_SOURCE_CHAT_ID`, `M0_SOURCE_TOPIC_ID`, and `M0_SOURCE_MESSAGE_IDS` from the environment. Keep the saved destination manifest and viewer credentials.
4. Run `verify`, then `serve`.
5. Play and seek the destination stream.

Pass only if playback still succeeds. The status endpoint reports `source_messages_used_for_playback: false`, but the access-removal test is the authoritative integration proof.

## Automated tests

The tests cover manifest validation, explicit ordering, virtual size, range parsing, cross-part offset calculation, exact cross-part byte concatenation, the zero-offset chunk boundary, destination-topic copy arguments, and partial-copy failure representation.

```bash
python -m unittest discover -s tests -p "test_m0_*.py" -v
```

They deliberately use only small fakes and do not attempt to emulate Telegram. Real copy, destination access, player seeking, and source independence remain manual integration tests.

## Known limitations

- Successful parts are not rolled back after a later copy failure.
- Destination manifests are local disposable JSON, with no locking beyond atomic replacement.
- Copying is sequential and intentionally unoptimized.
- Only one HTTP byte range is supported per request; multipart HTTP Range responses are not implemented.
- Telegram integration requires real private Forums, bot permissions, and a viewer session and cannot be certified by unit tests.
- The Stremio response is a hardcoded developer proof without discovery metadata or catalogs.
- The process is localhost-only and unauthenticated by design.
- This spike does not implement any Milestone 1 or production lifecycle behavior.
