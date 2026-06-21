# worker.py transfer performance — analysis, patch, and test results

## Constraint

The destination URL in your offer is a single presigned R2 `PutObject` URL —
a one-shot whole-body upload, not an S3 multipart part. Since only
`worker.py` can be changed (no changes to the orchestrator/offer protocol),
the upload (PUT) side cannot be parallelized. Only the source GET can be
split across multiple connections.

## Changes made

1. **Parallel sub-range GET, single PUT.** For object-storage destinations
   and chunks at or above a size threshold (default 4 MiB), the chunk's byte
   range is split into N sub-ranges (default 4), fetched concurrently over
   separate connections into one preallocated in-memory buffer at the
   correct offsets, hashed once with SHA256 over the full buffer, then sent
   as a single PUT. Below the threshold, or for non-object-storage
   destinations, the original single-stream GET/PUT-overlap path is used
   unchanged.
2. **Removed the shared 2-thread hash executor.** `hashlib.sha256().update()`
   releases the GIL internally for large buffers, so routing every 512 KB
   slice through `run_in_executor` was pure scheduling overhead, not a
   speedup. Hashing is now done inline in both paths.
3. **Raised `FETCH_STREAM_CHUNK_SIZE`** from 512 KB to 2 MB (env-tunable via
   `WORKER_FETCH_STREAM_CHUNK_SIZE`) — fewer, larger async generator
   round-trips per byte transferred in the fallback path.
4. **HTTP/2 + larger connection pool.** The client now requests HTTP/2 (falls
   back to HTTP/1.1 automatically if the `h2` package isn't installed) and
   sizes `max_connections`/`max_keepalive_connections` for
   `MAX_CONCURRENT_TASKS × (PARALLEL_FETCH_STREAMS + 1)` instead of a flat
   multiplier of 4, since each concurrent task can now open multiple GET
   connections simultaneously.

New environment variables (all optional, sensible defaults):

| Variable | Default | Purpose |
|---|---|---|
| `WORKER_PARALLEL_FETCH_STREAMS` | `4` | Sub-ranges per chunk for the parallel GET path |
| `WORKER_PARALLEL_FETCH_MIN_CHUNK_BYTES` | `4194304` (4 MiB) | Minimum chunk size before parallel fetch kicks in |
| `WORKER_PARALLEL_FETCH_MIN_SUBRANGE_BYTES` | `1048576` (1 MiB) | Floor per sub-range; caps fan-out for smaller chunks |
| `WORKER_FETCH_STREAM_CHUNK_SIZE` | `2097152` (2 MiB) | Streaming read granularity in the fallback path |

`pip install h2` enables HTTP/2; the worker runs fine without it, just on
HTTP/1.1.

## A real bug found and fixed during testing

`execute_transfer` (the production call site) never passes `chunk_offset` or
`chunk_size` to `fetch_and_send_chunk` — it sets the source `Range` header
directly via `extra_fetch_headers` (the offer's own signed header) and
passes the absolute offset separately as `send_chunk_offset`. My first patch
gated the new parallel path on `chunk_offset is not None`, which is **never
true in production** — the parallel path would have silently never
activated. Fixed by deriving the absolute byte range from the `Range` header
inside `extra_fetch_headers` (falling back to explicit `chunk_offset`/
`chunk_size` if those are ever passed directly). This is now covered by an
end-to-end test that calls the function exactly the way `execute_transfer`
does.

## Test results

All three test scripts pass against the patched file:

```
$ python3 test_import.py
IMPORT_OK
chunk total bytes: 12582912
n=1: streams=1 ... total_match=True
n=2: streams=2 ... total_match=True
n=3: streams=3 ... total_match=True
n=4: streams=4 ... total_match=True
n=5: streams=5 ... total_match=True
n=7: streams=7 ... total_match=True
small chunk (100 bytes, 8 streams requested): sizes=[13,13,13,13,12,12,12,12] sum=100
1-byte range, 4 streams requested: [(5, 5)]
ALL_PLAN_SUBRANGES_TESTS_PASSED
```
Verifies the range-splitting math: no gaps, no overlaps, full coverage,
correct behavior at the edges (chunk smaller than stream count, 1-byte
chunk).

```
$ python3 test_e2e.py
[Worker] Parallel fetch+PUT ok chunk=0 streams=4 bytes=5000 etag='"fake-etag-12345"'
bytes_transferred=5000 response_code=200
fetch_ms=27.8 send_ms=2.7
etag='"fake-etag-12345"'
computed_hash matches expected: True
received PUT body matches expected bytes: True
received PUT body length: 5000 expected: 5000
etag correctly surfaced: True
ALL_END_TO_END_TESTS_PASSED
```
Runs `fetch_and_send_chunk` against a local mock R2-style server using the
exact calling convention `execute_transfer` uses (Range header via
`extra_fetch_headers`, no explicit `chunk_offset`/`chunk_size`). Confirms 4
parallel sub-range GETs land in the correct buffer positions, the SHA256
over the assembled buffer matches the hash of the real source slice, the PUT
body is byte-for-byte correct, and the ETag is surfaced correctly.

```
$ python3 test_fallback.py
PARALLEL_FETCH_MIN_CHUNK_BYTES=4194304
[Worker] Staging PUT ok chunk=0 bytes=100000 etag='"small-chunk-etag"'
bytes_transferred=100000 response_code=200 etag='"small-chunk-etag"'
hash matches: True
put body matches: True
FALLBACK_PATH_TEST_PASSED
```
Confirms chunks below the parallel-fetch size threshold still go through the
original single-stream GET/PUT-overlap path, with correct hash, bytes, and
ETag.

## What this does and doesn't fix

**Fixed:** the GET side of each chunk now uses multiple concurrent
connections instead of one, which is the main lever available given the
destination is a fixed single-shot presigned PUT URL. This should noticeably
reduce per-chunk wall time on a 1 Gbps link, where a single TCP/TLS stream
rarely saturates the full pipe due to slow-start and per-connection caps.

**Not fixed, and not fixable from worker.py alone:** the PUT itself is still
one sequential request per chunk, and GET/PUT no longer overlap in time for
chunks that take the parallel path (buffer-then-PUT, vs. the old
stream-while-uploading design). For your 12.58 MB chunk size this trade is
favorable — the PUT is short in absolute terms — but if chunk sizes grow
much larger, true overlap (start PUT before all GETs finish) would require
either keeping the old streaming design with multiplexed sub-streams feeding
one ordered queue, or a protocol change on the offer side to get true S3
multipart upload URLs.

## Files

- `worker.py` — patched file, drop-in replacement
- `test_import.py`, `test_e2e.py`, `test_fallback.py` — test scripts used to
  validate the patch (not required for deployment, included for your own
  re-verification)
