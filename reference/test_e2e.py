import asyncio
import hashlib
import sys
import threading
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --- stub bittensor (same as test_import.py) ---
bt = types.ModuleType("bittensor")


class FakeWallet:
    @staticmethod
    def add_args(parser):
        pass


class FakeSubtensor:
    @staticmethod
    def add_args(parser):
        pass


class FakeConfig(dict):
    def __init__(self, parser, args=None):
        super().__init__()
        self.subtensor = {}


bt.Wallet = FakeWallet
bt.Subtensor = FakeSubtensor
bt.Config = FakeConfig
sys.modules["bittensor"] = bt
sys.argv = ["worker.py"]

import httpx  # noqa: E402

import worker  # noqa: E402

# Tune for the test: small min sizes so a small fake object still parallelizes
worker.PARALLEL_FETCH_MIN_CHUNK_BYTES = 1024
worker.PARALLEL_FETCH_MIN_SUBRANGE_BYTES = 256
worker.PARALLEL_FETCH_STREAMS = 4
worker.MAX_RETRIES = 1  # fail fast in tests, no need for retry backoff

# Build deterministic fake object content: 10000 bytes of varying content
FAKE_OBJECT = bytes((i * 37 + 11) % 256 for i in range(10000))

received_put_body = {"data": None, "headers": None}


class FakeR2Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence default request logging

    def do_GET(self):
        range_header = self.headers.get("Range")
        if not range_header:
            body = FAKE_OBJECT
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        spec = range_header.split("=", 1)[1]
        start_s, end_s = spec.split("-")
        start, end = int(start_s), int(end_s)
        body = FAKE_OBJECT[start : end + 1]
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{len(FAKE_OBJECT)}")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        received_put_body["data"] = body
        received_put_body["headers"] = dict(self.headers)
        self.send_response(200)
        self.send_header("ETag", '"fake-etag-12345"')
        self.send_header("Content-Length", "0")
        self.end_headers()


def run_server(server):
    server.serve_forever()


async def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeR2Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=run_server, args=(server,), daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    source_url = f"{base_url}/source?X-Amz-Signature=fake"
    dest_url = f"{base_url}/dest?X-Amz-Signature=fake"

    chunk_offset = 2000
    chunk_size = 5000  # subset of the 10000-byte fake object
    range_end = chunk_offset + chunk_size - 1

    async with httpx.AsyncClient() as client:
        result = await worker.fetch_and_send_chunk(
            client,
            source_url,
            dest_url,
            "test-transfer",
            0,
            total_size=chunk_size,
            expected_max_bytes=chunk_size,
            task_id="test-task",
            offer_id="test-offer",
            extra_fetch_headers={"Range": f"bytes={chunk_offset}-{range_end}"},
            send_chunk_offset=chunk_offset,
            is_canary=False,
        )

    bytes_transferred, chunk_hash, etag, response_code, fetch_ms, send_ms = result
    print(f"bytes_transferred={bytes_transferred} response_code={response_code}")
    print(f"fetch_ms={fetch_ms:.1f} send_ms={send_ms:.1f}")
    print(f"etag={etag!r}")

    expected_slice = FAKE_OBJECT[chunk_offset : chunk_offset + chunk_size]
    expected_hash = hashlib.sha256(expected_slice).hexdigest()

    print(f"computed_hash matches expected: {chunk_hash == expected_hash}")
    print(f"received PUT body matches expected bytes: {received_put_body['data'] == expected_slice}")
    print(f"received PUT body length: {len(received_put_body['data'])} expected: {len(expected_slice)}")
    print(f"etag correctly surfaced: {etag == '\"fake-etag-12345\"'}")

    assert bytes_transferred == chunk_size, "bytes_transferred mismatch"
    assert response_code == 200, "unexpected response code"
    assert chunk_hash == expected_hash, "hash mismatch — buffer assembly is wrong"
    assert received_put_body["data"] == expected_slice, "PUT body bytes don't match source slice"
    assert etag == '"fake-etag-12345"', "etag not surfaced correctly"

    print("ALL_END_TO_END_TESTS_PASSED")
    server.shutdown()


asyncio.run(main())
