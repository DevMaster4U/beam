/**
 * Cloudflare Worker: Beam chunk transfer via fetch_and_send_chunk.
 *
 * Mirrors neurons/worker fetch_and_send_chunk object-storage path:
 *   - GET source (Range via source_headers)
 *   - Concurrently stream body into dest PUT (producer/consumer overlap)
 *   - fetch_ms  = until source stream fully consumed
 *   - send_ms   = from PUT start until dest response
 *
 * GET  /  → { name, version, mode, updated_at }
 * POST /  → task/offer JSON → { success, etag, timings_ms, ... }
 *
 * Bump WORKER_VERSION on every meaningful change, then redeploy all names.
 *
 * Deploy one:
 *   npx wrangler deploy scripts/cloudflare/transfer-worker.js --name still-base-8f94
 * Deploy many:
 *   ./scripts/cloudflare/deploy-workers.sh --file scripts/cloudflare/workers.txt
 */
const WORKER_NAME = "beam-cf-transfer";
const WORKER_VERSION = "1.2.0";
const WORKER_MODE = "fetch_and_send_chunk";
/** ISO date of this script revision (update when bumping WORKER_VERSION). */
const WORKER_UPDATED_AT = "2026-07-23";

function versionResponse() {
  return new Response(
    JSON.stringify({
      ok: true,
      name: WORKER_NAME,
      version: WORKER_VERSION,
      mode: WORKER_MODE,
      updated_at: WORKER_UPDATED_AT,
    }),
    {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "X-Beam-Worker-Version": WORKER_VERSION,
      },
    }
  );
}

function jsonResponse(payload, status = 200, extraHeaders = {}) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      "X-Beam-Worker-Version": WORKER_VERSION,
      ...extraHeaders,
    },
  });
}

function partNumberFromDest(destUrl) {
  try {
    return new URL(destUrl).searchParams.get("partNumber");
  } catch (_) {
    return null;
  }
}

function rangeFromHeaders(headers) {
  if (!headers || typeof headers !== "object") return "-";
  return String(headers.Range || headers.range || "-");
}

/**
 * fetch_and_send_chunk — same shape as the Python worker helper.
 *
 * Streams source GET into destination PUT concurrently (queue/backpressure
 * via TransformStream), then returns etag + fetch_ms/send_ms.
 */
async function fetchAndSendChunk(task) {
  const taskId = String(task.task_id || "");
  const offerId = String(task.offer_id || "");
  const sourceUrl = String(task.source_url || "");
  const destUrl = String(task.dest_url || "");
  const sourceHeaders = task.source_headers || {};
  const destHeaders = task.dest_headers || {};
  const expectedBytes = Number(task.chunk_size) || 0;
  const partNumber = partNumberFromDest(destUrl);
  const rangeLabel = rangeFromHeaders(sourceHeaders);

  if (!sourceUrl || !destUrl) {
    throw new Error("missing source_url or dest_url");
  }

  console.log(
    `[Worker] task_chunk stage=start task=${taskId} offer=${offerId} ` +
      `part=${partNumber} range=${rangeLabel} bytes_expected=${expectedBytes || "?"}`
  );

  const fetchStarted = performance.now();
  const srcResponse = await fetch(sourceUrl, {
    method: "GET",
    headers: sourceHeaders,
  });

  if (!srcResponse.ok) {
    const err = await srcResponse.text();
    const fetchMs = Number((performance.now() - fetchStarted).toFixed(2));
    const error = `Source Fetch Failed: ${srcResponse.status} ${err}`.slice(0, 500);
    console.log(
      `[Worker] task_chunk stage=failed task=${taskId} offer=${offerId} reason=${error}`
    );
    return {
      ok: false,
      status: srcResponse.status,
      body: {
        success: false,
        error,
        task_id: taskId,
        offer_id: offerId,
        part_number: partNumber,
        worker_version: WORKER_VERSION,
        timings_ms: { fetch_ms: fetchMs, send_ms: 0 },
      },
    };
  }

  if (!srcResponse.body) {
    throw new Error("source response missing body");
  }

  const contentLengthHeader = srcResponse.headers.get("Content-Length");
  if (expectedBytes > 0 && contentLengthHeader) {
    const responseSize = Number(contentLengthHeader);
    if (Number.isFinite(responseSize) && responseSize > expectedBytes) {
      throw new Error(
        `response too large: ${responseSize} bytes > expected ${expectedBytes}`
      );
    }
  }

  let bytesTransferred = 0;
  const { readable, writable } = new TransformStream({
    transform(chunk, controller) {
      const size = chunk.byteLength ?? chunk.length ?? 0;
      bytesTransferred += size;
      if (expectedBytes > 0 && bytesTransferred > expectedBytes) {
        throw new Error(
          `response exceeded expected size while streaming: ` +
            `${bytesTransferred} bytes > expected ${expectedBytes}`
        );
      }
      controller.enqueue(chunk);
    },
  });

  const putLength =
    expectedBytes > 0
      ? expectedBytes
      : contentLengthHeader && Number.isFinite(Number(contentLengthHeader))
        ? Number(contentLengthHeader)
        : null;

  const sendHeaders = {
    "Content-Type": "application/octet-stream",
    ...destHeaders,
  };
  if (putLength != null && putLength > 0) {
    sendHeaders["Content-Length"] = String(putLength);
  }

  const abort = new AbortController();
  const sendStarted = performance.now();
  const pipePromise = srcResponse.body.pipeTo(writable, { signal: abort.signal });
  const destPromise = fetch(destUrl, {
    method: "PUT",
    headers: sendHeaders,
    body: readable,
    signal: abort.signal,
  });

  try {
    await pipePromise;
  } catch (err) {
    abort.abort();
    throw err;
  }

  const fetchMs = Number((performance.now() - fetchStarted).toFixed(2));
  console.log(
    `[Worker] task_chunk stage=fetch_done task=${taskId} offer=${offerId} ` +
      `part=${partNumber} bytes=${bytesTransferred} fetch_ms=${fetchMs}`
  );

  if (expectedBytes > 0 && bytesTransferred !== expectedBytes) {
    abort.abort();
    return {
      ok: false,
      status: 502,
      body: {
        success: false,
        error: `fetch_size_mismatch: expected ${expectedBytes} got ${bytesTransferred}`,
        task_id: taskId,
        offer_id: offerId,
        part_number: partNumber,
        worker_version: WORKER_VERSION,
        timings_ms: { fetch_ms: fetchMs, send_ms: 0 },
      },
    };
  }

  let destResponse;
  try {
    destResponse = await destPromise;
  } catch (err) {
    abort.abort();
    throw err;
  }
  const sendMs = Number((performance.now() - sendStarted).toFixed(2));

  if (!destResponse.ok) {
    const errorBody = await destResponse.text();
    console.log(
      `[Worker] task_chunk stage=failed task=${taskId} offer=${offerId} ` +
        `reason=Destination rejected payload status=${destResponse.status}`
    );
    return {
      ok: false,
      status: destResponse.status,
      body: {
        success: false,
        error: "Destination rejected payload",
        status: destResponse.status,
        r2_error_body: errorBody,
        task_id: taskId,
        offer_id: offerId,
        part_number: partNumber,
        worker_version: WORKER_VERSION,
        timings_ms: {
          source_fetch: fetchMs,
          fetch_ms: fetchMs,
          stream_upload: sendMs,
          send_ms: sendMs,
        },
      },
    };
  }

  const rawEtag =
    destResponse.headers.get("ETag") || destResponse.headers.get("etag");
  const cleanEtag = rawEtag ? rawEtag.replace(/"/g, "") : null;

  console.log(
    `[Worker] task_chunk stage=put_done task=${taskId} offer=${offerId} ` +
      `part=${partNumber} bytes=${bytesTransferred} etag=${cleanEtag} ` +
      `fetch_ms=${fetchMs} send_ms=${sendMs}`
  );

  return {
    ok: true,
    status: 200,
    body: {
      success: true,
      task_id: taskId,
      offer_id: offerId,
      etag: cleanEtag,
      part_number: partNumber,
      bytes_processed: String(bytesTransferred),
      worker_version: WORKER_VERSION,
      timings_ms: {
        source_fetch: fetchMs,
        fetch_ms: fetchMs,
        stream_upload: sendMs,
        send_ms: sendMs,
      },
    },
  };
}

export default {
  async fetch(request) {
    if (request.method === "GET" || request.method === "HEAD") {
      if (request.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
            "X-Beam-Worker-Version": WORKER_VERSION,
          },
        });
      }
      return versionResponse();
    }

    const t0 = performance.now();
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    let taskId = "";
    let partNumber = null;
    try {
      const task = await request.json();
      taskId = String(task.task_id || "");
      partNumber = partNumberFromDest(task.dest_url);

      const result = await fetchAndSendChunk(task);
      const totalMs = Number((performance.now() - t0).toFixed(2));
      if (result.body.timings_ms) {
        result.body.timings_ms.total_execution = totalMs;
      }

      if (result.ok) {
        console.log(
          `[Worker] ok v=${WORKER_VERSION} task=${taskId} part=${partNumber} ` +
            `bytes=${result.body.bytes_processed} ` +
            `fetch_ms=${result.body.timings_ms.fetch_ms} ` +
            `send_ms=${result.body.timings_ms.send_ms} wall_ms=${totalMs} ` +
            `etag=${result.body.etag}`
        );
      }

      return jsonResponse(result.body, result.status);
    } catch (error) {
      const totalMs = Number((performance.now() - t0).toFixed(2));
      console.error(
        `[Worker] fail v=${WORKER_VERSION} task=${taskId} error=${error.message} wall_ms=${totalMs}`
      );
      return jsonResponse(
        {
          success: false,
          error: error.message,
          task_id: taskId,
          part_number: partNumber,
          worker_version: WORKER_VERSION,
          total_execution_ms: totalMs,
        },
        500
      );
    }
  },
};
