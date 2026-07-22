/**
 * Cloudflare Worker: Beam chunk transfer (fetch then PUT).
 *
 * Timing matches fetch_and_send_chunk:
 *   1) Download full source body → fetch_ms
 *   2) PUT buffer to dest         → send_ms
 *
 * GET  /  → { name, version, mode, updated_at }  (health / version probe)
 * POST /  → transfer task JSON → { success, etag, timings_ms, ... }
 *
 * Bump WORKER_VERSION on every meaningful change, then redeploy all names.
 *
 * Deploy one:
 *   npx wrangler deploy scripts/cloudflare/transfer-worker.js --name still-base-8f94
 * Deploy many:
 *   ./scripts/cloudflare/deploy-workers.sh still-base-8f94 noisy-union-160b ancient-cloud-c0e4
 */
const WORKER_NAME = "beam-cf-transfer";
const WORKER_VERSION = "1.1.0";
const WORKER_MODE = "fetch_then_put";
/** ISO date of this script revision (update when bumping WORKER_VERSION). */
const WORKER_UPDATED_AT = "2026-07-22";

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
      try {
        partNumber = new URL(task.dest_url).searchParams.get("partNumber");
      } catch (_) {}

      const expectedBytes = Number(task.chunk_size) || 0;

      const fetchStarted = performance.now();
      const srcResponse = await fetch(task.source_url, {
        method: "GET",
        headers: task.source_headers || {},
      });
      if (!srcResponse.ok) {
        const err = await srcResponse.text();
        return new Response(
          JSON.stringify({
            success: false,
            error: `Source Fetch Failed: ${srcResponse.status} ${err}`.slice(0, 500),
            task_id: taskId,
            part_number: partNumber,
            worker_version: WORKER_VERSION,
            timings_ms: {
              fetch_ms: Number((performance.now() - fetchStarted).toFixed(2)),
              send_ms: 0,
            },
          }),
          { status: srcResponse.status, headers: { "Content-Type": "application/json" } }
        );
      }

      const body = await srcResponse.arrayBuffer();
      const fetchMs = Number((performance.now() - fetchStarted).toFixed(2));
      const bytes = body.byteLength;
      if (expectedBytes > 0 && bytes !== expectedBytes) {
        return new Response(
          JSON.stringify({
            success: false,
            error: `fetch_size_mismatch: expected ${expectedBytes} got ${bytes}`,
            task_id: taskId,
            part_number: partNumber,
            worker_version: WORKER_VERSION,
            timings_ms: { fetch_ms: fetchMs, send_ms: 0 },
          }),
          { status: 502, headers: { "Content-Type": "application/json" } }
        );
      }

      const sendStarted = performance.now();
      const destResponse = await fetch(task.dest_url, {
        method: "PUT",
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length": String(bytes),
          ...(task.dest_headers || {}),
        },
        body,
      });
      const sendMs = Number((performance.now() - sendStarted).toFixed(2));

      if (!destResponse.ok) {
        const errorBody = await destResponse.text();
        return new Response(
          JSON.stringify({
            success: false,
            error: "Destination rejected payload",
            status: destResponse.status,
            r2_error_body: errorBody,
            task_id: taskId,
            part_number: partNumber,
            worker_version: WORKER_VERSION,
            timings_ms: {
              source_fetch: fetchMs,
              fetch_ms: fetchMs,
              stream_upload: sendMs,
              send_ms: sendMs,
              total_execution: Number((performance.now() - t0).toFixed(2)),
            },
          }),
          {
            status: destResponse.status,
            headers: { "Content-Type": "application/json" },
          }
        );
      }

      const rawEtag =
        destResponse.headers.get("ETag") || destResponse.headers.get("etag");
      const cleanEtag = rawEtag ? rawEtag.replace(/"/g, "") : null;
      const totalMs = Number((performance.now() - t0).toFixed(2));

      console.log(
        `[Worker] ok v=${WORKER_VERSION} task=${taskId} part=${partNumber} bytes=${bytes} ` +
          `fetch_ms=${fetchMs} send_ms=${sendMs} wall_ms=${totalMs} etag=${cleanEtag}`
      );

      return new Response(
        JSON.stringify({
          success: true,
          task_id: taskId,
          etag: cleanEtag,
          part_number: partNumber,
          bytes_processed: String(bytes),
          worker_version: WORKER_VERSION,
          timings_ms: {
            source_fetch: fetchMs,
            fetch_ms: fetchMs,
            stream_upload: sendMs,
            send_ms: sendMs,
            total_execution: totalMs,
          },
        }),
        {
          headers: {
            "Content-Type": "application/json",
            "X-Beam-Worker-Version": WORKER_VERSION,
          },
        }
      );
    } catch (error) {
      const totalMs = Number((performance.now() - t0).toFixed(2));
      console.error(
        `[Worker] fail v=${WORKER_VERSION} task=${taskId} error=${error.message} wall_ms=${totalMs}`
      );
      return new Response(
        JSON.stringify({
          success: false,
          error: error.message,
          task_id: taskId,
          part_number: partNumber,
          worker_version: WORKER_VERSION,
          total_execution_ms: totalMs,
        }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  },
};
