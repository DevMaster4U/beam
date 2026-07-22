/**
 * Cloudflare Worker (JavaScript ES module): Beam chunk transfer.
 *
 * Matches neurons/worker fetch_and_send_chunk timing model:
 *   1) Download full source body  → fetch_ms
 *   2) PUT buffer to dest         → send_ms
 *
 * Deploy:
 *   cd scripts/cloudflare && wrangler deploy transfer-worker.js
 *
 * Request:  POST application/json  BeamCore task/offer body
 * Response: { success, task_id, etag, part_number, timings_ms: { source_fetch, stream_upload, ... } }
 *
 * Note: ~40 MiB chunks fit Worker memory (128 MiB). Do not raise chunk size past ~80 MiB
 * without switching back to streaming.
 */
export default {
  async fetch(request, env) {
    const workerStartTime = performance.now();

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    let taskInfo = "Unknown Task";
    try {
      const parseStart = performance.now();
      const task = await request.json();
      const parseDuration = Number((performance.now() - parseStart).toFixed(2));

      let partNumber = null;
      try {
        partNumber = new URL(task.dest_url).searchParams.get("partNumber");
      } catch (_) {
        partNumber = null;
      }
      taskInfo = `Task: ${task.task_id} | Part: ${partNumber}`;
      const expectedBytes = Number(task.chunk_size) || 0;
      console.log(
        `[Worker] [START] ${taskInfo} | Size: ${expectedBytes} bytes | JSON Parse Time: ${parseDuration}ms`
      );

      // --- fetch_and_send_chunk step 1: download full body (fetch_ms) ---
      const fetchStarted = performance.now();
      const srcResponse = await fetch(task.source_url, {
        method: "GET",
        headers: task.source_headers || {},
      });

      if (!srcResponse.ok) {
        const errorText = await srcResponse.text();
        const fetchMs = Number((performance.now() - fetchStarted).toFixed(2));
        console.error(
          `[Worker] [FETCH_FAIL] ${taskInfo} | fetch_ms=${fetchMs} | Status: ${srcResponse.status} | Error: ${errorText}`
        );
        return new Response(`Source Fetch Failed: ${srcResponse.status}`, {
          status: srcResponse.status,
        });
      }

      const body = await srcResponse.arrayBuffer();
      const fetchMs = Number((performance.now() - fetchStarted).toFixed(2));
      const bytes = body.byteLength;
      const contentLength = String(bytes);

      if (expectedBytes > 0 && bytes !== expectedBytes) {
        console.error(
          `[Worker] [FETCH_SIZE_MISMATCH] ${taskInfo} | expected=${expectedBytes} got=${bytes} fetch_ms=${fetchMs}`
        );
        return new Response(
          JSON.stringify({
            success: false,
            error: `fetch_size_mismatch: expected ${expectedBytes} got ${bytes}`,
            task_id: task.task_id,
            part_number: partNumber,
            timings_ms: {
              json_parse: parseDuration,
              source_fetch: fetchMs,
              fetch_ms: fetchMs,
              stream_upload: 0,
              send_ms: 0,
              total_execution: Number(
                (performance.now() - workerStartTime).toFixed(2)
              ),
            },
          }),
          { status: 502, headers: { "Content-Type": "application/json" } }
        );
      }

      const fetchSec = Math.max(fetchMs / 1000, 0.001);
      const fetchMbps = ((bytes * 8) / fetchSec / 1e6).toFixed(1);
      console.log(
        `[Worker] [FETCH_DONE] ${taskInfo} | fetch_ms=${fetchMs} bytes=${bytes} ~${fetchMbps} Mbps`
      );

      // --- fetch_and_send_chunk step 2: upload buffer (send_ms) ---
      const sendStarted = performance.now();
      const destHeaders = {
        "Content-Type": "application/octet-stream",
        "Content-Length": contentLength,
        ...(task.dest_headers || {}),
      };

      const destResponse = await fetch(task.dest_url, {
        method: "PUT",
        headers: destHeaders,
        body,
      });
      const sendMs = Number((performance.now() - sendStarted).toFixed(2));

      if (!destResponse.ok) {
        const errorBody = await destResponse.text();
        const totalFailed = Number(
          (performance.now() - workerStartTime).toFixed(2)
        );
        console.error(
          `[Worker] [UPLOAD_FAIL] ${taskInfo} | send_ms=${sendMs} | Status: ${destResponse.status} | R2 Error: ${errorBody}`
        );
        console.error(
          `[Worker] [TOTAL_TIME_FAILED] ${taskInfo} | Total Lifespan: ${totalFailed}ms`
        );
        return new Response(
          JSON.stringify({
            success: false,
            error: "Destination rejected payload",
            status: destResponse.status,
            r2_error_body: errorBody,
            task_id: task.task_id,
            part_number: partNumber,
            timings_ms: {
              json_parse: parseDuration,
              source_fetch: fetchMs,
              fetch_ms: fetchMs,
              stream_upload: sendMs,
              send_ms: sendMs,
              upload_attempt: sendMs,
              total_execution: totalFailed,
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
      const totalMs = Number((performance.now() - workerStartTime).toFixed(2));
      const sendSec = Math.max(sendMs / 1000, 0.001);
      const sendMbps = ((bytes * 8) / sendSec / 1e6).toFixed(1);

      console.log(
        `[Worker] [SUCCESS] ${taskInfo} | fetch_ms=${fetchMs} send_ms=${sendMs} ` +
          `etag=${cleanEtag} fetch_mbps=${fetchMbps} send_mbps=${sendMbps}`
      );
      console.log(
        `[Worker] [TOTAL_TIME_SUCCESS] ${taskInfo} | wall_ms=${totalMs}`
      );

      return new Response(
        JSON.stringify({
          success: true,
          task_id: task.task_id,
          etag: cleanEtag,
          part_number: partNumber,
          bytes_processed: contentLength,
          timings_ms: {
            json_parse: parseDuration,
            // Aliases match both Worker logs and orch cloudflare_transfer parser.
            source_fetch: fetchMs,
            fetch_ms: fetchMs,
            stream_upload: sendMs,
            send_ms: sendMs,
            total_execution: totalMs,
          },
        }),
        {
          headers: { "Content-Type": "application/json" },
        }
      );
    } catch (error) {
      const totalExceptionDuration = Number(
        (performance.now() - workerStartTime).toFixed(2)
      );
      console.error(
        `[Worker] [CRITICAL_EXCEPTION] ${taskInfo} | Error: ${error.message} | Total Time: ${totalExceptionDuration}ms`
      );

      return new Response(
        JSON.stringify({
          success: false,
          error: error.message,
          total_execution_ms: totalExceptionDuration,
        }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
