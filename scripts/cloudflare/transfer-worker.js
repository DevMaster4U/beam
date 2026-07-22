/**
 * Cloudflare Worker: stream Beam chunk source → dest multipart PUT.
 *
 * Deploy (wrangler):
 *   wrangler deploy scripts/cloudflare/transfer-worker.js
 *
 * Request: POST application/json BeamCore task/offer body
 * Response JSON: { success, task_id, etag, part_number, timings_ms, ... }
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
      const parseDuration = (performance.now() - parseStart).toFixed(2);

      let partNumber = null;
      try {
        partNumber = new URL(task.dest_url).searchParams.get("partNumber");
      } catch (_) {
        partNumber = null;
      }
      taskInfo = `Task: ${task.task_id} | Part: ${partNumber}`;
      console.log(
        `[Worker] [START] ${taskInfo} | Size: ${task.chunk_size} bytes | JSON Parse Time: ${parseDuration}ms`
      );

      const fetchStart = performance.now();
      const srcResponse = await fetch(task.source_url, {
        method: "GET",
        headers: task.source_headers || {},
      });
      const fetchDuration = (performance.now() - fetchStart).toFixed(2);

      if (!srcResponse.ok) {
        const errorText = await srcResponse.text();
        console.error(
          `[Worker] [FETCH_FAIL] ${taskInfo} | After ${fetchDuration}ms | Status: ${srcResponse.status} | Error: ${errorText}`
        );
        return new Response(`Source Fetch Failed: ${srcResponse.status}`, {
          status: srcResponse.status,
        });
      }

      console.log(
        `[Worker] [FETCH_DONE] ${taskInfo} | Source response received in ${fetchDuration}ms. Starting stream to destination...`
      );

      const uploadStart = performance.now();
      const destResponse = await fetch(task.dest_url, {
        method: "PUT",
        headers: {
          "Content-Type": "application/octet-stream",
          "Content-Length":
            srcResponse.headers.get("Content-Length") ||
            String(task.chunk_size ?? ""),
        },
        body: srcResponse.body,
      });
      const uploadDuration = (performance.now() - uploadStart).toFixed(2);

      if (!destResponse.ok) {
        const errorBody = await destResponse.text();
        const totalFailedDuration = (performance.now() - workerStartTime).toFixed(2);

        console.error(
          `[Worker] [UPLOAD_FAIL] ${taskInfo} | Upload Attempt Duration: ${uploadDuration}ms | Status: ${destResponse.status} | R2 Error: ${errorBody}`
        );
        console.error(
          `[Worker] [TOTAL_TIME_FAILED] ${taskInfo} | Total Lifespan: ${totalFailedDuration}ms`
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
              json_parse: parseFloat(parseDuration),
              source_fetch: parseFloat(fetchDuration),
              upload_attempt: parseFloat(uploadDuration),
              total_execution: parseFloat(totalFailedDuration),
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
      const totalSuccessDuration = (performance.now() - workerStartTime).toFixed(2);

      console.log(
        `[Worker] [SUCCESS] ${taskInfo} | Stream Upload Time: ${uploadDuration}ms | ETag: ${cleanEtag}`
      );
      console.log(
        `[Worker] [TOTAL_TIME_SUCCESS] ${taskInfo} | Total Execution Lifespan: ${totalSuccessDuration}ms`
      );

      return new Response(
        JSON.stringify({
          success: true,
          task_id: task.task_id,
          etag: cleanEtag,
          part_number: partNumber,
          bytes_processed: srcResponse.headers.get("Content-Length"),
          timings_ms: {
            json_parse: parseFloat(parseDuration),
            source_fetch: parseFloat(fetchDuration),
            stream_upload: parseFloat(uploadDuration),
            total_execution: parseFloat(totalSuccessDuration),
          },
        }),
        {
          headers: { "Content-Type": "application/json" },
        }
      );
    } catch (error) {
      const totalExceptionDuration = (performance.now() - workerStartTime).toFixed(2);
      console.error(
        `[Worker] [CRITICAL_EXCEPTION] ${taskInfo} | Error: ${error.message} | Total Time: ${totalExceptionDuration}ms`
      );

      return new Response(
        JSON.stringify({
          success: false,
          error: error.message,
          total_execution_ms: parseFloat(totalExceptionDuration),
        }),
        {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }
      );
    }
  },
};
