package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam/sn105/beam-embedded/internal/config"
)

// HTTPDoer is satisfied by *http.Client.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Engine runs fetch/upload transfer logic.
type Engine struct {
	Cfg        config.Settings
	FastPathSem chan struct{}
}

// NewEngine creates a transfer engine with fast-path parallelism limit.
func NewEngine(cfg config.Settings) *Engine {
	sem := make(chan struct{}, cfg.PredefinedETAGMaxParallel)
	return &Engine{Cfg: cfg, FastPathSem: sem}
}

// ExecutionResult mirrors worker.TaskExecutionResult.
type ExecutionResult struct {
	Success          bool
	BytesTransferred int
	DurationMS       float64
	ChunkHash        string
	ETag             string
	ErrorMsg         string
}

// Execute runs a transfer task and returns normalized metrics.
func (e *Engine) Execute(
	ctx context.Context,
	client HTTPDoer,
	task map[string]any,
	tctx *Context,
	deadlineUS int64,
	fetchReady *FetchReady,
	logPrefix string,
) ExecutionResult {
	start := time.Now()
	result := ExecutionResult{}

	bytes, ok, errMsg, chunkHash, etag := e.executeTransfer(
		ctx, client, task, tctx, deadlineUS, fetchReady, logPrefix,
	)
	result.Success = ok
	result.BytesTransferred = bytes
	result.ChunkHash = chunkHash
	result.ETag = etag
	result.ErrorMsg = errMsg
	result.DurationMS = float64(time.Since(start).Milliseconds())
	if errMsg != "" && fetchReady != nil {
		select {
		case <-fetchReady.Done():
		default:
			fetchReady.SignalError(errMsg)
		}
	}
	return result
}

func (e *Engine) executeTransfer(
	ctx context.Context,
	client HTTPDoer,
	task map[string]any,
	tctx *Context,
	deadlineUS int64,
	fetchReady *FetchReady,
	logPrefix string,
) (totalBytes int, success bool, errMsg, chunkHash, etag string) {
	if deadlineUS > 0 {
		remaining := time.Until(time.UnixMicro(deadlineUS))
		if remaining <= 0 {
			return 0, false, "Deadline exceeded before chunk 0", "", ""
		}
	}

	expectedHash := ChunkHashFromOffer(task)
	isCanary := IsCanaryDestination(tctx.DestURL)

	fetchHeaders := BuildFetchHeaders(tctx.RangeStart, tctx.ChunkSize, tctx.TotalSize)
	for k, v := range tctx.SourceHeaders {
		fetchHeaders[k] = v
	}

	bytesFetched, hash, responseETag, _, _, sendMS, err := e.fetchAndSendChunk(
		ctx,
		client,
		tctx.SourceURL,
		tctx.DestURL,
		tctx.TransferID,
		0,
		tctx.ChunkSize,
		int(tctx.RangeStart),
		expectedHash,
		fetchHeaders,
		tctx.DestHeaders,
		isCanary,
		fetchReady,
	)
	if err != nil {
		return totalBytes, false, err.Error(), hash, responseETag
	}
	if bytesFetched != tctx.ChunkSize {
		return totalBytes, false,
			fmt.Sprintf("source range returned %d bytes, expected %d", bytesFetched, tctx.ChunkSize),
			hash, responseETag
	}
	if isCanary {
		return bytesFetched, true, "", hash, ""
	}
	if tctx.ETagRequired && responseETag == "" {
		return bytesFetched, false, "missing ETag from storage PUT response", hash, ""
	}
	_ = sendMS
	return bytesFetched, true, "", hash, responseETag
}

func (e *Engine) fetchAndSendChunk(
	ctx context.Context,
	client HTTPDoer,
	sourceURL, destinationURL, transferID string,
	chunkIndex, chunkSize, uploadOffset int,
	expectedChunkHash string,
	fetchHeaders, destHeaders map[string]string,
	isCanary bool,
	fetchReady *FetchReady,
) (bytesTransferred int, chunkHash, etag string, responseCode int, fetchMS, sendMS float64, err error) {
	isObjectStorage := IsObjectStoragePresignedURL(destinationURL)
	bufferFetch := ShouldBufferPredefinedETAGFetch(
		fetchReady,
		sourceURL,
		chunkSize,
		isObjectStorage,
		isCanary,
		e.Cfg,
	)

	if bufferFetch {
		return e.bufferedPredefinedFetch(
			ctx, client, sourceURL, chunkSize, expectedChunkHash, fetchHeaders, fetchReady,
		)
	}
	return e.streamFetchAndSend(
		ctx, client, sourceURL, destinationURL, transferID, chunkIndex,
		chunkSize, uploadOffset, expectedChunkHash, fetchHeaders, destHeaders, isCanary, isObjectStorage,
	)
}

func (e *Engine) bufferedPredefinedFetch(
	ctx context.Context,
	client HTTPDoer,
	sourceURL string,
	expectedMaxBytes int,
	expectedChunkHash string,
	fetchHeaders map[string]string,
	fetchReady *FetchReady,
) (int, string, string, int, float64, float64, error) {
	select {
	case e.FastPathSem <- struct{}{}:
		defer func() { <-e.FastPathSem }()
	case <-ctx.Done():
		return 0, "", "", 0, 0, 0, ctx.Err()
	}

	fetchStarted := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		fetchReady.SignalError(err.Error())
		return 0, "", "", 0, 0, 0, err
	}
	for k, v := range fetchHeaders {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		fetchReady.SignalError(err.Error())
		return 0, "", "", 0, 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		err = fmt.Errorf("GET %s: HTTP %d", RedactURL(sourceURL), resp.StatusCode)
		fetchReady.SignalError(err.Error())
		return 0, "", "", 0, 0, 0, err
	}

	if cl := resp.Header.Get("Content-Length"); cl != "" && expectedMaxBytes > 0 {
		var responseSize int
		fmt.Sscanf(cl, "%d", &responseSize)
		if responseSize > expectedMaxBytes {
			err = fmt.Errorf("response too large: %d bytes > expected %d", responseSize, expectedMaxBytes)
			fetchReady.SignalError(err.Error())
			return 0, "", "", 0, 0, 0, err
		}
	}

	buf := make([]byte, 0, expectedMaxBytes)
	tmp := make([]byte, config.FetchStreamChunkSize)
	bytesTransferred := 0
	for {
		n, readErr := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			bytesTransferred += n
			if expectedMaxBytes > 0 && bytesTransferred > expectedMaxBytes {
				err = fmt.Errorf(
					"response exceeded expected size while buffering: %d bytes > expected %d",
					bytesTransferred, expectedMaxBytes,
				)
				fetchReady.SignalError(err.Error())
				return 0, "", "", 0, 0, 0, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			fetchReady.SignalError(readErr.Error())
			return 0, "", "", 0, 0, 0, readErr
		}
	}

	fetchMS := float64(time.Since(fetchStarted).Milliseconds())
	sum := sha256.Sum256(buf)
	chunkHash := hex.EncodeToString(sum[:])
	if expectedChunkHash != "" && !strings.EqualFold(chunkHash, expectedChunkHash) {
		err = fmt.Errorf("chunk hash mismatch: expected %s, got %s", expectedChunkHash, chunkHash)
		fetchReady.SignalError(err.Error())
		return 0, "", "", 0, fetchMS, 0, err
	}

	fetchReady.SignalReady(bytesTransferred, chunkHash, fetchMS, config.PredefinedETag, buf)
	return bytesTransferred, chunkHash, config.PredefinedETag, 200, fetchMS, 0, nil
}

func (e *Engine) streamFetchAndSend(
	ctx context.Context,
	client HTTPDoer,
	sourceURL, destinationURL, transferID string,
	chunkIndex, chunkSize, uploadOffset int,
	expectedChunkHash string,
	fetchHeaders, destHeaders map[string]string,
	isCanary, isObjectStorage bool,
) (int, string, string, int, float64, float64, error) {
	pr, pw := io.Pipe()
	hasher := sha256.New()
	var wg sync.WaitGroup
	var fetchErr error
	bytesTransferred := 0
	fetchStarted := time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer pw.Close()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
		if err != nil {
			fetchErr = err
			return
		}
		for k, v := range fetchHeaders {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			fetchErr = err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			fetchErr = fmt.Errorf("GET %s: HTTP %d", RedactURL(sourceURL), resp.StatusCode)
			return
		}
		tmp := make([]byte, config.FetchStreamChunkSize)
		for {
			n, readErr := resp.Body.Read(tmp)
			if n > 0 {
				hasher.Write(tmp[:n])
				bytesTransferred += n
				if chunkSize > 0 && bytesTransferred > chunkSize {
					fetchErr = fmt.Errorf("response exceeded expected size: %d > %d", bytesTransferred, chunkSize)
					return
				}
				if _, err := pw.Write(tmp[:n]); err != nil {
					fetchErr = err
					return
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				fetchErr = readErr
				return
			}
		}
	}()

	chunkHash := ""
	var etag string
	var response *http.Response
	sendStarted := time.Now()
	sendMS := 0.0

	if isCanary {
		_, _ = io.Copy(io.Discard, pr)
		wg.Wait()
		fetchMS := float64(time.Since(fetchStarted).Milliseconds())
		if fetchErr != nil {
			return 0, "", "", 0, fetchMS, 0, fetchErr
		}
		return bytesTransferred, hex.EncodeToString(hasher.Sum(nil)), "", 0, fetchMS, 0, nil
	}

	var sendErr error
	if isObjectStorage {
		response, sendErr = e.putObjectStorage(ctx, client, destinationURL, pr, chunkSize, destHeaders)
	} else {
		response, sendErr = e.postChunk(ctx, client, destinationURL, pr, transferID, chunkIndex, uploadOffset, chunkSize, destHeaders)
	}

	wg.Wait()
	fetchMS := float64(time.Since(fetchStarted).Milliseconds())
	if fetchErr != nil {
		return 0, "", "", 0, fetchMS, 0, fetchErr
	}
	if sendErr != nil {
		return 0, "", "", 0, fetchMS, 0, sendErr
	}
	sendMS = float64(time.Since(sendStarted).Milliseconds())
	chunkHash = hex.EncodeToString(hasher.Sum(nil))
	if expectedChunkHash != "" && !strings.EqualFold(chunkHash, expectedChunkHash) {
		return 0, chunkHash, "", 0, fetchMS, sendMS,
			fmt.Errorf("chunk hash mismatch: expected %s, got %s", expectedChunkHash, chunkHash)
	}
	etag = response.Header.Get("ETag")
	if etag == "" {
		etag = response.Header.Get("etag")
	}
	return bytesTransferred, chunkHash, etag, response.StatusCode, fetchMS, sendMS, nil
}

func (e *Engine) putObjectStorage(
	ctx context.Context,
	client HTTPDoer,
	destinationURL string,
	body io.Reader,
	expectedMaxBytes int,
	extraHeaders map[string]string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, destinationURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if expectedMaxBytes > 0 {
		req.ContentLength = int64(expectedMaxBytes)
		req.Header.Set("Content-Length", fmt.Sprintf("%d", expectedMaxBytes))
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("PUT %s: HTTP %d", RedactURL(destinationURL), resp.StatusCode)
	}
	return resp, nil
}

func (e *Engine) postChunk(
	ctx context.Context,
	client HTTPDoer,
	destinationURL string,
	body io.Reader,
	transferID string,
	chunkIndex, uploadOffset, chunkSize int,
	extraHeaders map[string]string,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, destinationURL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Transfer-ID", transferID)
	req.Header.Set("X-Chunk-ID", fmt.Sprintf("chunk_%d", chunkIndex))
	req.Header.Set("X-Offset", fmt.Sprintf("%d", uploadOffset))
	req.Header.Set("X-Length", fmt.Sprintf("%d", chunkSize))
	req.Header.Set("X-Total-Size", fmt.Sprintf("%d", chunkSize))
	if chunkSize > 0 {
		req.ContentLength = int64(chunkSize)
		req.Header.Set("Content-Length", fmt.Sprintf("%d", chunkSize))
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, fmt.Errorf("POST %s: HTTP %d", RedactURL(destinationURL), resp.StatusCode)
	}
	return resp, nil
}

// RunPredefinedETAGBackgroundUpload PUTs buffered body after download completes.
func (e *Engine) RunPredefinedETAGBackgroundUpload(
	ctx context.Context,
	client HTTPDoer,
	fetchReady *FetchReady,
	tctx *Context,
) error {
	<-fetchReady.Done()
	if fetchReady.Error != "" || !fetchReady.Ready || len(fetchReady.Buffer) == 0 {
		if fetchReady.Error != "" {
			return fmt.Errorf("buffer not ready: %s", fetchReady.Error)
		}
		return fmt.Errorf("buffer not ready")
	}
	_, err := e.putObjectStorage(ctx, client, tctx.DestURL, bytesReader(fetchReady.Buffer), tctx.ChunkSize, tctx.DestHeaders)
	return err
}

func bytesReader(b []byte) io.Reader {
	return &byteReader{b: b}
}

type byteReader struct {
	b []byte
	i int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

// PrewarmOrigin issues HEAD against origin root to warm DNS/TLS.
func PrewarmOrigin(ctx context.Context, client HTTPDoer, origin string, timeout time.Duration) {
	url := strings.TrimRight(origin, "/") + "/"
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodHead, url, nil)
	if err != nil {
		return
	}
	resp, err := client.Do(req)
	if err == nil && resp != nil {
		resp.Body.Close()
	}
}

// PrewarmForTransfer warms source and destination origins.
func PrewarmForTransfer(ctx context.Context, client HTTPDoer, cfg config.Settings, sourceURL, destURL string) {
	if !cfg.PrewarmEnabled {
		return
	}
	timeout := time.Duration(cfg.PrewarmTimeoutSec * float64(time.Second))
	for _, origin := range []string{URLOrigin(sourceURL), URLOrigin(destURL)} {
		if origin != "" {
			PrewarmOrigin(ctx, client, origin, timeout)
		}
	}
}
