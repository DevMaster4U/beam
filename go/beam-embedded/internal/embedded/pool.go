package embedded

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/beam/sn105/beam-embedded/internal/config"
	"github.com/beam/sn105/beam-embedded/internal/logutil"
	"github.com/beam/sn105/beam-embedded/internal/transfer"
	"github.com/beam/sn105/beam-embedded/internal/upstream"
)

// WorkerConfig is one embedded worker slot from WORKER_N env vars.
type WorkerConfig struct {
	Slot               int
	WalletName         string
	Hotkey             string
	InitialOrder       int
	MaxConcurrentTasks int
	WorkerID           string
	APIKey             string
	RegistrationSig    string
}

// Worker is a registered embedded worker with its own HTTP client.
type Worker struct {
	Slot               int
	WorkerID           string
	APIKey             string
	Hotkey             string
	IP                 string
	InitialOrder       int
	MaxConcurrentTasks int
	HTTPClient         *http.Client
	ActiveOfferIDs     map[string]struct{}
}

func (w *Worker) activeCount() int {
	return len(w.ActiveOfferIDs)
}

func (w *Worker) hasCapacity() bool {
	return w.activeCount() < w.MaxConcurrentTasks
}

// Pool runs worker transfer logic inside the orchestrator process.
type Pool struct {
	Settings config.Settings
	Upstream upstream.Client
	Engine   *transfer.Engine

	workers    []*Worker
	cursor     int
	log        Logger
	walletPath string
}

// Logger is satisfied by slog or a thin wrapper.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// StdLogger writes INFO/WARN/ERROR lines to stdout.
type StdLogger struct{}

func (StdLogger) Infof(format string, args ...any)  { fmt.Printf("INFO | "+format+"\n", args...) }
func (StdLogger) Warnf(format string, args ...any)  { fmt.Printf("WARN | "+format+"\n", args...) }
func (StdLogger) Errorf(format string, args ...any) { fmt.Printf("ERROR | "+format+"\n", args...) }

// NewPool creates an embedded worker pool.
func NewPool(settings config.Settings, up upstream.Client, walletPath string, log Logger) *Pool {
	if log == nil {
		log = StdLogger{}
	}
	return &Pool{
		Settings:   settings,
		Upstream:   up,
		Engine:     transfer.NewEngine(settings),
		log:        log,
		walletPath: walletPath,
	}
}

// WorkerCount returns the number of embedded workers.
func (p *Pool) WorkerCount() int { return len(p.workers) }

// Start registers embedded workers with BeamCore.
func (p *Pool) Start(ctx context.Context) error {
	configs, err := ParseWorkerConfigs(p.Settings)
	if err != nil {
		return err
	}
	if len(configs) == 0 {
		return fmt.Errorf("WORKER_GATEWAY_MODE=embedded requires WORKER_1 (or WORKER_1_HOTKEY) in env")
	}

	for _, cfg := range configs {
		client := newHTTPClient(cfg.MaxConcurrentTasks)
		ip, err := publicIP(ctx, client)
		if err != nil {
			ip = ""
		}

		workerID := cfg.WorkerID
		apiKey := cfg.APIKey
		if workerID == "" || apiKey == "" {
			workerID, apiKey, err = registerWorker(ctx, client, p.Settings.CoreServerURL, cfg, ip)
			if err != nil {
				return fmt.Errorf("embedded worker registration failed for slot %d: %w", cfg.Slot, err)
			}
		}

		w := &Worker{
			Slot:               cfg.Slot,
			WorkerID:           workerID,
			APIKey:             apiKey,
			Hotkey:             cfg.Hotkey,
			IP:                 ip,
			InitialOrder:       cfg.InitialOrder,
			MaxConcurrentTasks: cfg.MaxConcurrentTasks,
			HTTPClient:         client,
			ActiveOfferIDs:     make(map[string]struct{}),
		}
		p.workers = append(p.workers, w)
		p.log.Infof(
			"Embedded worker ready slot=%d worker_id=%s hotkey=%s",
			cfg.Slot, logutil.ShortID(workerID), shortHotkey(cfg.Hotkey),
		)
	}

	p.log.Infof(
		"Embedded predefined ETag config: early_submit=%v max_parallel=%d source_prefix=%q file_size=%d",
		p.Settings.EarlySubmit,
		p.Settings.PredefinedETAGMaxParallel,
		transfer.NormalizedCapabilityURL(p.Settings.PredefinedETAGSourceURL),
		p.Settings.PredefinedETAGSourceFileSz,
	)
	return nil
}

// DeliverTaskOfferBatch handles a worker_task_offer_batch from BeamCore.
func (p *Pool) DeliverTaskOfferBatch(ctx context.Context, batchID string, offers []map[string]any) (delivered, failed int) {
	batchUsedIPs := make(map[string]struct{})
	batchAssigned := make(map[string]struct{})

	for _, offer := range offers {
		taskID := transfer.TaskID(offer)
		offerID := transfer.OfferID(offer)
		tctx, validationErr := transfer.BuildContext(offer)
		if validationErr != "" || tctx == nil {
			p.log.Warnf(
				"Embedded batch offer invalid: batch=%s task=%s offer=%s error=%s",
				logutil.ShortID(batchID, 12), logutil.ShortID(taskID), logutil.ShortID(offerID), validationErr,
			)
			failed++
			continue
		}

		if transfer.UsesPredefinedETAGEarlySubmit(tctx, p.Settings) {
			p.log.Infof(
				"Embedded batch offer fast-path: batch=%s task=%s offer=%s chunk_size=%d",
				logutil.ShortID(batchID, 12), logutil.ShortID(taskID), logutil.ShortID(offerID), tctx.ChunkSize,
			)
		} else {
			reasons := transfer.PredefinedETAGEarlySubmitSkipReasons(tctx, p.Settings)
			p.log.Infof(
				"Embedded batch offer standard-path: batch=%s task=%s offer=%s reasons=%s",
				logutil.ShortID(batchID, 12), logutil.ShortID(taskID), logutil.ShortID(offerID),
				logutil.JoinSkipReasons(reasons),
			)
		}

		worker := p.selectWorker(batchUsedIPs, batchAssigned)
		if worker == nil {
			p.log.Warnf("No embedded worker capacity for batch=%s task=%s", batchID, taskID)
			failed++
			continue
		}

		batchAssigned[worker.WorkerID] = struct{}{}
		if worker.IP != "" {
			batchUsedIPs[worker.IP] = struct{}{}
		}

		p.log.Infof(
			"Embedded offer assigned: batch=%s task=%s offer=%s worker_slot=%d worker_id=%s",
			logutil.ShortID(batchID, 12), logutil.ShortID(taskID), logutil.ShortID(offerID),
			worker.Slot, logutil.ShortID(worker.WorkerID),
		)

		go func(w *Worker, off map[string]any, tc *transfer.Context) {
			if err := p.handleOffer(ctx, w, off, tc); err != nil {
				p.log.Errorf(
					"Embedded offer task failed: task=%s offer=%s error=%v",
					logutil.ShortID(transfer.TaskID(off)),
					logutil.ShortID(transfer.OfferID(off)),
					err,
				)
			}
		}(worker, offer, tctx)
		delivered++
	}

	p.log.Infof(
		"Embedded batch queued: batch=%s offers=%d delivered=%d failed=%d",
		logutil.ShortID(batchID, 12), len(offers), delivered, failed,
	)
	return delivered, failed
}

func (p *Pool) handleOffer(ctx context.Context, worker *Worker, offer map[string]any, tctx *transfer.Context) error {
	taskID := transfer.TaskID(offer)
	offerID := transfer.OfferID(offer)
	worker.ActiveOfferIDs[offerID] = struct{}{}
	defer delete(worker.ActiveOfferIDs, offerID)

	p.log.Infof(
		"Embedded offer handler started: task=%s offer=%s worker_slot=%d",
		logutil.ShortID(taskID), logutil.ShortID(offerID), worker.Slot,
	)

	deadlineUS := transfer.DeadlineUS(offer)
	estimated := transfer.EstimateTaskBytes(offer)
	if capacityErr := p.reserveCapacity(worker, estimated); capacityErr != "" {
		p.log.Warnf(
			"Embedded rejecting offer: task=%s offer=%s worker_slot=%d reason=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID), worker.Slot, capacityErr,
		)
		return p.Upstream.SendTaskReject(ctx, taskID, worker.WorkerID, offerID, capacityErr)
	}

	reasons := transfer.PredefinedETAGEarlySubmitSkipReasons(tctx, p.Settings)
	if len(reasons) > 0 {
		p.log.Infof(
			"Embedded fast path skipped: task=%s offer=%s worker_slot=%d reasons=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID), worker.Slot, logutil.JoinSkipReasons(reasons),
		)
	}

	if transfer.UsesPredefinedETAGEarlySubmit(tctx, p.Settings) {
		return p.handlePredefinedETAGOffer(ctx, worker, offer, taskID, offerID, tctx, deadlineUS)
	}

	p.log.Infof(
		"Embedded standard transfer: task=%s offer=%s worker_slot=%d",
		logutil.ShortID(taskID), logutil.ShortID(offerID), worker.Slot,
	)
	return p.handleStandardOffer(ctx, worker, offer, taskID, offerID, tctx, deadlineUS)
}

func (p *Pool) handlePredefinedETAGOffer(
	ctx context.Context,
	worker *Worker,
	offer map[string]any,
	taskID, offerID string,
	tctx *transfer.Context,
	deadlineUS int64,
) error {
	offerStartedAt := time.Now()
	fetchReady := transfer.NewFetchReady()

	if !transfer.ShouldBufferPredefinedETAGFetch(
		fetchReady,
		tctx.SourceURL,
		tctx.ChunkSize,
		transfer.IsObjectStoragePresignedURL(tctx.DestURL),
		transfer.IsCanaryDestination(tctx.DestURL),
		p.Settings,
	) {
		p.log.Warnf(
			"Embedded predefined handler buffer gate mismatch; using standard transfer: task=%s offer=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID),
		)
		return p.handleStandardOffer(ctx, worker, offer, taskID, offerID, tctx, deadlineUS)
	}

	execCtx, execCancel := context.WithCancel(ctx)
	uploadDone := make(chan error, 1)

	go func() {
		_ = p.Engine.Execute(execCtx, worker.HTTPClient, offer, tctx, deadlineUS, fetchReady, "[Embedded]")
	}()

	go func() {
		uploadDone <- p.Engine.RunPredefinedETAGBackgroundUpload(context.Background(), worker.HTTPClient, fetchReady, tctx)
	}()

	p.log.Infof(
		"Embedded predefined ETag: download + task_accept in parallel: task=%s offer=%s",
		logutil.ShortID(taskID), logutil.ShortID(offerID),
	)

	acceptTimeout := time.Duration(p.Settings.AcceptAckTimeoutSec * float64(time.Second))
	fetchTimeout := time.Duration(config.FetchTimeoutSec+5) * time.Second
	accepted, waitErr := transfer.WaitAcceptAndBufferedFetch(
		ctx,
		func(c context.Context) (bool, error) {
			resp, err := p.Upstream.SendTaskAccept(c, taskID, worker.WorkerID, offerID, config.WorkerVersion)
			if err != nil {
				return false, err
			}
			ok, _ := resp["accepted"].(bool)
			return ok, nil
		},
		fetchReady,
		acceptTimeout,
		fetchTimeout,
	)

	if !accepted {
		p.log.Warnf(
			"Embedded stopping download: task=%s offer=%s worker=%s reason=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID), logutil.ShortID(worker.WorkerID), waitErr,
		)
		execCancel()
		return nil
	}
	if waitErr != "" {
		p.log.Warnf(
			"Embedded predefined transfer failed: task=%s offer=%s worker=%s reason=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID), logutil.ShortID(worker.WorkerID), waitErr,
		)
		execCancel()
		return p.sendResult(ctx, worker, taskID, offerID, false, fetchReady.ChunkHash, "", waitErr)
	}

	if bytesErr := transfer.ValidateFetchReadyBytes(fetchReady); bytesErr != "" {
		p.log.Infof(
			"Embedded falling back to standard transfer (%s): task=%s offer=%s",
			bytesErr, logutil.ShortID(taskID), logutil.ShortID(offerID),
		)
		execCancel()
		result := p.Engine.Execute(ctx, worker.HTTPClient, offer, tctx, deadlineUS, nil, "[Embedded]")
		return p.sendResult(ctx, worker, taskID, offerID, result.Success, result.ChunkHash, result.ETag, result.ErrorMsg)
	}

	waited := transfer.WaitPredefinedETAGMinSubmitDelay(offerStartedAt, p.Settings.PredefinedETAGMinSubmitSec)
	if waited > 0 {
		p.log.Infof(
			"Embedded accept_ack + hash ready, waited %.3fs (min_submit=%.3fs) before submit: task=%s offer=%s",
			waited.Seconds(), p.Settings.PredefinedETAGMinSubmitSec, logutil.ShortID(taskID), logutil.ShortID(offerID),
		)
	} else {
		p.log.Infof(
			"Embedded accept_ack + hash ready, submitting: task=%s offer=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID),
		)
	}

	etag := config.PredefinedETag
	if fetchReady.ETag != "" {
		etag = fetchReady.ETag
	}
	if err := p.sendResult(ctx, worker, taskID, offerID, true, fetchReady.ChunkHash, etag, ""); err != nil {
		return err
	}

	go func() {
		if err := <-uploadDone; err != nil {
			p.log.Warnf(
				"Embedded background upload failed after task_result: task=%s offer=%s err=%v",
				logutil.ShortID(taskID), logutil.ShortID(offerID), err,
			)
			return
		}
		p.log.Infof(
			"Embedded background upload finished after task_result: task=%s offer=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID),
		)
	}()
	return nil
}

func (p *Pool) handleStandardOffer(
	ctx context.Context,
	worker *Worker,
	offer map[string]any,
	taskID, offerID string,
	tctx *transfer.Context,
	deadlineUS int64,
) error {
	resp, err := p.Upstream.SendTaskAccept(ctx, taskID, worker.WorkerID, offerID, config.WorkerVersion)
	if err != nil {
		return err
	}
	accepted, _ := resp["accepted"].(bool)
	if !accepted {
		reason, _ := resp["reason"].(string)
		if reason == "" {
			reason = "task_accept_rejected"
		}
		p.log.Warnf(
			"Embedded accept rejected: task=%s offer=%s worker=%s reason=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID), logutil.ShortID(worker.WorkerID), reason,
		)
		return nil
	}

	result := p.Engine.Execute(ctx, worker.HTTPClient, offer, tctx, deadlineUS, nil, "[Embedded]")
	return p.sendResult(ctx, worker, taskID, offerID, result.Success, result.ChunkHash, result.ETag, result.ErrorMsg)
}

func (p *Pool) sendResult(
	ctx context.Context,
	worker *Worker,
	taskID, offerID string,
	success bool,
	chunkHash, etag, errMsg string,
) error {
	payload := map[string]any{
		"task_id":   taskID,
		"offer_id":  offerID,
		"worker_id": worker.WorkerID,
		"success":   success,
	}
	if chunkHash != "" {
		payload["chunk_hash"] = chunkHash
	}
	if etag != "" {
		payload["etag"] = etag
	}
	if errMsg != "" {
		payload["error"] = errMsg
	}
	ack, err := p.Upstream.SendTaskResult(ctx, payload)
	if err != nil {
		return err
	}
	if completed, _ := ack["completed"].(bool); completed {
		p.log.Infof(
			"Embedded task completed on BeamCore: task=%s offer=%s worker=%s",
			logutil.ShortID(taskID), logutil.ShortID(offerID), logutil.ShortID(worker.WorkerID),
		)
	} else {
		p.log.Warnf(
			"Embedded task result not completed: task=%s offer=%s worker=%s received=%v completed=%v",
			logutil.ShortID(taskID), logutil.ShortID(offerID), logutil.ShortID(worker.WorkerID),
			ack["received"], ack["completed"],
		)
	}
	return nil
}

func (p *Pool) reserveCapacity(worker *Worker, estimatedBytes int) string {
	if estimatedBytes > p.Settings.MaxInFlightBytes {
		return fmt.Sprintf("task_too_large:%d", estimatedBytes)
	}
	if !worker.hasCapacity() {
		return fmt.Sprintf("queue_full:%d", worker.activeCount())
	}
	return ""
}

func (p *Pool) selectWorker(batchUsedIPs, batchAssigned map[string]struct{}) *Worker {
	if len(p.workers) == 0 {
		return nil
	}
	poolSize := len(p.workers)
	start := p.cursor % poolSize
	inBatch := len(batchUsedIPs) > 0 || len(batchAssigned) > 0

	eligible := func(w *Worker, allowUsedIP bool) bool {
		if !w.hasCapacity() {
			return false
		}
		if _, ok := batchAssigned[w.WorkerID]; ok {
			return false
		}
		if !allowUsedIP && w.IP != "" {
			if _, ok := batchUsedIPs[w.IP]; ok {
				return false
			}
		}
		return true
	}

	pick := func(allowUsedIP bool) *Worker {
		for offset := 0; offset < poolSize; offset++ {
			idx := (start + offset) % poolSize
			w := p.workers[idx]
			if !eligible(w, allowUsedIP) {
				continue
			}
			p.cursor = (idx + 1) % poolSize
			return w
		}
		return nil
	}

	if inBatch {
		if w := pick(false); w != nil {
			return w
		}
		return pick(true)
	}
	return pick(true)
}

func newHTTPClient(maxTasks int) *http.Client {
	maxConns := maxTasks * 4
	if maxConns < 16 {
		maxConns = 16
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxConns
	transport.MaxIdleConnsPerHost = max(maxTasks*2, 8)
	return &http.Client{
		Timeout:   time.Duration(config.SendTimeoutSec) * time.Second,
		Transport: transport,
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func shortHotkey(hotkey string) string {
	if len(hotkey) > 16 {
		return hotkey[:16]
	}
	return hotkey
}

func publicIP(ctx context.Context, client *http.Client) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.ipify.org?format=text", nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

func registerWorker(
	ctx context.Context,
	client *http.Client,
	apiURL string,
	cfg WorkerConfig,
	ip string,
) (workerID, apiKey string, err error) {
	if cfg.Hotkey == "" {
		return "", "", fmt.Errorf("missing hotkey for slot %d", cfg.Slot)
	}
	if cfg.RegistrationSig == "" {
		return "", "", fmt.Errorf(
			"slot %d: set WORKER_%d_REGISTRATION_SIGNATURE or WORKER_%d_WORKER_ID + WORKER_%d_API_KEY",
			cfg.Slot, cfg.Slot, cfg.Slot, cfg.Slot,
		)
	}

	port := 9000
	message := fmt.Sprintf("%s:%s:%d", cfg.Hotkey, ip, port)
	_ = message

	payload := map[string]any{
		"hotkey":                 cfg.Hotkey,
		"ip":                     ip,
		"port":                   port,
		"claimed_bandwidth_mbps": 100,
		"coldkey":                cfg.Hotkey,
		"payment_pubkey":         paymentPubkey(cfg.Hotkey),
		"signature":              cfg.RegistrationSig,
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, strings.TrimRight(apiURL, "/")+"/workers/register", bytes.NewReader(body),
	)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return "", "", err
	}
	if ok, _ := data["success"].(bool); !ok {
		return "", "", fmt.Errorf("registration failed: %s", string(raw))
	}
	workerID, _ = data["worker_id"].(string)
	apiKey, _ = data["api_key"].(string)
	if workerID == "" || apiKey == "" {
		return "", "", fmt.Errorf("registration missing worker_id/api_key")
	}
	return workerID, apiKey, nil
}

func paymentPubkey(hotkey string) string {
	sum := sha256.Sum256([]byte("payment:" + hotkey))
	return hex.EncodeToString(sum[:])
}

// ParseWorkerConfigs reads WORKER_1, WORKER_2, ... from environment.
func ParseWorkerConfigs(settings config.Settings) ([]WorkerConfig, error) {
	_ = settings
	defaultWallet := strings.TrimSpace(envDefault("WORKER_WALLET_NAME", ""))
	defaultMax := max(1, envIntDefault("WORKER_MAX_CONCURRENT_TASKS", 4))

	var configs []WorkerConfig
	for idx := 1; ; idx++ {
		combined := strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d", idx)))
		hotkey := strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d_HOTKEY", idx)))
		walletName := strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d_WALLET_NAME", idx)))

		if combined != "" {
			if parts := strings.SplitN(combined, ":", 2); len(parts) == 2 {
				if walletName == "" {
					walletName = strings.TrimSpace(parts[0])
				}
				if hotkey == "" {
					hotkey = strings.TrimSpace(parts[1])
				}
			} else if hotkey == "" {
				hotkey = combined
			}
		}
		if hotkey == "" {
			break
		}
		if walletName == "" {
			walletName = defaultWallet
		}

		order := idx - 1
		if raw := strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d_ORDER", idx))); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil {
				order = v
			}
		}

		maxTasks := defaultMax
		if raw := strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d_MAX_CONCURRENT_TASKS", idx))); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v > 0 {
				maxTasks = v
			}
		}

		configs = append(configs, WorkerConfig{
			Slot:               idx,
			WalletName:         walletName,
			Hotkey:             hotkey,
			InitialOrder:       order,
			MaxConcurrentTasks: maxTasks,
			WorkerID:           strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d_WORKER_ID", idx))),
			APIKey:             strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d_API_KEY", idx))),
			RegistrationSig:    strings.TrimSpace(os.Getenv(fmt.Sprintf("WORKER_%d_REGISTRATION_SIGNATURE", idx))),
		})
	}
	return configs, nil
}

func envDefault(name, defaultVal string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return defaultVal
}

func envIntDefault(name string, defaultVal int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}
