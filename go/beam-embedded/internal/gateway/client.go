package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam/sn105/beam-embedded/internal/config"
	"github.com/beam/sn105/beam-embedded/internal/embedded"
	"github.com/beam/sn105/beam-embedded/internal/logutil"
	"github.com/beam/sn105/beam-embedded/internal/upstream"
	"github.com/beam/sn105/beam-embedded/internal/wallet"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Client maintains the orch-gateway WebSocket and relays worker messages to BeamCore.
type Client struct {
	cfg    config.OrchestratorSettings
	hotkey string
	signer *wallet.Signer
	pool   *embedded.Pool
	log    embedded.Logger

	mu           sync.Mutex
	conn         *websocket.Conn
	connected    bool
	pending      map[string]chan map[string]any
	desiredReady bool
	registered   bool
	apiKey       string

	httpClient *http.Client
}

// New creates a gateway client that implements upstream.Client.
func New(cfg config.OrchestratorSettings, hotkey string, signer *wallet.Signer, pool *embedded.Pool, log embedded.Logger) *Client {
	if log == nil {
		log = embedded.StdLogger{}
	}
	return &Client{
		cfg:          cfg,
		hotkey:       hotkey,
		signer:       signer,
		pool:         pool,
		log:          log,
		pending:      make(map[string]chan map[string]any),
		desiredReady: cfg.Ready,
		apiKey:       cfg.BeamcoreAPIKey,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// SetAPIKey stores the BeamCore API key used for WebSocket auth.
func (c *Client) SetAPIKey(key string) {
	c.apiKey = key
}

var _ upstream.Client = (*Client)(nil)

func (c *Client) wsURL() string {
	base := c.cfg.OrchGatewayURL
	base = strings.Replace(base, "https://", "wss://", 1)
	base = strings.Replace(base, "http://", "ws://", 1)
	return base + "/ws/orchestrators/" + c.hotkey
}

func (c *Client) localIP() string {
	if c.cfg.ExternalIP != "" {
		return c.cfg.ExternalIP
	}
	return "127.0.0.1"
}

func (c *Client) orchURL() string {
	return fmt.Sprintf("http://%s:%d", c.localIP(), c.cfg.APIPort)
}

func (c *Client) gatewayURL() string {
	if c.cfg.WorkerGatewayURL != "" {
		return c.cfg.WorkerGatewayURL
	}
	return c.orchURL()
}

// Run connects to orch-gateway, registers, and processes push messages until ctx is done.
func (c *Client) Run(ctx context.Context) error {
	if c.apiKey == "" {
		return fmt.Errorf("BeamCore API key not configured")
	}
	if c.cfg.OrchGatewayURL == "" {
		return fmt.Errorf("ORCH_GATEWAY_URL is required")
	}

	reconnect := 5 * time.Second
	for {
		if err := c.connect(ctx); err != nil {
			c.log.Warnf("gateway connect failed: %v", err)
		} else {
			readErr := c.readLoop(ctx)
			c.disconnect()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.log.Warnf("gateway disconnected: %v", readErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnect):
		}
		if reconnect < 60*time.Second {
			reconnect = time.Duration(float64(reconnect) * 1.5)
			if reconnect > 60*time.Second {
				reconnect = 60 * time.Second
			}
		}
	}
}

func (c *Client) connect(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: time.Duration(c.cfg.WSOpenTimeoutSec * float64(time.Second)),
	}
	headers := http.Header{}
	headers.Set("x-api-key", c.apiKey)
	if c.signer != nil {
		ts := fmt.Sprintf("%d", time.Now().Unix())
		msg := c.hotkey + ":" + ts
		if sig, err := c.signer.SignHex(msg); err == nil {
			headers.Set("x-signature", sig)
			headers.Set("x-timestamp", ts)
		} else {
			c.log.Warnf("WS auth sign failed, using unsigned: %v", err)
			headers.Set("x-signature", "unsigned")
			headers.Set("x-timestamp", ts)
		}
	} else {
		headers.Set("x-signature", "unsigned")
		headers.Set("x-timestamp", fmt.Sprintf("%d", time.Now().Unix()))
	}

	c.log.Infof("Connecting gateway WebSocket: %s", c.wsURL())
	conn, _, err := dialer.DialContext(ctx, c.wsURL(), headers)
	if err != nil {
		return err
	}
	conn.SetPingHandler(func(appData string) error {
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.mu.Unlock()

	c.log.Infof("Gateway WebSocket connected hotkey=%s", logutil.ShortID(c.hotkey, 16))
	return c.sendRegister()
}

func (c *Client) disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.connected = false
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
}

func (c *Client) sendRegister() error {
	regMsg := fmt.Sprintf("%s:%s:%s", c.hotkey, c.orchURL(), c.cfg.Region)
	signature := ""
	if c.signer != nil {
		if sig, err := c.signer.SignHex(regMsg); err == nil {
			signature = sig
		}
	}
	msg := map[string]any{
		"type":           "register",
		"url":            c.orchURL(),
		"region":         c.cfg.Region,
		"max_workers":    c.cfg.MaxWorkers,
		"uid":            c.cfg.OrchestratorUID,
		"fee_percentage": c.cfg.FeePercentage,
		"ready":          c.desiredReady,
		"signature":      signature,
		"gateway_url":    c.gatewayURL(),
	}
	return c.writeJSON(msg)
}

func (c *Client) readLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn == nil {
			return fmt.Errorf("connection closed")
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Duration(c.cfg.WSPingTimeoutSec * float64(time.Second))))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var data map[string]any
		if err := json.Unmarshal(raw, &data); err != nil {
			c.log.Warnf("invalid gateway JSON: %v", err)
			continue
		}
		c.handleMessage(data)
	}
}

func (c *Client) handleMessage(data map[string]any) {
	msgType, _ := data["type"].(string)
	requestID, _ := data["request_id"].(string)
	if requestID != "" {
		c.mu.Lock()
		ch := c.pending[requestID]
		if ch != nil {
			delete(c.pending, requestID)
		}
		c.mu.Unlock()
		if ch != nil {
			ch <- data
			return
		}
	}

	switch msgType {
	case "connected":
		c.log.Infof("Gateway connected ack hotkey=%v", data["hotkey"])
	case "register_ack", "register_result":
		c.registered = true
		c.log.Infof("Orchestrator registration ack: type=%s status=%v", msgType, data["status"])
		if c.desiredReady {
			_ = c.setReady(context.Background(), true)
		}
	case "register_error":
		c.log.Errorf("Orchestrator registration failed: %v", data["error"])
	case "worker_task_offer_batch":
		batchID, _ := data["batch_id"].(string)
		offers, _ := data["offers"].([]any)
		offerMaps := make([]map[string]any, 0, len(offers))
		for _, o := range offers {
			if m, ok := o.(map[string]any); ok {
				offerMaps = append(offerMaps, m)
			}
		}
		c.log.Infof(
			"worker_task_offer_batch batch=%s offers=%d",
			logutil.ShortID(batchID, 12), len(offerMaps),
		)
		go c.pool.DeliverTaskOfferBatch(context.Background(), batchID, offerMaps)
	case "upstream_down":
		c.log.Warnf("BeamCore upstream degraded: %v", data["message"])
	case "upstream_ok":
		c.log.Infof("BeamCore upstream recovered: %v", data["message"])
	case "error":
		c.log.Warnf("gateway error: %v", data["message"])
	default:
		if msgType != "" {
			log.Printf("gateway push type=%s", msgType)
		}
	}
}

func (c *Client) setReady(ctx context.Context, ready bool) error {
	_, err := c.sendWSRequest(ctx, map[string]any{
		"type":  "set_ready",
		"ready": ready,
	}, time.Duration(c.cfg.WSRequestTimeoutSec*float64(time.Second)))
	return err
}

func (c *Client) writeJSON(msg map[string]any) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("websocket not connected")
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, raw)
}

func (c *Client) sendWSRequest(ctx context.Context, msg map[string]any, timeout time.Duration) (map[string]any, error) {
	requestID := uuid.New().String()
	msg["request_id"] = requestID
	ch := make(chan map[string]any, 1)
	c.mu.Lock()
	if !c.connected || c.conn == nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("websocket not connected")
	}
	c.pending[requestID] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
	}()

	if err := c.writeJSON(msg); err != nil {
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("request cancelled")
		}
		if resp != nil && resp["type"] == "error" {
			return resp, fmt.Errorf("%v", resp["message"])
		}
		return resp, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("gateway request timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SendTaskAccept relays task_accept to BeamCore.
func (c *Client) SendTaskAccept(
	ctx context.Context, taskID, workerID, offerID, workerVersion string,
) (map[string]any, error) {
	if offerID == "" {
		offerID = taskID
	}
	return c.sendWSRequest(ctx, map[string]any{
		"type":           "task_accept",
		"task_id":        taskID,
		"worker_id":      workerID,
		"offer_id":       offerID,
		"worker_version": workerVersion,
	}, time.Duration(c.cfg.TaskAcceptTimeoutSec*float64(time.Second)))
}

// SendTaskReject relays task_reject to BeamCore.
func (c *Client) SendTaskReject(
	ctx context.Context, taskID, workerID, offerID, reason string,
) error {
	if offerID == "" {
		offerID = taskID
	}
	msg := map[string]any{
		"type":      "task_reject",
		"task_id":   taskID,
		"worker_id": workerID,
		"offer_id":  offerID,
	}
	if reason != "" {
		msg["reason"] = reason
	}
	_, err := c.sendWSRequest(ctx, msg, time.Duration(c.cfg.TaskAcceptTimeoutSec*float64(time.Second)))
	return err
}

// SendTaskResult relays task_result to BeamCore.
func (c *Client) SendTaskResult(ctx context.Context, payload map[string]any) (map[string]any, error) {
	taskID, _ := payload["task_id"].(string)
	offerID, _ := payload["offer_id"].(string)
	if offerID == "" {
		offerID = taskID
	}
	msg := map[string]any{
		"type":      "task_result",
		"task_id":   taskID,
		"offer_id":  offerID,
		"worker_id": payload["worker_id"],
		"success":   payload["success"],
	}
	for _, key := range []string{"etag", "chunk_hash", "error"} {
		if v, ok := payload[key]; ok && v != nil {
			msg[key] = v
		}
	}
	return c.sendWSRequest(ctx, msg, time.Duration(c.cfg.TaskResultTimeoutSec*float64(time.Second)))
}
