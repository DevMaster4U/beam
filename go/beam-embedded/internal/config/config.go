package config

import (
	"os"
	"strconv"
	"strings"
)

const (
	DefaultChunkSizeBytes           = 4 * 1024 * 1024
	PredefinedETagChunkSizeBytes    = 30 * 1024 * 1024
	PredefinedETag                  = `"281ed1d5ae50e8419f9b978aab16de83"`
	DefaultPredefinedETAGSourceFile = 10 * 1024 * 1024 * 1024
	FetchTimeoutSec                 = 30
	SendTimeoutSec                  = 120
	FetchStreamChunkSize            = 512 * 1024
	MaxRetries                      = 3
	WorkerVersion                   = "0.2.0-go"
)

// Settings mirrors the Python worker/orchestrator env knobs used by embedded mode.
type Settings struct {
	CoreServerURL              string
	WalletPath                 string
	MaxConcurrentTasks         int
	MaxInFlightBytes           int
	AcceptAckTimeoutSec        float64
	EarlySubmit                bool
	PredefinedETAGSourceURL    string
	PredefinedETAGSourceFileSz int64
	PredefinedETAGMaxParallel  int
	PredefinedETAGMinSubmitSec float64
	PrewarmEnabled             bool
	PrewarmTimeoutSec          float64
}

func envBool(name string, defaultVal bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultVal
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func envInt(name string, defaultVal int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return defaultVal
	}
	return v
}

func envInt64(name string, defaultVal int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

func envFloat(name string, defaultVal float64) float64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultVal
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

// LoadFromEnv reads embedded-worker settings from the process environment.
func LoadFromEnv() Settings {
	maxTasks := max(1, envInt("WORKER_MAX_CONCURRENT_TASKS", 4))
	maxInFlight := envInt("WORKER_MAX_IN_FLIGHT_BYTES", 256*1024*1024)
	if maxInFlight < DefaultChunkSizeBytes {
		maxInFlight = DefaultChunkSizeBytes
	}

	defaultSource := "https://ef88e61230a7f9cdaa979b6268878856.r2.cloudflarestorage.com" +
		"/beam-xfer-test/source/b1m_test/bin10GB.bin"

	return Settings{
		CoreServerURL:              strings.TrimSpace(os.Getenv("CORE_SERVER_URL")),
		WalletPath:                 strings.TrimSpace(os.Getenv("WALLET_PATH")),
		MaxConcurrentTasks:         maxTasks,
		MaxInFlightBytes:           maxInFlight,
		AcceptAckTimeoutSec:        envFloat("WORKER_TASK_ACCEPT_ACK_TIMEOUT", 8.0),
		EarlySubmit:                envBool("WORKER_PREDEFINED_ETAG_EARLY_SUBMIT", true),
		PredefinedETAGSourceURL:    strings.TrimRight(strings.TrimSpace(envString("WORKER_PREDEFINED_ETAG_SOURCE_URL", defaultSource)), "/"),
		PredefinedETAGSourceFileSz: envInt64("WORKER_PREDEFINED_ETAG_SOURCE_FILE_SIZE", DefaultPredefinedETAGSourceFile),
		PredefinedETAGMaxParallel:  max(1, envInt("WORKER_PREDEFINED_ETAG_MAX_PARALLEL", 1)),
		PredefinedETAGMinSubmitSec: envFloat("WORKER_PREDEFINED_ETAG_MIN_SUBMIT_SEC", 0),
		PrewarmEnabled:             envBool("WORKER_PREWARM_ENABLED", true),
		PrewarmTimeoutSec:          envFloat("WORKER_PREWARM_TIMEOUT", 5),
	}
}

func envString(name, defaultVal string) string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return defaultVal
	}
	return raw
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
