package config

import (
	"os"
	"strconv"
	"strings"
)

// OrchestratorSettings are env vars used by the Go embedded orchestrator binary.
type OrchestratorSettings struct {
	Settings

	WalletName       string
	WalletHotkey     string
	OrchestratorHotkey string
	APIPort          int
	OrchGatewayURL   string
	BeamcoreAPIKey   string
	Region           string
	MaxWorkers       int
	FeePercentage    float64
	Ready            bool
	ExternalIP       string
	WorkerGatewayURL string
	OrchestratorUID  int

	WSRequestTimeoutSec   float64
	TaskAcceptTimeoutSec  float64
	TaskResultTimeoutSec  float64
	WSOpenTimeoutSec      float64
	WSPingIntervalSec     float64
	WSPingTimeoutSec      float64
}

// LoadOrchestratorFromEnv reads orchestrator + embedded worker settings.
func LoadOrchestratorFromEnv() OrchestratorSettings {
	base := LoadFromEnv()
	ready := false
	if raw := strings.TrimSpace(os.Getenv("READY")); raw != "" {
		ready = strings.EqualFold(raw, "true") || raw == "1"
	}
	uid := 0
	if raw := strings.TrimSpace(os.Getenv("ORCHESTRATOR_UID")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil {
			uid = v
		}
	}
	return OrchestratorSettings{
		Settings:             base,
		WalletName:           envString("WALLET_NAME", "orchestrator"),
		WalletHotkey:         envString("WALLET_HOTKEY", "default"),
		OrchestratorHotkey:   strings.TrimSpace(os.Getenv("ORCHESTRATOR_HOTKEY")),
		APIPort:              envInt("API_PORT", 9000),
		OrchGatewayURL:       strings.TrimRight(strings.TrimSpace(os.Getenv("ORCH_GATEWAY_URL")), "/"),
		BeamcoreAPIKey:       strings.TrimSpace(os.Getenv("BEAMCORE_API_KEY")),
		Region:               envString("REGION", "US"),
		MaxWorkers:           envInt("MAX_WORKERS", 10000),
		FeePercentage:        envFloat("FEE_PERCENTAGE", 0),
		Ready:                ready,
		ExternalIP:           strings.TrimSpace(os.Getenv("EXTERNAL_IP")),
		WorkerGatewayURL:     strings.TrimSpace(os.Getenv("ORCHESTRATOR_WORKER_GATEWAY_URL")),
		OrchestratorUID:      uid,
		WSRequestTimeoutSec:  envFloat("ORCH_WS_REQUEST_TIMEOUT", 15),
		TaskAcceptTimeoutSec: envFloat("ORCH_TASK_ACCEPT_TIMEOUT", 8),
		TaskResultTimeoutSec: envFloat("ORCH_TASK_RESULT_TIMEOUT", 30),
		WSOpenTimeoutSec:     envFloat("ORCH_WS_OPEN_TIMEOUT", 60),
		WSPingIntervalSec:    envFloat("ORCH_WS_PING_INTERVAL", 30),
		WSPingTimeoutSec:     envFloat("ORCH_WS_PING_TIMEOUT", 45),
	}
}
