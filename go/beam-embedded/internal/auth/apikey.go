package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/beam/sn105/beam-embedded/internal/config"
	"github.com/beam/sn105/beam-embedded/internal/wallet"
)

type cacheFile struct {
	Key     string  `json:"key"`
	Expires float64 `json:"expires"`
}

// EnsureAPIKey returns a BeamCore API key using the same flow as Python SubnetCoreClient.
// Order: in-memory/env BEAMCORE_API_KEY → disk cache → POST /auth/challenge + /auth/verify.
func EnsureAPIKey(
	ctx context.Context,
	cfg config.OrchestratorSettings,
	hotkey string,
	signer *wallet.Signer,
	logf func(string, ...any),
) (string, error) {
	if key := envAPIKey(); key != "" {
		if logf != nil {
			logf("Using BEAMCORE_API_KEY from environment for %s...", hotkeyPrefix(hotkey))
		}
		return key, nil
	}
	if key, ok := loadCachedKey(hotkey); ok {
		if logf != nil {
			logf("Loaded cached API key from disk for %s...", hotkeyPrefix(hotkey))
		}
		return key, nil
	}
	if signer == nil {
		return "", fmt.Errorf("no wallet signer and no BEAMCORE_API_KEY; set WALLET_NAME/WALLET_HOTKEY or BEAMCORE_API_KEY")
	}
	if cfg.CoreServerURL == "" {
		return "", fmt.Errorf("CORE_SERVER_URL is required for auth/challenge")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	base := strings.TrimRight(cfg.CoreServerURL, "/")

	challengeBody, _ := json.Marshal(map[string]string{
		"hotkey": hotkey,
		"role":   "orchestrator",
	})
	chReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/auth/challenge", bytes.NewReader(challengeBody))
	if err != nil {
		return "", err
	}
	chReq.Header.Set("Content-Type", "application/json")
	chResp, err := client.Do(chReq)
	if err != nil {
		return "", fmt.Errorf("auth/challenge: %w", err)
	}
	defer chResp.Body.Close()
	chRaw, _ := io.ReadAll(chResp.Body)
	if chResp.StatusCode == 429 {
		retry := chResp.Header.Get("Retry-After")
		return "", fmt.Errorf("auth/challenge rate limited; retry after %s seconds", retry)
	}
	if chResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth/challenge HTTP %d: %s", chResp.StatusCode, string(chRaw))
	}

	var challenge struct {
		ChallengeID string `json:"challenge_id"`
		Message     string `json:"message"`
	}
	if err := json.Unmarshal(chRaw, &challenge); err != nil {
		return "", fmt.Errorf("auth/challenge decode: %w", err)
	}
	if challenge.ChallengeID == "" || challenge.Message == "" {
		return "", fmt.Errorf("auth/challenge missing fields: %s", string(chRaw))
	}

	signature, err := signer.SignHex(challenge.Message)
	if err != nil {
		return "", fmt.Errorf("sign challenge: %w", err)
	}

	verifyBody, _ := json.Marshal(map[string]string{
		"challenge_id": challenge.ChallengeID,
		"hotkey":       hotkey,
		"signature":    signature,
		"role":         "orchestrator",
		"key_name":     "Orchestrator WebSocket Key",
	})
	vReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/auth/verify", bytes.NewReader(verifyBody))
	if err != nil {
		return "", err
	}
	vReq.Header.Set("Content-Type", "application/json")
	vResp, err := client.Do(vReq)
	if err != nil {
		return "", fmt.Errorf("auth/verify: %w", err)
	}
	defer vResp.Body.Close()
	vRaw, _ := io.ReadAll(vResp.Body)
	if vResp.StatusCode == 409 {
		return "", fmt.Errorf(
			"API key already exists for this orchestrator; set BEAMCORE_API_KEY with your existing key or revoke the old key",
		)
	}
	if vResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("auth/verify HTTP %d: %s", vResp.StatusCode, string(vRaw))
	}

	var verify struct {
		Success bool   `json:"success"`
		APIKey  string `json:"api_key"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(vRaw, &verify); err != nil {
		return "", fmt.Errorf("auth/verify decode: %w", err)
	}
	if !verify.Success || verify.APIKey == "" {
		return "", fmt.Errorf("auth/verify failed: %s", verify.Message)
	}

	expires := float64(time.Now().Add(24 * time.Hour).Unix())
	_ = persistKey(hotkey, verify.APIKey, expires)
	if logf != nil {
		logf("Obtained API key via auth/challenge+verify for %s...", hotkeyPrefix(hotkey))
	}
	return verify.APIKey, nil
}

func envAPIKey() string {
	key := strings.TrimSpace(os.Getenv("BEAMCORE_API_KEY"))
	if key == "" {
		return ""
	}
	if strings.HasPrefix(key, "b1m_") || strings.HasPrefix(key, "bck_") {
		return key
	}
	return ""
}

func cachePath(hotkey string) string {
	return fmt.Sprintf("/tmp/beam_orch_api_key_%s.json", hotkeyPrefix(hotkey))
}

func hotkeyPrefix(hotkey string) string {
	if len(hotkey) > 16 {
		return hotkey[:16]
	}
	return hotkey
}

func loadCachedKey(hotkey string) (string, bool) {
	raw, err := os.ReadFile(cachePath(hotkey))
	if err != nil {
		return "", false
	}
	var data cacheFile
	if json.Unmarshal(raw, &data) != nil || data.Key == "" {
		return "", false
	}
	if data.Expires > 0 && time.Now().Unix() >= int64(data.Expires)-60 {
		return "", false
	}
	return data.Key, true
}

func persistKey(hotkey, key string, expires float64) error {
	raw, err := json.Marshal(cacheFile{Key: key, Expires: expires})
	if err != nil {
		return err
	}
	return os.WriteFile(cachePath(hotkey), raw, 0o600)
}
