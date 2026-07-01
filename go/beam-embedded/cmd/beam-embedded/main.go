// Go embedded orchestrator: BeamCore WebSocket + in-process worker pool.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/beam/sn105/beam-embedded/internal/auth"
	"github.com/beam/sn105/beam-embedded/internal/config"
	"github.com/beam/sn105/beam-embedded/internal/embedded"
	"github.com/beam/sn105/beam-embedded/internal/gateway"
	"github.com/beam/sn105/beam-embedded/internal/wallet"
)

func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		val = strings.Trim(val, `"'`)
		if key != "" {
			_ = os.Setenv(key, val)
		}
	}
	return scanner.Err()
}

func main() {
	envFile := flag.String("env-file", "", "orchestrator env file (KEY=VALUE lines)")
	flag.Parse()

	if *envFile != "" {
		if err := loadEnvFile(*envFile); err != nil {
			fmt.Fprintf(os.Stderr, "load env file: %v\n", err)
			os.Exit(1)
		}
	}

	orchCfg := config.LoadOrchestratorFromEnv()
	if orchCfg.CoreServerURL == "" {
		fmt.Fprintln(os.Stderr, "CORE_SERVER_URL is required")
		os.Exit(1)
	}

	hotkey, err := wallet.ResolveHotkeySS58(
		orchCfg.WalletPath,
		orchCfg.WalletName,
		orchCfg.WalletHotkey,
		orchCfg.OrchestratorHotkey,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "orchestrator hotkey: %v\n", err)
		os.Exit(1)
	}
	orchCfg.OrchestratorHotkey = hotkey

	var signer *wallet.Signer
	signer, _ = wallet.LoadSigner(orchCfg.WalletPath, orchCfg.WalletName, orchCfg.WalletHotkey)

	log := embedded.StdLogger{}
	ctx := context.Background()

	apiKey, err := auth.EnsureAPIKey(ctx, orchCfg, hotkey, signer, log.Infof)
	if err != nil {
		fmt.Fprintf(os.Stderr, "BeamCore API key: %v\n", err)
		os.Exit(1)
	}
	orchCfg.BeamcoreAPIKey = apiKey

	pool := embedded.NewPool(orchCfg.Settings, nil, orchCfg.WalletPath, log)
	gw := gateway.New(orchCfg, hotkey, signer, pool, log)
	gw.SetAPIKey(apiKey)
	pool.SetUpstream(gw)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := pool.Start(runCtx); err != nil {
		fmt.Fprintf(os.Stderr, "embedded pool start failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf(
		"Go embedded orchestrator ready: hotkey=%s workers=%d early_submit=%v max_parallel=%d gateway=%s\n",
		hotkey[:min(16, len(hotkey))],
		pool.WorkerCount(),
		orchCfg.EarlySubmit,
		orchCfg.PredefinedETAGMaxParallel,
		orchCfg.OrchGatewayURL,
	)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	if err := gw.Run(runCtx); err != nil && runCtx.Err() == nil {
		fmt.Fprintf(os.Stderr, "gateway exited: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("shutting down")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
