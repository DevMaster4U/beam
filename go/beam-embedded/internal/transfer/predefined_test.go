package transfer_test

import (
	"testing"

	"github.com/beam/sn105/beam-embedded/internal/config"
	"github.com/beam/sn105/beam-embedded/internal/transfer"
)

func TestUsesPredefinedETAGEarlySubmit(t *testing.T) {
	cfg := config.Settings{
		EarlySubmit:                true,
		PredefinedETAGSourceURL:    "https://example.r2.cloudflarestorage.com/beam-xfer-test/source/b1m_test/bin10GB.bin",
		PredefinedETAGSourceFileSz: 10 * 1024 * 1024 * 1024,
	}
	ctx := &transfer.Context{
		SourceURL:  cfg.PredefinedETAGSourceURL + "?sig=abc",
		DestURL:    "https://example.r2.cloudflarestorage.com/dest?X-Amz-Signature=abc",
		ChunkSize:  config.PredefinedETagChunkSizeBytes,
		RangeStart: 0,
		RangeEnd:   config.PredefinedETagChunkSizeBytes - 1,
	}
	if !transfer.UsesPredefinedETAGEarlySubmit(ctx, cfg) {
		t.Fatal("expected fast path for 30 MiB bin10GB offer")
	}

	ctx.ChunkSize = 20 * 1024 * 1024
	reasons := transfer.PredefinedETAGEarlySubmitSkipReasons(ctx, cfg)
	if len(reasons) == 0 {
		t.Fatal("expected skip reasons for 20 MiB chunk")
	}
}

func TestBuildContextRangeMismatch(t *testing.T) {
	offer := map[string]any{
		"source_url":  "https://example.com/src",
		"dest_url":    "https://example.com/dst?X-Amz-Signature=x",
		"chunk_size":  31457280,
		"source_headers": map[string]any{
			"Range": "bytes=0-20971519",
		},
	}
	_, err := transfer.BuildContext(offer)
	if err != "range_size_mismatch:20971520!=31457280" {
		t.Fatalf("unexpected err: %q", err)
	}
}

func TestValidateFetchReadyBytes(t *testing.T) {
	ready := transfer.NewFetchReady()
	ready.SignalReady(config.PredefinedETagChunkSizeBytes, "abc", 1, config.PredefinedETag, nil)
	if msg := transfer.ValidateFetchReadyBytes(ready); msg != "" {
		t.Fatalf("unexpected validation error: %q", msg)
	}
	ready.BytesTransferred = 1
	if msg := transfer.ValidateFetchReadyBytes(ready); msg == "" {
		t.Fatal("expected bytes mismatch error")
	}
}
