package transfer

import (
	"fmt"
	"strconv"

	"github.com/beam/sn105/beam-embedded/internal/config"
)

// UsesPredefinedETag returns true for 30 MiB staging chunks.
func UsesPredefinedETag(chunkSize int) bool {
	return chunkSize == config.PredefinedETagChunkSizeBytes
}

// UsesPredefinedETagTransfer returns true for fixed-size staging uploads.
func UsesPredefinedETagTransfer(ctx *Context) bool {
	return UsesPredefinedETag(ctx.ChunkSize) &&
		IsObjectStoragePresignedURL(ctx.DestURL) &&
		!IsCanaryDestination(ctx.DestURL)
}

// MatchesPredefinedETAGSource returns true when source URL path matches prefix.
func MatchesPredefinedETAGSource(sourceURL, prefix string) bool {
	if prefix == "" {
		return false
	}
	got := NormalizedCapabilityURL(sourceURL)
	want := NormalizedCapabilityURL(prefix)
	if got == "" || want == "" {
		return false
	}
	return got == want || stringsHasPrefixPath(got, want)
}

func stringsHasPrefixPath(got, want string) bool {
	return len(got) > len(want) && got[:len(want)] == want && got[len(want)] == '/'
}

// MatchesPredefinedETAGFileSize returns true when offer range lies within file size.
func MatchesPredefinedETAGFileSize(ctx *Context, fileSize int64) bool {
	if fileSize <= 0 {
		return false
	}
	if ctx.RangeStart < 0 || ctx.RangeEnd < ctx.RangeStart {
		return false
	}
	return ctx.RangeEnd <= fileSize-1
}

// UsesPredefinedETAGEarlySubmit returns true when fast path may run.
func UsesPredefinedETAGEarlySubmit(ctx *Context, cfg config.Settings) bool {
	return cfg.EarlySubmit &&
		MatchesPredefinedETAGSource(ctx.SourceURL, cfg.PredefinedETAGSourceURL) &&
		MatchesPredefinedETAGFileSize(ctx, cfg.PredefinedETAGSourceFileSz) &&
		UsesPredefinedETagTransfer(ctx)
}

// ShouldBufferPredefinedETAGFetch returns true when download should buffer for early submit.
func ShouldBufferPredefinedETAGFetch(
	fetchReady *FetchReady,
	sourceURL string,
	chunkSize int,
	isObjectStorage, isCanary bool,
	cfg config.Settings,
) bool {
	if fetchReady == nil || isCanary || !isObjectStorage || !cfg.EarlySubmit {
		return false
	}
	if !UsesPredefinedETag(chunkSize) {
		return false
	}
	return MatchesPredefinedETAGSource(sourceURL, cfg.PredefinedETAGSourceURL)
}

// PredefinedETAGEarlySubmitSkipReasons explains why fast path is not used.
func PredefinedETAGEarlySubmitSkipReasons(ctx *Context, cfg config.Settings) []string {
	if !cfg.EarlySubmit {
		return nil
	}
	if UsesPredefinedETAGEarlySubmit(ctx, cfg) {
		return nil
	}

	var reasons []string
	if !UsesPredefinedETag(ctx.ChunkSize) {
		reasons = append(reasons, fmt.Sprintf(
			"chunk_size=%d expected=%d",
			ctx.ChunkSize, config.PredefinedETagChunkSizeBytes,
		))
	}
	if IsCanaryDestination(ctx.DestURL) {
		reasons = append(reasons, "canary_destination")
	} else if !IsObjectStoragePresignedURL(ctx.DestURL) {
		reasons = append(reasons, "dest_not_presigned_object_storage")
	}
	if !MatchesPredefinedETAGSource(ctx.SourceURL, cfg.PredefinedETAGSourceURL) {
		reasons = append(reasons, fmt.Sprintf(
			"source_url_prefix_mismatch got=%q expected_prefix=%q",
			NormalizedCapabilityURL(ctx.SourceURL),
			NormalizedCapabilityURL(cfg.PredefinedETAGSourceURL),
		))
	}
	if !MatchesPredefinedETAGFileSize(ctx, cfg.PredefinedETAGSourceFileSz) {
		reasons = append(reasons, fmt.Sprintf(
			"file_size_mismatch range=%d-%d max_end=%d",
			ctx.RangeStart, ctx.RangeEnd, cfg.PredefinedETAGSourceFileSz-1,
		))
	}
	return reasons
}

// ValidateFetchReadyBytes returns error when buffered bytes are not exactly 30 MiB.
func ValidateFetchReadyBytes(fetchReady *FetchReady) string {
	if fetchReady.BytesTransferred == config.PredefinedETagChunkSizeBytes {
		return ""
	}
	return fmt.Sprintf(
		"bytes_mismatch: got %d expected %d",
		fetchReady.BytesTransferred,
		config.PredefinedETagChunkSizeBytes,
	)
}

// BuildFetchHeaders builds Range headers for source GET.
func BuildFetchHeaders(chunkOffset int64, chunkSize int, totalSize *int64) map[string]string {
	headers := map[string]string{"ngrok-skip-browser-warning": "true"}
	if chunkSize <= 0 {
		return headers
	}
	var rangeEnd int64
	if totalSize != nil {
		rangeEnd = min64(chunkOffset+int64(chunkSize)-1, *totalSize-1)
	} else {
		rangeEnd = chunkOffset + int64(chunkSize) - 1
	}
	headers["Range"] = fmt.Sprintf("bytes=%d-%d", chunkOffset, rangeEnd)
	return headers
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ChunkHashFromOffer reads expected chunk hash from offer payload.
func ChunkHashFromOffer(task map[string]any) string {
	if m, ok := task["chunk_hashes"].(map[string]any); ok {
		if v, ok := m["0"].(string); ok {
			return v
		}
	}
	if v, ok := task["chunk_hash"].(string); ok {
		return v
	}
	return ""
}

// OfferID returns offer_id or task_id from offer map.
func OfferID(task map[string]any) string {
	if s, ok := task["offer_id"].(string); ok && s != "" {
		return s
	}
	if s, ok := task["task_id"].(string); ok {
		return s
	}
	return ""
}

// TaskID returns task_id or offer_id from offer map.
func TaskID(task map[string]any) string {
	if s, ok := task["task_id"].(string); ok && s != "" {
		return s
	}
	return OfferID(task)
}

// DeadlineUS reads deadline_us from offer.
func DeadlineUS(task map[string]any) int64 {
	switch v := task["deadline_us"].(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	default:
		return 0
	}
}
