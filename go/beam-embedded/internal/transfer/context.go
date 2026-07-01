package transfer

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/beam/sn105/beam-embedded/internal/config"
)

var rangeHeaderRE = regexp.MustCompile(`^bytes=(\d+)-(\d+)$`)

// Context is the normalized transfer offer used by embedded workers.
type Context struct {
	SourceURL            string
	DestURL              string
	ChunkSize            int
	RangeStart           int64
	RangeEnd             int64
	TotalSize            *int64
	SourceHeaders        map[string]string
	DestHeaders          map[string]string
	SignedURLFlow        string
	MinimumWorkerVersion string
	TransferID           string
	ETagRequired         bool
}

// OfferHeaders returns string-only offer headers.
func OfferHeaders(value any) map[string]string {
	m, ok := value.(map[string]any)
	if !ok {
		if sm, ok := value.(map[string]string); ok {
			return sm
		}
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// ParseOfferRange parses the signed source Range header as start, end, length.
func ParseOfferRange(headers map[string]string) (start, end, size int64, err error) {
	rangeHeader := headers["Range"]
	if rangeHeader == "" {
		rangeHeader = headers["range"]
	}
	if rangeHeader == "" {
		return 0, 0, 0, nil
	}
	match := rangeHeaderRE.FindStringSubmatch(strings.TrimSpace(rangeHeader))
	if match == nil {
		return 0, 0, 0, errInvalidRange{header: rangeHeader}
	}
	start, _ = strconv.ParseInt(match[1], 10, 64)
	end, _ = strconv.ParseInt(match[2], 10, 64)
	if end < start {
		return 0, 0, 0, errInvalidRange{header: rangeHeader}
	}
	return start, end, end - start + 1, nil
}

type errInvalidRange struct{ header string }

func (e errInvalidRange) Error() string {
	return "invalid source Range header: " + e.header
}

// BuildContext validates and normalizes a flat worker task offer.
func BuildContext(task map[string]any) (*Context, string) {
	sourceURL, _ := task["source_url"].(string)
	destURL, _ := task["dest_url"].(string)
	if strings.TrimSpace(sourceURL) == "" {
		return nil, "missing_source_url"
	}
	if strings.TrimSpace(destURL) == "" {
		return nil, "missing_dest_url"
	}

	chunkSize, err := intFromAny(task["chunk_size"])
	if err != nil || chunkSize <= 0 {
		return nil, "invalid_chunk_size"
	}

	sourceHeaders := OfferHeaders(task["source_headers"])
	destHeaders := OfferHeaders(task["dest_headers"])

	minVersion, _ := task["minimum_worker_version"].(string)
	minVersion = strings.TrimSpace(minVersion)
	if minVersion != "" && !workerVersionSatisfies(minVersion) {
		return nil, "unsupported_worker_version"
	}

	signedURLFlow, _ := task["signed_url_flow"].(string)
	signedURLFlow = strings.TrimSpace(signedURLFlow)
	if signedURLFlow == "signed_url_v1" && IsObjectStoragePresignedURL(destURL) {
		if destHeaders["Content-MD5"] == "" && destHeaders["content-md5"] == "" {
			return nil, "missing_content_md5"
		}
	}

	start, end, rangeSize, err := ParseOfferRange(sourceHeaders)
	if err != nil {
		return nil, err.Error()
	}
	if rangeSize == 0 {
		return nil, "missing_source_range"
	}
	if int64(chunkSize) != rangeSize {
		return nil, "range_size_mismatch:" + strconv.FormatInt(rangeSize, 10) + "!=" + strconv.Itoa(chunkSize)
	}

	var totalSize *int64
	for _, key := range []string{"total_size", "total_bytes", "file_size"} {
		if raw := task[key]; raw != nil {
			if v, err := int64FromAny(raw); err == nil {
				totalSize = &v
				break
			}
		}
	}

	transferID := ""
	for _, key := range []string{"transfer_id", "task_id"} {
		if s, ok := task[key].(string); ok && s != "" {
			transferID = s
			break
		}
	}

	etagRequired, _ := task["etag_required"].(bool)

	return &Context{
		SourceURL:            strings.TrimSpace(sourceURL),
		DestURL:              strings.TrimSpace(destURL),
		ChunkSize:            chunkSize,
		RangeStart:           start,
		RangeEnd:             end,
		TotalSize:            totalSize,
		SourceHeaders:        sourceHeaders,
		DestHeaders:          destHeaders,
		SignedURLFlow:        signedURLFlow,
		MinimumWorkerVersion: minVersion,
		TransferID:           transferID,
		ETagRequired:         etagRequired,
	}, ""
}

func intFromAny(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int64:
		return int(n), nil
	case float64:
		return int(n), nil
	case string:
		return strconv.Atoi(strings.TrimSpace(n))
	default:
		return 0, strconv.ErrSyntax
	}
}

func int64FromAny(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case float64:
		return int64(n), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(n), 10, 64)
	default:
		return 0, strconv.ErrSyntax
	}
}

func workerVersionSatisfies(minimum string) bool {
	// Go worker reports config.WorkerVersion; accept any minimum for now.
	return minimum == "" || config.WorkerVersion != ""
}

// EstimateTaskBytes estimates in-memory bytes for capacity checks.
func EstimateTaskBytes(task map[string]any) int {
	chunkSize, err := intFromAny(task["chunk_size"])
	if err != nil || chunkSize <= 0 {
		return config.DefaultChunkSizeBytes
	}
	return chunkSize
}
