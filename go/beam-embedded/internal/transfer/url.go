package transfer

import (
	"net/url"
	"strings"
)

// RedactURL drops query parameters from capability URLs.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		if i := strings.Index(raw, "?"); i >= 0 {
			return raw[:i]
		}
		return raw
	}
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/")
}

// NormalizedCapabilityURL normalizes signed URLs for prefix comparison.
func NormalizedCapabilityURL(raw string) string {
	return strings.TrimRight(strings.TrimSpace(RedactURL(raw)), "/")
}

// IsObjectStoragePresignedURL checks for S3/GCS/R2 presigned upload URLs.
func IsObjectStoragePresignedURL(raw string) bool {
	if raw == "" {
		return false
	}
	return strings.Contains(raw, "X-Amz-Signature") ||
		strings.Contains(raw, "X-Goog-Signature") ||
		strings.Contains(raw, "r2.cloudflarestorage.com") ||
		strings.Contains(raw, "storage.googleapis.com")
}

// IsCanaryDestination returns true for canary/null/skip destinations.
func IsCanaryDestination(raw string) bool {
	return strings.HasPrefix(raw, "null://") ||
		strings.HasPrefix(raw, "canary://") ||
		strings.HasPrefix(raw, "skip://")
}

// URLOrigin returns scheme://host for connection prewarm.
func URLOrigin(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" {
		return ""
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if u.Port() != "" && u.Port() != "80" && u.Port() != "443" {
		return u.Scheme + "://" + host + ":" + u.Port()
	}
	return u.Scheme + "://" + host
}
