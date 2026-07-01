package logutil

import "strings"

// ShortID mirrors core.relay_log.short_id.
func ShortID(value string, maxLen ...int) string {
	n := 16
	if len(maxLen) > 0 && maxLen[0] > 0 {
		n = maxLen[0]
	}
	if len(value) <= n {
		return value
	}
	return value[:n] + "..."
}

// TaskLabel mirrors worker.task_label.
func TaskLabel(taskID string) string {
	return ShortID(taskID, 16)
}

func JoinSkipReasons(reasons []string) string {
	if len(reasons) == 0 {
		return "early_submit_disabled"
	}
	return strings.Join(reasons, "; ")
}
