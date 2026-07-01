package upstream

import "context"

// Client relays task_accept / task_reject / task_result to BeamCore over orchestrator WS.
type Client interface {
	SendTaskAccept(ctx context.Context, taskID, workerID, offerID, workerVersion string) (map[string]any, error)
	SendTaskReject(ctx context.Context, taskID, workerID, offerID, reason string) error
	SendTaskResult(ctx context.Context, payload map[string]any) (map[string]any, error)
}
