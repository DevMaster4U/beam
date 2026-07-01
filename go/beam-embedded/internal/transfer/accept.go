package transfer

import (
	"context"
	"fmt"
	"time"
)

// AcceptFunc sends task_accept and returns whether BeamCore accepted the task.
type AcceptFunc func(ctx context.Context) (bool, error)

// WaitAcceptAndBufferedFetch waits for accept ack and buffered download+hash.
func WaitAcceptAndBufferedFetch(
	ctx context.Context,
	accept AcceptFunc,
	fetchReady *FetchReady,
	acceptTimeout, fetchTimeout time.Duration,
) (accepted bool, waitError string) {
	acceptCtx, acceptCancel := context.WithTimeout(ctx, acceptTimeout)
	defer acceptCancel()

	fetchCtx, fetchCancel := context.WithTimeout(ctx, fetchTimeout)
	defer fetchCancel()

	type acceptResult struct {
		ok  bool
		err error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		ok, err := accept(acceptCtx)
		acceptCh <- acceptResult{ok: ok, err: err}
	}()

	fetchCh := make(chan error, 1)
	go func() {
		select {
		case <-fetchReady.Done():
			fetchCh <- nil
		case <-fetchCtx.Done():
			fetchCh <- fetchCtx.Err()
		}
	}()

	acceptDone := false
	fetchDone := false
	var acceptRes acceptResult
	var fetchErr error

	for !acceptDone || !fetchDone {
		select {
		case <-ctx.Done():
			return false, ctx.Err().Error()
		case acceptRes = <-acceptCh:
			acceptDone = true
			if acceptRes.err != nil {
				return false, acceptRes.err.Error()
			}
			if !acceptRes.ok {
				return false, "task_accept rejected"
			}
		case fetchErr = <-fetchCh:
			fetchDone = true
			if fetchErr != nil {
				return false, fmt.Sprintf("download timeout (%.0fs)", fetchTimeout.Seconds())
			}
		}
	}

	if fetchReady.Error != "" || !fetchReady.Ready {
		if fetchReady.Error != "" {
			return true, fetchReady.Error
		}
		return true, "download failed"
	}
	return true, ""
}
