package transfer

import (
	"sync"
	"time"
)

// FetchReady signals when a predefined-etag transfer has finished downloading.
type FetchReady struct {
	mu               sync.Mutex
	done             chan struct{}
	closed           bool
	Ready            bool
	Error            string
	BytesTransferred int
	ChunkHash        string
	FetchMS          float64
	ETag             string
	Buffer           []byte
}

// NewFetchReady creates a fetch-ready gate.
func NewFetchReady() *FetchReady {
	return &FetchReady{done: make(chan struct{})}
}

func (f *FetchReady) SignalReady(bytesTransferred int, chunkHash string, fetchMS float64, etag string, buffer []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.BytesTransferred = bytesTransferred
	f.ChunkHash = chunkHash
	f.FetchMS = fetchMS
	f.ETag = etag
	f.Buffer = buffer
	f.Ready = true
	if !f.closed {
		f.closed = true
		close(f.done)
	}
}

func (f *FetchReady) SignalError(err string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Error = err
	if !f.closed {
		f.closed = true
		close(f.done)
	}
}

// Wait blocks until ready or error is signaled.
func (f *FetchReady) Wait() {
	<-f.done
}

// Done returns the notification channel.
func (f *FetchReady) Done() <-chan struct{} {
	return f.done
}

// WaitPredefinedETAGMinSubmitDelay waits until offerStartedAt + minSubmitSec.
func WaitPredefinedETAGMinSubmitDelay(offerStartedAt time.Time, minSubmitSec float64) time.Duration {
	if minSubmitSec <= 0 {
		return 0
	}
	remaining := time.Duration(minSubmitSec*float64(time.Second)) - time.Since(offerStartedAt)
	if remaining > 0 {
		time.Sleep(remaining)
		return remaining
	}
	return 0
}
