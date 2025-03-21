package middleware

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/model"
)

// QueuedRequest represents a request waiting in the queue
type QueuedRequest struct {
	Context    context.Context
	ResultChan chan *QueueResult
}

// QueueResult represents the result of a queued request
type QueueResult struct {
	Channel *model.Channel
	Error   error
}

// RequestQueue manages queued requests
type RequestQueue struct {
	queue     []QueuedRequest
	mu        sync.Mutex
	timeout   time.Duration
	maxLength int
}

var (
	defaultQueue *RequestQueue
	once         sync.Once
)

// GetDefaultQueue returns the singleton request queue instance
func GetDefaultQueue() *RequestQueue {
	once.Do(func() {
		defaultQueue = NewRequestQueue(
			time.Duration(config.RequestQueueTimeout)*time.Second,
			config.RequestQueueMaxLength,
		)
	})
	return defaultQueue
}

// NewRequestQueue creates a new request queue
func NewRequestQueue(timeout time.Duration, maxLength int) *RequestQueue {
	return &RequestQueue{
		queue:     make([]QueuedRequest, 0),
		timeout:   timeout,
		maxLength: maxLength,
	}
}

// EnqueueRequest adds a request to the queue and waits for a result
func (q *RequestQueue) EnqueueRequest(ctx context.Context) (*model.Channel, error) {
	q.mu.Lock()
	if len(q.queue) >= q.maxLength {
		q.mu.Unlock()
		return nil, errors.New("queue is full")
	}

	resultChan := make(chan *QueueResult, 1)
	request := QueuedRequest{
		Context:    ctx,
		ResultChan: resultChan,
	}
	q.queue = append(q.queue, request)
	q.mu.Unlock()

	// Wait for result with timeout
	select {
	case result := <-resultChan:
		return result.Channel, result.Error
	case <-time.After(q.timeout):
		// Remove request from queue on timeout
		q.removeRequest(request)
		return nil, errors.New("request timeout while waiting in queue")
	case <-ctx.Done():
		// Remove request from queue if context is cancelled
		q.removeRequest(request)
		return nil, ctx.Err()
	}
}

// ProcessNextRequest processes the next request in the queue
func (q *RequestQueue) ProcessNextRequest(channel *model.Channel, err error) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) == 0 {
		return false
	}

	// Get next request
	request := q.queue[0]
	q.queue = q.queue[1:]

	// Send result to waiting goroutine
	select {
	case request.ResultChan <- &QueueResult{Channel: channel, Error: err}:
	default:
		// Request might have timed out
	}

	return true
}

// removeRequest removes a specific request from the queue
func (q *RequestQueue) removeRequest(request QueuedRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, req := range q.queue {
		if req.ResultChan == request.ResultChan {
			q.queue = append(q.queue[:i], q.queue[i+1:]...)
			break
		}
	}
}

// QueueLength returns the current length of the queue
func (q *RequestQueue) QueueLength() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}
