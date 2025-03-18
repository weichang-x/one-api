package circuitbreaker

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/logger"
)

// State represents the state of the circuit breaker
type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF_OPEN"
)

// Config represents the configuration for the circuit breaker
type Config struct {
	FailureThreshold   int32   `json:"failure_threshold"`    // Number of consecutive failures before opening
	ErrorRateThreshold float64 `json:"error_rate_threshold"` // Error rate threshold (0.0-1.0)
	SlowCallDuration   int     `json:"slow_call_duration"`   // Duration in ms to consider a call as slow
	CooldownPeriod     int     `json:"cooldown_period"`      // Period in seconds before transitioning from OPEN to HALF_OPEN
	HalfOpenMaxCalls   int32   `json:"half_open_max_calls"`  // Maximum number of calls allowed in HALF_OPEN state
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		FailureThreshold:   5,
		ErrorRateThreshold: 0.6,
		SlowCallDuration:   2000,
		CooldownPeriod:     30,
		HalfOpenMaxCalls:   3,
	}
}

// CircuitBreaker represents the circuit breaker for an API key
type CircuitBreaker struct {
	channelId     int
	state         State
	config        *Config
	failureCount  int32
	lastFailure   time.Time
	halfOpenCalls int32
	mu            sync.RWMutex
}

// CircuitBreakerMetrics represents the metrics for circuit breaker decisions
type CircuitBreakerMetrics struct {
	TotalCalls   int32   `json:"total_calls"`
	FailureCalls int32   `json:"failure_calls"`
	ErrorRate    float64 `json:"error_rate"`
	LastFailure  int64   `json:"last_failure"`
	SlowCalls    int32   `json:"slow_calls"`
}

const (
	circuitBreakerKeyPrefix = "circuit_breaker:"
	metricsKeyPrefix        = "circuit_breaker_metrics:"
)

// NewCircuitBreaker creates a new circuit breaker instance
func NewCircuitBreaker(channelId int, config *Config) *CircuitBreaker {
	if config == nil {
		config = DefaultConfig()
	}
	return &CircuitBreaker{
		channelId: channelId,
		state:     StateClosed,
		config:    config,
	}
}

// getRedisKey returns the Redis key for the circuit breaker state
func getRedisKey(channelId int) string {
	return fmt.Sprintf("%s%d", circuitBreakerKeyPrefix, channelId)
}

// getMetricsKey returns the Redis key for the circuit breaker metrics
func getMetricsKey(channelId int) string {
	return fmt.Sprintf("%s%d", metricsKeyPrefix, channelId)
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() (State, error) {
	key := getRedisKey(cb.channelId)
	data, err := common.RedisGet(key)
	if err != nil && err != redis.Nil {
		return StateClosed, err
	}
	if data == "" {
		if err == redis.Nil {
			cb.setState(StateClosed)
		}
		return StateClosed, nil
	}
	return State(data), nil
}

// setState sets the state of the circuit breaker
func (cb *CircuitBreaker) setState(state State) error {
	key := getRedisKey(cb.channelId)
	expiry := time.Duration(cb.config.CooldownPeriod) * time.Second
	return common.RedisSetWithExpiration(key, string(state), expiry)
}

// RecordSuccess records a successful call
func (cb *CircuitBreaker) RecordSuccess() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	state, err := cb.GetState()
	if err != nil {
		return err
	}

	switch state {
	case StateHalfOpen:
		atomic.AddInt32(&cb.halfOpenCalls, 1)
		if cb.halfOpenCalls >= cb.config.HalfOpenMaxCalls {
			return cb.setState(StateClosed)
		}
	case StateOpen:
		// Should not happen in normal flow
		logger.SysError("RecordSuccess called while circuit breaker is OPEN")
	}

	// Update metrics
	return cb.updateMetrics(true, 0)
}

// RecordFailure records a failed call
func (cb *CircuitBreaker) RecordFailure() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt32(&cb.failureCount, 1)
	cb.lastFailure = time.Now()

	// Update metrics
	if err := cb.updateMetrics(false, 0); err != nil {
		return err
	}

	metrics, err := cb.getMetrics()
	if err != nil {
		return err
	}

	// Check if we should open the circuit
	if cb.shouldOpen(metrics) {
		return cb.setState(StateOpen)
	}

	return nil
}

// shouldOpen determines if the circuit should be opened based on metrics
func (cb *CircuitBreaker) shouldOpen(metrics *CircuitBreakerMetrics) bool {
	// Check consecutive failures
	if cb.failureCount >= cb.config.FailureThreshold {
		return true
	}

	// Check error rate
	if metrics.TotalCalls > 0 && metrics.ErrorRate >= cb.config.ErrorRateThreshold {
		return true
	}

	return false
}

// AllowRequest determines if a request should be allowed through
func (cb *CircuitBreaker) AllowRequest() (bool, error) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	state, err := cb.GetState()
	if err != nil {
		return true, err // Fail open on Redis errors
	}

	switch state {
	case StateClosed:
		return true, nil
	case StateOpen:
		if time.Since(cb.lastFailure) > time.Duration(cb.config.CooldownPeriod)*time.Second {
			if err := cb.setState(StateHalfOpen); err != nil {
				return false, err
			}
			cb.halfOpenCalls = 0
			return true, nil
		}
		return false, nil
	case StateHalfOpen:
		return cb.halfOpenCalls < cb.config.HalfOpenMaxCalls, nil
	default:
		return true, nil
	}
}

// getMetrics retrieves the current metrics from Redis
func (cb *CircuitBreaker) getMetrics() (*CircuitBreakerMetrics, error) {
	key := getMetricsKey(cb.channelId)
	data, err := common.RedisGet(key)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if data == "" {
		return &CircuitBreakerMetrics{}, nil
	}

	var metrics CircuitBreakerMetrics
	err = json.Unmarshal([]byte(data), &metrics)
	return &metrics, err
}

// updateMetrics updates the metrics in Redis
func (cb *CircuitBreaker) updateMetrics(success bool, duration int64) error {
	key := getMetricsKey(cb.channelId)
	metrics, err := cb.getMetrics()
	if err != nil {
		return err
	}

	atomic.AddInt32(&metrics.TotalCalls, 1)
	if !success {
		atomic.AddInt32(&metrics.FailureCalls, 1)
	}
	if duration > int64(cb.config.SlowCallDuration) {
		atomic.AddInt32(&metrics.SlowCalls, 1)
	}
	metrics.ErrorRate = float64(metrics.FailureCalls) / float64(metrics.TotalCalls)
	metrics.LastFailure = time.Now().Unix()

	data, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	// Store metrics with 1 hour expiration
	return common.RedisSetWithExpiration(key, string(data), time.Hour)
}
