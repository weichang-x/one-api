package circuitbreaker

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/config"
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
		FailureThreshold:   int32(config.CircuitBreakerFailureThreshold),
		ErrorRateThreshold: config.CircuitBreakerErrorRateThreshold,
		SlowCallDuration:   config.CircuitBreakerSlowCallDuration,
		CooldownPeriod:     config.CircuitBreakerCooldownPeriod,
		HalfOpenMaxCalls:   int32(config.CircuitBreakerHalfOpenMaxCalls),
	}
}

// CircuitBreaker represents the circuit breaker for an API key
type CircuitBreaker struct {
	channelId     int
	state         State
	config        *Config
	failureCount  int32
	lastFailure   int64
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
// duration 参数代表 API 调用的耗时，用于统计慢调用的次数
func (cb *CircuitBreaker) RecordSuccess(duration int64) error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	state, err := cb.GetState()
	if err != nil {
		return err
	}
	// logger.SysLog(fmt.Sprintf("CircuitBreaker RecordSuccess channel #%d state: %s", cb.channelId, state))

	// Reset failure count on success
	atomic.StoreInt32(&cb.failureCount, 0)

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
	return cb.updateMetrics(true, duration)
}

// RecordFailure records a failed call
func (cb *CircuitBreaker) RecordFailure() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt32(&cb.failureCount, 1)
	atomic.StoreInt64(&cb.lastFailure, time.Now().Unix())
	// logger.SysLog(fmt.Sprintf("CircuitBreaker RecordFailure channel #%d failureCount: %d", cb.channelId, cb.failureCount))

	// Update metrics
	metricsErr := cb.updateMetrics(false, 0)

	// Get metrics for error rate check
	metrics, err := cb.getMetrics()
	if err != nil {
		metrics = &CircuitBreakerMetrics{} // Use empty metrics if can't get from Redis
	}

	// Check if we should open the circuit
	if cb.shouldOpen(metrics) {
		if err := cb.setState(StateOpen); err != nil {
			logger.SysError(fmt.Sprintf("Failed to set circuit breaker state to OPEN: %v", err))
		}
		return fmt.Errorf("circuit breaker opened after %d consecutive failures", cb.failureCount)
	}

	return metricsErr // Return metrics error if any
}

// shouldOpen determines if the circuit should be opened based on metrics
func (cb *CircuitBreaker) shouldOpen(metrics *CircuitBreakerMetrics) bool {
	// Check consecutive failures
	if atomic.LoadInt32(&cb.failureCount) >= cb.config.FailureThreshold {
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
		if time.Now().Unix()-atomic.LoadInt64(&cb.lastFailure) > int64(cb.config.CooldownPeriod) {
			if err := cb.setState(StateHalfOpen); err != nil {
				return false, err
			}
			atomic.StoreInt32(&cb.halfOpenCalls, 0)
			return true, nil
		}
		return false, nil
	case StateHalfOpen:
		return atomic.LoadInt32(&cb.halfOpenCalls) < cb.config.HalfOpenMaxCalls, nil
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
		atomic.StoreInt64(&metrics.LastFailure, time.Now().UnixMilli())
		failures := atomic.LoadInt32(&metrics.FailureCalls)
		total := atomic.LoadInt32(&metrics.TotalCalls)
		if total > 0 {
			metrics.ErrorRate = float64(failures) / float64(total)
		}
	}
	// 慢调用
	if duration > int64(cb.config.SlowCallDuration) {
		atomic.AddInt32(&metrics.SlowCalls, 1)
	}

	data, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	// logger.SysLog(fmt.Sprintf("CircuitBreaker updateMetrics channel #%d metrics: %v", cb.channelId, metrics))
	// Store metrics with 1 hour expiration
	return common.RedisSetWithExpiration(key, string(data), time.Hour)
}
