package circuitbreaker

import (
	"sync"

	"github.com/songquanpeng/one-api/model"
)

// Manager manages circuit breakers for multiple channels
type Manager struct {
	breakers map[int]*CircuitBreaker
	config   *Config
	mu       sync.RWMutex
}

// NewManager creates a new circuit breaker manager
func NewManager(config *Config) *Manager {
	if config == nil {
		config = DefaultConfig()
	}
	return &Manager{
		breakers: make(map[int]*CircuitBreaker),
		config:   config,
	}
}

// GetBreaker returns a circuit breaker for the given channel ID
func (m *Manager) GetBreaker(channelId int) *CircuitBreaker {
	m.mu.RLock()
	if cb, exists := m.breakers[channelId]; exists {
		m.mu.RUnlock()
		return cb
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check after acquiring write lock
	if cb, exists := m.breakers[channelId]; exists {
		return cb
	}

	cb := NewCircuitBreaker(channelId, m.config)
	m.breakers[channelId] = cb
	return cb
}

// GetAvailableChannels filters out channels that are in OPEN state
func (m *Manager) GetAvailableChannels(channels []*model.Channel) ([]*model.Channel, error) {
	available := make([]*model.Channel, 0, len(channels))
	for _, channel := range channels {
		cb := m.GetBreaker(channel.Id)
		allowed, err := cb.AllowRequest()
		if err != nil {
			return nil, err
		}
		if allowed {
			available = append(available, channel)
		}
	}
	return available, nil
}

// RecordSuccess records a successful request for the given channel
func (m *Manager) RecordSuccess(channelId int, duration int64) error {
	cb := m.GetBreaker(channelId)
	return cb.RecordSuccess(duration)
}

// RecordFailure records a failed request for the given channel
func (m *Manager) RecordFailure(channelId int) error {
	cb := m.GetBreaker(channelId)
	return cb.RecordFailure()
}

var (
	circuitBreakerManager *Manager
)

func init() {
	circuitBreakerManager = NewManager(nil)
}

func GetManager() *Manager {
	return circuitBreakerManager
}
