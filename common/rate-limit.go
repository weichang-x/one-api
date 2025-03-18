package common

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type InMemoryRateLimiter struct {
	store              map[string]*[]int64
	mutex              sync.Mutex
	expirationDuration time.Duration
}

func (l *InMemoryRateLimiter) Init(expirationDuration time.Duration) {
	if l.store == nil {
		l.mutex.Lock()
		if l.store == nil {
			l.store = make(map[string]*[]int64)
			l.expirationDuration = expirationDuration
			if expirationDuration > 0 {
				go l.clearExpiredItems()
			}
		}
		l.mutex.Unlock()
	}
}

func (l *InMemoryRateLimiter) clearExpiredItems() {
	for {
		time.Sleep(l.expirationDuration)
		l.mutex.Lock()
		now := time.Now().Unix()
		for key := range l.store {
			queue := l.store[key]
			size := len(*queue)
			if size == 0 || now-(*queue)[size-1] > int64(l.expirationDuration.Seconds()) {
				delete(l.store, key)
			}
		}
		l.mutex.Unlock()
	}
}

// Request parameter duration's unit is seconds
func (l *InMemoryRateLimiter) Request(key string, maxRequestNum int, duration int64) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	// [old <-- new]
	queue, ok := l.store[key]
	now := time.Now().Unix()
	if ok {
		if len(*queue) < maxRequestNum {
			*queue = append(*queue, now)
			return true
		} else {
			if now-(*queue)[0] >= duration {
				*queue = (*queue)[1:]
				*queue = append(*queue, now)
				return true
			} else {
				return false
			}
		}
	} else {
		s := make([]int64, 0, maxRequestNum)
		l.store[key] = &s
		*(l.store[key]) = append(*(l.store[key]), now)
	}
	return true
}

// 使用Redis Lua脚本实现原子操作
const (

	// updateQuotaScript atomically updates quota with CAS
	updateQuotaScript = `
		local key = KEYS[1]
		local newData = ARGV[1]
		local expectedVersion = ARGV[2]
		
		local currentData = redis.call('GET', key)
		if not currentData then
			redis.call('SETEX', key, 60, newData)
			return {ok = newData}
		end

		-- Decode both current and new data
		local current = cjson.decode(currentData)
		local new = cjson.decode(newData)
		
		-- Merge the quotas by taking the minimum values
		current.remaining_tpm = math.min(current.remaining_tpm or 0, new.remaining_tpm or 0)
		current.remaining_rpm = math.min(current.remaining_rpm or 0, new.remaining_rpm or 0)
		current.remaining_otpm = math.min(current.remaining_otpm or 0, new.remaining_otpm or 0)
		current.remaining_itpm = math.min(current.remaining_itpm or 0, new.remaining_itpm or 0)
		
		-- Update version
		current.version = new.version
		
		-- Encode and store the merged result
		local mergedData = cjson.encode(current)
		redis.call('SETEX', key, 60, mergedData)
		return {ok = mergedData}
	`
)

// 为ChannelQuota添加版本号以支持CAS
type ChannelQuota struct {
	RemainingTPM  int64  `json:"remaining_tpm"`  // 剩余每分钟令牌数
	RemainingRPM  int64  `json:"remaining_rpm"`  // 剩余每分钟请求数
	RemainingOTPM int64  `json:"remaining_otpm"` // 剩余每分钟输出令牌数
	RemainingITPM int64  `json:"remaining_itpm"` // 剩余每分钟输入令牌数
	Version       string `json:"version"`        // 版本号，用于CAS操作
}

// Redis key 格式设计
const (
	ChannelQuotaKeyPrefix = "channel:quota:"
	QuotaExpiration       = 60 * time.Second // 1分钟过期
)

func GetChannelQuotaKey(channelId int) string {
	return fmt.Sprintf("%s%d", ChannelQuotaKeyPrefix, channelId)
}

// 使用CAS机制原子性更新配额
func AtomicUpdateChannelQuota(channelId int, quota *ChannelQuota) error {
	key := GetChannelQuotaKey(channelId)
	ctx := context.Background()

	// 生成新的版本号
	quota.Version = uuid.New().String()

	data, err := json.Marshal(quota)
	if err != nil {
		return fmt.Errorf("failed to marshal quota: %v", err)
	}

	expectedVersion := ""
	if quota != nil {
		expectedVersion = quota.Version
	}

	result, err := RDB.Eval(ctx, updateQuotaScript, []string{key}, string(data), expectedVersion).Result()
	if err != nil {
		return fmt.Errorf("failed to execute update quota script: %v", err)
	}

	// 检查结果是否为 nil
	if result == nil {
		return fmt.Errorf("script returned nil result")
	}

	// 将字符串结果解析为JSON
	resultStr, ok := result.(string)
	if !ok {
		return fmt.Errorf("invalid script result type: %T (expected string)", result)
	}

	var resultQuota ChannelQuota
	if err := json.Unmarshal([]byte(resultStr), &resultQuota); err != nil {
		return fmt.Errorf("failed to parse script result: %v", err)
	}

	// 更新原始quota对象的值
	*quota = resultQuota

	return nil
}

// 使用Redis原子操作更新渠道配额信息
func UpdateChannelQuota(channelId int, quota *ChannelQuota) error {
	return AtomicUpdateChannelQuota(channelId, quota)
}

// 获取渠道配额信息
func GetChannelQuota(channelId int) (*ChannelQuota, error) {
	key := GetChannelQuotaKey(channelId)
	ctx := context.Background()

	result, err := RDB.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	quota := &ChannelQuota{}
	err = json.Unmarshal([]byte(result), quota)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal quota: %v", err)
	}

	return quota, nil
}

func GetChannelSelectLockKey(key string) string {
	return fmt.Sprintf("channel_lock:%s", key)
}
func GetCircuitBreakerManagerLockKey(channelId int) string {
	return fmt.Sprintf("circuit_breaker_manager_lock:%d", channelId)
}

// 使用 Redis 分布式锁执行回调函数
func NewRedisLock(key string, callback func()) {
	lock := newRedisLock(RDB, key, 10*time.Second)
	if lock.Acquire() {
		defer lock.Release()
		callback()
	}
}
