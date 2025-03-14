package common

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
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

type ChannelQuota struct {
	RemainingTPM  int64 `json:"remaining_tpm"`  // 剩余每分钟令牌数
	RemainingRPM  int64 `json:"remaining_rpm"`  // 剩余每分钟请求数
	RemainingOTPM int64 `json:"remaining_otpm"` // 剩余每分钟输出令牌数
	RemainingITPM int64 `json:"remaining_itpm"` // 剩余每分钟输入令牌数
}

// Redis key 格式设计
const (
	ChannelQuotaKeyPrefix = "channel:quota:"
	QuotaExpiration       = 60 * time.Second // 1分钟过期
)

func GetChannelQuotaKey(channelId int) string {
	return fmt.Sprintf("%s%d", ChannelQuotaKeyPrefix, channelId)
}

// 更新渠道配额信息
func UpdateChannelQuota(channelId int, quota *ChannelQuota) error {
	key := GetChannelQuotaKey(channelId)
	data, err := json.Marshal(quota)
	if err != nil {
		return err
	}
	return RedisSet(key, string(data), QuotaExpiration)
}

// 获取渠道配额信息
func GetChannelQuota(channelId int) (*ChannelQuota, error) {
	key := GetChannelQuotaKey(channelId)
	data, err := RedisGet(key)
	if err != nil {
		return nil, err
	}

	var quota ChannelQuota
	err = json.Unmarshal([]byte(data), &quota)
	return &quota, err
}
