package common

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common/logger"
)

var RDB *redis.Client
var RedisEnabled = true

// InitRedisClient This function is called after init()
func InitRedisClient() (err error) {
	if os.Getenv("REDIS_CONN_STRING") == "" {
		RedisEnabled = false
		logger.SysLog("REDIS_CONN_STRING not set, Redis is not enabled")
		return nil
	}
	if os.Getenv("SYNC_FREQUENCY") == "" {
		RedisEnabled = false
		logger.SysLog("SYNC_FREQUENCY not set, Redis is disabled")
		return nil
	}
	logger.SysLog("Redis is enabled")
	opt, err := redis.ParseURL(os.Getenv("REDIS_CONN_STRING"))
	if err != nil {
		logger.FatalLog("failed to parse Redis connection string: " + err.Error())
	}
	RDB = redis.NewClient(opt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = RDB.Ping(ctx).Result()
	if err != nil {
		logger.FatalLog("Redis ping test failed: " + err.Error())
	}
	return err
}

func ParseRedisOption() *redis.Options {
	opt, err := redis.ParseURL(os.Getenv("REDIS_CONN_STRING"))
	if err != nil {
		logger.FatalLog("failed to parse Redis connection string: " + err.Error())
	}
	return opt
}

func RedisSet(key string, value string, expiration time.Duration) error {
	ctx := context.Background()
	return RDB.Set(ctx, key, value, expiration).Err()
}

func RedisGet(key string) (string, error) {
	ctx := context.Background()
	return RDB.Get(ctx, key).Result()
}

func RedisDel(key string) error {
	ctx := context.Background()
	return RDB.Del(ctx, key).Err()
}

func RedisDecrease(key string, value int64) error {
	ctx := context.Background()
	return RDB.DecrBy(ctx, key, value).Err()
}

// RedisSetWithExpiration sets a key with expiration time
func RedisSetWithExpiration(key string, value string, expiration time.Duration) error {
	if RDB == nil {
		return errors.New("redis client is nil")
	}
	return RDB.Set(context.Background(), key, value, expiration).Err()
}

// 使用 Redis 分布式锁执行回调函数
func NewRedisLock(key string, callback func()) {
	lock := newRedisLock(RDB, key, 10*time.Second)
	if lock.Acquire() {
		defer lock.Release()
		callback()
	}
}

type RedisLock struct {
	client    *redis.Client
	key       string
	value     string
	expire    time.Duration
	cancelCtx context.Context
	cancel    context.CancelFunc
}

func newRedisLock(client *redis.Client, key string, expire time.Duration) *RedisLock {
	return &RedisLock{
		client: client,
		key:    key,
		value:  fmt.Sprintf("%d", time.Now().UnixNano()),
		expire: expire,
	}
}

// Acquire 尝试获取锁
func (rl *RedisLock) Acquire() bool {
	ctx := context.Background()
	// 使用 SET NX EX 命令尝试获取锁
	result, err := rl.client.SetNX(ctx, rl.key, rl.value, rl.expire).Result()
	if err != nil {
		logger.SysError(fmt.Sprintf("Failed to acquire lock: %v", err))
		return false
	}
	if result {
		// 创建一个可取消的上下文，用于续期
		rl.cancelCtx, rl.cancel = context.WithCancel(ctx)
		go rl.renewLock()
	}
	return result
}

// Release 释放锁
func (rl *RedisLock) Release() {
	// 取消续期
	if rl.cancel != nil {
		rl.cancel()
	}

	ctx := context.Background()
	// 使用 Lua 脚本来确保只有锁的持有者才能释放锁
	script := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end`
	_, err := rl.client.Eval(ctx, script, []string{rl.key}, rl.value).Result()
	if err != nil {
		logger.SysError(fmt.Sprintf("Failed to release lock: %v", err))
	}
}

// renewLock 续期锁
func (rl *RedisLock) renewLock() {
	ticker := time.NewTicker(rl.expire / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			// 使用 EXPIRE 命令续期锁
			_, err := rl.client.Expire(ctx, rl.key, rl.expire).Result()
			if err != nil {
				log.Printf("Failed to renew lock: %v", err)
				return
			}
		case <-rl.cancelCtx.Done():
			return
		}
	}
}
