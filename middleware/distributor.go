package middleware

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/circuitbreaker"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/common/helper"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/common/random"
	"github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/channeltype"
)

var (
	circuitBreakerManager *circuitbreaker.Manager
)

func init() {
	// Initialize circuit breaker manager with default config
	circuitBreakerManager = circuitbreaker.NewManager(nil)
}

type ModelRequest struct {
	Model string `json:"model" form:"model"`
}

func Distribute() func(c *gin.Context) {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		requestId := c.GetString(helper.RequestIdKey)
		userId := c.GetInt(ctxkey.Id)
		userGroup, _ := model.CacheGetUserGroup(userId)
		c.Set(ctxkey.Group, userGroup)

		var requestModel string
		var channel *model.Channel
		channelId, ok := c.Get(ctxkey.SpecificChannelId)
		if ok {
			id, err := strconv.Atoi(channelId.(string))
			if err != nil {
				abortWithMessage(c, http.StatusBadRequest, "无效的渠道 Id")
				return
			}
			channel, err = model.GetChannelById(id, true)
			if err != nil {
				abortWithMessage(c, http.StatusBadRequest, "无效的渠道 Id")
				return
			}
			if channel.Status != model.ChannelStatusEnabled {
				abortWithMessage(c, http.StatusForbidden, "该渠道已被禁用")
				return
			}

			if config.CircuitBreakerEnabled {
				// Check if channel is available (not in circuit breaker OPEN state)
				cb := circuitBreakerManager.GetBreaker(channel.Id)
				allowed, err := cb.AllowRequest()
				if err != nil {
					logger.SysError(fmt.Sprintf("Failed to check circuit breaker state: %v", err))
				} else if !allowed {
					abortWithMessage(c, http.StatusServiceUnavailable, "该渠道暂时不可用，请稍后重试")
					return
				}
			}
		} else {
			requestModel = c.GetString(ctxkey.RequestModel)
			// 设置渠道选择策略
			strategy := "default"
			// 对于 Claude 和 GPT 模型使用配置文件中指定的渠道选择策略
			// 这样可以对这些主流模型进行更细粒度的负载均衡控制
			if strings.HasPrefix(requestModel, "claude") || strings.HasPrefix(requestModel, "gpt") {
				strategy = config.ChannelSelectorStrategy
			}

			// 如果请求模型在使用配置策略模型列表中，则使用配置策略
			// 这样可以进行更细粒度的负载均衡控制
			if isModelInList(requestModel, config.ChannelSelectorStrategyModels) {
				strategy = config.ChannelSelectorStrategy
			}

			// 获取选择策略
			selector := getChannelSelector(strategy)
			//默认随机策略（保持现有逻辑）
			if selector == nil {
				var err error
				channel, err = model.CacheGetRandomSatisfiedChannel(userGroup, requestModel, false)
				if err != nil {
					message := fmt.Sprintf("当前分组 %s 下对于模型 %s 无可用渠道", userGroup, requestModel)
					if channel != nil {
						logger.SysError(fmt.Sprintf("渠道不存在：%d", channel.Id))
						message = "数据库一致性已被破坏，请联系管理员"
					}
					abortWithMessage(c, http.StatusServiceUnavailable, message)
					return
				}

				if config.CircuitBreakerEnabled {
					// Check if selected channel is available
					cb := circuitBreakerManager.GetBreaker(channel.Id)
					allowed, err := cb.AllowRequest()
					if err != nil {
						logger.SysError(fmt.Sprintf("Failed to check circuit breaker state: %v", err))
					} else if !allowed {
						// Try to get another channel if this one is not available
						channel, err = model.CacheGetRandomSatisfiedChannel(userGroup, requestModel, true)
						if err != nil {
							abortWithMessage(c, http.StatusServiceUnavailable, "所有渠道暂时不可用，请稍后重试")
							return
						}
					}
				}
			} else {
				// 使用Redis分布式锁确保选择渠道时只有一个请求在执行
				common.NewRedisLock(common.GetChannelSelectLockKey(requestId), func() {
					// 获取可用渠道
					availableChannels := model.GetAvailableChannels(userGroup, requestModel)
					if len(availableChannels) == 0 {
						message := fmt.Sprintf("当前分组 %s 下对于模型 %s 无可用渠道", userGroup, requestModel)
						abortWithMessage(c, http.StatusServiceUnavailable, message)
						return
					}
					if config.CircuitBreakerEnabled {
						// Filter channels through circuit breaker
						availableChannels, err := circuitBreakerManager.GetAvailableChannels(availableChannels)
						if err != nil {
							logger.SysError(fmt.Sprintf("Failed to filter channels through circuit breaker: %v", err))
						} else if len(availableChannels) == 0 {
							abortWithMessage(c, http.StatusServiceUnavailable, "所有渠道暂时不可用，请稍后重试")
							return
						}
					}

					// 使用选择策略选择渠道
					var err error
					channel, err = selector.SelectChannel(availableChannels, c)
					if err != nil {
						logger.SysError(fmt.Sprintf("select channel with strategy %s error: %v", selector.Strategy(), err))
						message := fmt.Sprintf("当前分组 %s 下对于模型 %s 无可用渠道", userGroup, requestModel)
						if channel != nil {
							logger.SysError(fmt.Sprintf("渠道不存在：%d", channel.Id))
							message = "数据库一致性已被破坏，请联系管理员"
						}
						abortWithMessage(c, http.StatusServiceUnavailable, message)
						return
					}
				})
			}

			if selector != nil {
				strategy = selector.Strategy()
			}
			logger.Infof(ctx, "user id %d, user group: %s, request model: %s, select channel strategy: %s, using channel #%d", userId, userGroup, requestModel, strategy, channel.Id)
		}

		logger.Debugf(ctx, "user id %d, user group: %s, request model: %s, using channel #%d", userId, userGroup, requestModel, channel.Id)
		// 设置上下文并继续处理请求
		SetupContextForSelectedChannel(c, channel, requestModel)

		// 继续处理请求
		c.Next()
		// 使用Redis分布式锁确保更新熔断器状态时只有一个请求在执行
		common.NewRedisLock(common.GetCircuitBreakerManagerLockKey(channel.Id), func() {
			if config.CircuitBreakerEnabled {
				// 获取响应状态码
				statusCode := c.Writer.Status()

				// 根据响应状态码更新熔断器状态
				if statusCode >= 500 {
					err := circuitBreakerManager.RecordFailure(channel.Id)
					if err != nil {
						logger.SysError(fmt.Sprintf("Failed to record failure in circuit breaker: %v", err))
					}
				} else if statusCode < 500 && statusCode >= 200 {
					err := circuitBreakerManager.RecordSuccess(channel.Id)
					if err != nil {
						logger.SysError(fmt.Sprintf("Failed to record success in circuit breaker: %v", err))
					}
				}
			}
		})
		// 请求结束后使用Redis原子操作更新配额信息
		updateQuotaAfterRequest(c, channel, requestModel)
	}
}

func SetupContextForSelectedChannel(c *gin.Context, channel *model.Channel, modelName string) {
	c.Set(ctxkey.Channel, channel.Type)
	c.Set(ctxkey.ChannelId, channel.Id)
	c.Set(ctxkey.ChannelName, channel.Name)
	if channel.SystemPrompt != nil && *channel.SystemPrompt != "" {
		c.Set(ctxkey.SystemPrompt, *channel.SystemPrompt)
	}
	c.Set(ctxkey.ModelMapping, channel.GetModelMapping())
	c.Set(ctxkey.OriginalModel, modelName) // for retry
	c.Request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", channel.Key))
	c.Set(ctxkey.BaseURL, channel.GetBaseURL())
	cfg, _ := channel.LoadConfig()
	// this is for backward compatibility
	if channel.Other != nil {
		switch channel.Type {
		case channeltype.Azure:
			if cfg.APIVersion == "" {
				cfg.APIVersion = *channel.Other
			}
		case channeltype.Xunfei:
			if cfg.APIVersion == "" {
				cfg.APIVersion = *channel.Other
			}
		case channeltype.Gemini:
			if cfg.APIVersion == "" {
				cfg.APIVersion = *channel.Other
			}
		case channeltype.AIProxyLibrary:
			if cfg.LibraryID == "" {
				cfg.LibraryID = *channel.Other
			}
		case channeltype.Ali:
			if cfg.Plugin == "" {
				cfg.Plugin = *channel.Other
			}
		}
	}
	c.Set(ctxkey.Config, cfg)
}

// 根据请求模型获取响应头配额信息
// 返回值为nil表示不支持该模型
func getQuotaHeader(c *gin.Context, requestModel string) *common.ChannelQuota {
	if strings.HasPrefix(requestModel, "claude") {
		return &common.ChannelQuota{
			RemainingTPM:  parseQuotaHeader(c, "anthropic-ratelimit-tokens-remaining"),
			RemainingRPM:  parseQuotaHeader(c, "anthropic-ratelimit-requests-remaining"),
			RemainingOTPM: parseQuotaHeader(c, "anthropic-ratelimit-output-tokens-remaining"),
			RemainingITPM: parseQuotaHeader(c, "anthropic-ratelimit-input-tokens-remaining"),
		}
	} else if strings.HasPrefix(requestModel, "gpt") {
		return &common.ChannelQuota{
			RemainingTPM: parseQuotaHeader(c, "x-ratelimit-remaining-tokens"),
			RemainingRPM: parseQuotaHeader(c, "x-ratelimit-remaining-requests"),
		}
	}
	return nil
}

// 根据响应头更新配额信息
func updateQuotaAfterRequest(c *gin.Context, channel *model.Channel, requestModel string) {
	// 获取响应头中的配额信息
	newQuota := getQuotaHeader(c, requestModel)
	if newQuota == nil {
		return
	}

	// 使用Redis原子操作更新配额信息
	err := common.UpdateChannelQuota(channel.Id, newQuota)
	if err != nil {
		logger.SysError(fmt.Sprintf("Failed to update channel quota: %v", err))
	}
}

func parseQuotaHeader(c *gin.Context, header string) int64 {
	value := c.Writer.Header().Get(header)
	if value == "" {
		return 0
	}
	quota, _ := strconv.ParseInt(value, 10, 64)
	return quota
}

// 获取选择器
func getChannelSelector(strategy string) ChannelSelector {
	// 从请求参数或配置中获取选择策略
	switch strategy {
	case "quota_round_robin":
		return NewQuotaRoundRobinSelector()
	case "min_request":
		return NewMinRequestSelector()
	}
	return nil
}

// =======channel selector========

type ChannelSelector interface {
	SelectChannel(channels []*model.Channel, ctx *gin.Context) (*model.Channel, error)
	Strategy() string
}

// 剩余配额轮询策略
type QuotaRoundRobinSelector struct{}

func NewQuotaRoundRobinSelector() ChannelSelector {
	return &QuotaRoundRobinSelector{}
}

func (s *QuotaRoundRobinSelector) Strategy() string {
	return "quota_round_robin"
}

func (s *QuotaRoundRobinSelector) SelectChannel(channels []*model.Channel, ctx *gin.Context) (*model.Channel, error) {
	maxQuota := int64(0)
	selectedChannel := (*model.Channel)(nil)
	availableChannels := make([]*model.Channel, 0)

	getQuota := func(quota *common.ChannelQuota) int64 {
		if quota.RemainingOTPM > 0 {
			return quota.RemainingOTPM
		}
		return quota.RemainingTPM
	}

	for _, channel := range channels {
		quota, err := common.GetChannelQuota(channel.Id)
		if err != nil {
			if err == redis.Nil {
				availableChannels = append(availableChannels, channel)
				continue
			}
			// logger.SysLogf(fmt.Sprintf("Failed to get channel quota: %v", err))
			continue
		}
		remainingQuota := getQuota(quota)
		if remainingQuota > maxQuota {
			maxQuota = remainingQuota
			selectedChannel = channel
		}
	}

	if selectedChannel == nil && len(availableChannels) > 0 {
		if len(availableChannels) == 1 {
			return availableChannels[0], nil
		}
		randomIndex := random.RandRange(0, len(availableChannels)-1)
		selectedChannel = availableChannels[randomIndex]
		return selectedChannel, nil
	}

	if selectedChannel == nil {
		return nil, errors.New("no available channel with sufficient quota")
	}
	return selectedChannel, nil
}

// 最小请求数策略
type MinRequestSelector struct{}

func NewMinRequestSelector() ChannelSelector {
	return &MinRequestSelector{}
}

func (s *MinRequestSelector) Strategy() string {
	return "min_request"
}

func (s *MinRequestSelector) SelectChannel(channels []*model.Channel, ctx *gin.Context) (*model.Channel, error) {
	minRequests := int64(math.MaxInt64)
	selectedChannel := (*model.Channel)(nil)
	availableChannels := make([]*model.Channel, 0)

	for _, channel := range channels {
		quota, err := common.GetChannelQuota(channel.Id)
		if err != nil {
			if err == redis.Nil {
				availableChannels = append(availableChannels, channel)
				continue
			}
			// logger.SysLogf(fmt.Sprintf("Failed to get channel quota: %v", err))
			continue
		}
		if quota.RemainingRPM < minRequests {
			minRequests = quota.RemainingRPM
			selectedChannel = channel
		}
	}

	if selectedChannel == nil && len(availableChannels) > 0 {
		if len(availableChannels) == 1 {
			return availableChannels[0], nil
		}
		randomIndex := random.RandRange(0, len(availableChannels)-1)
		selectedChannel = availableChannels[randomIndex]
		return selectedChannel, nil
	}

	if selectedChannel == nil {
		return nil, errors.New("no available channel")
	}
	return selectedChannel, nil
}
