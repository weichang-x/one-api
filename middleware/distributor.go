package middleware

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/circuitbreaker"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/channeltype"
)

var (
	circuitBreakerManager *circuitbreaker.Manager
	strategySelector      *StrategySelector
	errNoAvailableChannel = "no available channel with sufficient quota"
)

func init() {
	// Initialize circuit breaker manager with default config
	circuitBreakerManager = circuitbreaker.NewManager(nil)
	strategySelector = &StrategySelector{}
}

type ModelRequest struct {
	Model string `json:"model" form:"model"`
}

func Distribute() func(c *gin.Context) {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
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
			// 对于 Claude 和 GPT 模型使用配置文件中指定的渠道选择策略
			// 这样可以对这些主流模型进行更细粒度的负载均衡控制
			if strings.HasPrefix(requestModel, "claude") || strings.HasPrefix(requestModel, "gpt") {
				config.ChannelStrategySelectEnabled = true
			}

			// 如果请求模型在使用配置策略模型列表中，则使用配置策略
			if isModelInList(requestModel, config.ChannelSelectorStrategyModels) {
				config.ChannelStrategySelectEnabled = true
			}

			if !config.ChannelStrategySelectEnabled {
				// 默认随机策略
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
				currentTime := time.Now().UnixMilli()
				var err error
				channel, err = strategySelector.SelectChannel(availableChannels, c)
				if err != nil {
					if err.Error() == errNoAvailableChannel {
						// Try to queue the request
						queue := GetDefaultQueue()
						channel, err = queue.EnqueueRequest(c.Request.Context())
						if err != nil {
							logger.SysError(fmt.Sprintf("Failed to queue request: %v", err))
							abortWithMessage(c, http.StatusServiceUnavailable, "所有渠道暂时不可用，请稍后重试")
							return
						}
					} else {
						logger.SysError(fmt.Sprintf("Failed to select channel: %v", err))
						abortWithMessage(c, http.StatusServiceUnavailable, "所有渠道暂时不可用，请稍后重试")
						return
					}
				}
				//耗时计算
				duration := time.Now().UnixMilli() - currentTime
				logger.Infof(ctx, "====>select channel #%d duration: %d ms", channel.Id, duration)
			}

			logger.Infof(ctx, "user id %d, user group: %s, request model: %s, select channel strategy_enable: %v, using channel #%d", userId, userGroup, requestModel, config.ChannelStrategySelectEnabled, channel.Id)
		}

		logger.Debugf(ctx, "user id %d, user group: %s, request model: %s, using channel #%d", userId, userGroup, requestModel, channel.Id)

		// 设置上下文并继续处理请求
		SetupContextForSelectedChannel(c, channel, requestModel)

		// 继续处理请求
		c.Next()
		currentTime := time.Now().UnixMilli()
		group := sync.WaitGroup{}
		group.Add(2)

		// 获取响应头中的配额信息
		quota := getQuotaHeader(c, channel.Type, int64(channel.Id))
		logger.Infof(ctx, "model: %s, using channel #%d, Level: %d", requestModel, channel.Id, channel.KeyLevel)
		// 更新账户等级
		go func() {
			model.UpdateChannelKeyLevel(int64(channel.Id), channel.Type, quota.RPM, quota.TPM, requestModel)
			group.Done()
		}()

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
		// 请求结束后更新实际配额信息
		go func() {
			updateQuotaAfterRequest(c, quota, userGroup, requestModel)
			group.Done()
		}()
		group.Wait()
		duration := time.Now().UnixMilli() - currentTime
		logger.Infof(ctx, "====>update quota #%d duration: %d ms", channel.Id, duration)
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
func getQuotaHeader(c *gin.Context, channelType int, channelId int64) *model.ChannelQuota {
	if channelType == channeltype.Anthropic {
		return &model.ChannelQuota{
			ChannelId:     channelId,
			RemainingTPM:  parseQuotaHeaderInt64(c, "anthropic-ratelimit-tokens-remaining"),
			RemainingRPM:  parseQuotaHeaderInt64(c, "anthropic-ratelimit-requests-remaining"),
			RemainingOTPM: parseQuotaHeaderInt64(c, "anthropic-ratelimit-output-tokens-remaining"),
			RemainingITPM: parseQuotaHeaderInt64(c, "anthropic-ratelimit-input-tokens-remaining"),
			TPM:           parseQuotaHeaderInt64(c, "anthropic-ratelimit-tokens-limit"),
			OTPM:          parseQuotaHeaderInt64(c, "anthropic-ratelimit-output-tokens-limit"),
			ITPM:          parseQuotaHeaderInt64(c, "anthropic-ratelimit-input-tokens-limit"),
			RPM:           parseQuotaHeaderInt64(c, "anthropic-ratelimit-requests-limit"),
			ResetTimeOTPM: parseQuotaRFC3339ResetTime(c, "anthropic-ratelimit-output-tokens-reset"), //'2025-03-20T01:42:59Z'
			// ResetTimeTPM:  parseQuotaRFC3339ResetTime(c, "anthropic-ratelimit-tokens-reset"),
		}
	} else if channelType == channeltype.OpenAI {
		return &model.ChannelQuota{
			ChannelId:    channelId,
			RemainingTPM: parseQuotaHeaderInt64(c, "x-ratelimit-remaining-tokens"),
			RemainingRPM: parseQuotaHeaderInt64(c, "x-ratelimit-remaining-requests"),
			TPM:          parseQuotaHeaderInt64(c, "x-ratelimit-limit-tokens"),
			RPM:          parseQuotaHeaderInt64(c, "x-ratelimit-limit-requests"),
			ResetTimeTPM: parseQuotaDurationResetTime(c, "x-ratelimit-reset-tokens"), //'1m6s'
		}
	}

	return &model.ChannelQuota{
		ChannelId:    channelId,
		RemainingTPM: parseQuotaHeaderInt64(c, "x-ratelimit-remaining-tokens"),
		RemainingRPM: parseQuotaHeaderInt64(c, "x-ratelimit-remaining-requests"),
		TPM:          parseQuotaHeaderInt64(c, "x-ratelimit-limit-tokens"),
		RPM:          parseQuotaHeaderInt64(c, "x-ratelimit-limit-requests"),
		ResetTimeTPM: parseQuotaDurationResetTime(c, "x-ratelimit-reset-tokens"),
	}
}

// 根据响应头更新配额信息
func updateQuotaAfterRequest(c *gin.Context, quota *model.ChannelQuota, userGroup, requestModel string) {
	// 获取最新请求响应时间
	latestLog, err := model.GetLatestLogByChannelId(int(quota.ChannelId))
	if err != nil {
		logger.SysError(fmt.Sprintf("Failed to get latest log: %v", err))
		return
	}

	// 检查当前响应时间是否大于等于最新请求响应时间
	currentTime := time.Now().Unix()
	if latestLog != nil && currentTime >= latestLog.CreatedAt {
		// 使用内存存储更新配额信息
		err := model.UpdateChannelQuota(userGroup, requestModel, quota)
		if err != nil {
			logger.SysError(fmt.Sprintf("Failed to update channel quota: %v", err))
		}
	} else {
		logger.Warnf(c, "当前响应时间小于最新请求响应时间不更新配额 channel #%d", quota.ChannelId)
	}
}

func parseQuotaHeaderInt64(c *gin.Context, header string) int64 {
	value := c.Writer.Header().Get(header)
	if value == "" {
		return 0
	}
	quota, _ := strconv.ParseInt(value, 10, 64)
	return quota
}

// 解析时间间隔 '1m6s' 返回 时间戳=当前时间+
func parseQuotaDurationResetTime(c *gin.Context, header string) int64 {
	value := c.Writer.Header().Get(header)
	if value == "" {
		return 0
	}
	resetTime, _ := time.ParseDuration(value)
	return time.Now().Unix() + int64(resetTime)
}

// 解析字符串时间格式 '2025-03-20T01:42:59Z'
func parseQuotaRFC3339ResetTime(c *gin.Context, header string) int64 {
	value := c.Writer.Header().Get(header)
	if value == "" {
		return 0
	}
	quota, _ := time.Parse(time.RFC3339, value)
	return quota.Unix()
}

// 策略
type StrategySelector struct {
	//互斥锁实现实现对于保护共享资源访问是线程安全的，可以有效防止竞态条件。
	//不过需要注意的是，这种全局锁的实现可能会在高并发场景下造成性能瓶颈，因为所有请求都需要串行执行 SelectChannel 方法。
	//如果性能成为问题，可能需要考虑使用更细粒度的锁策略或其他并发控制机制。
	mu sync.Mutex
}

func (s *StrategySelector) SelectChannel(channels []*model.Channel, ctx *gin.Context) (*model.Channel, error) {
	// 获取互斥锁
	s.mu.Lock()
	defer s.mu.Unlock()

	// 捕获异常，防止程序崩溃而导致互斥锁没有释放
	defer func() {
		if r := recover(); r != nil {
			logger.SysError(fmt.Sprintf("Failed to select channel recover panic: %v", r))
		}
	}()

	if len(channels) == 0 {
		return nil, errors.New(errNoAvailableChannel)
	}

	// 获取当前时间和请求参数
	userGroup := ctx.GetString(ctxkey.Group)
	requestModel := ctx.GetString(ctxkey.RequestModel)

	// 分类通道：重置时间已到和未到的通道
	var channelsA, channelsB []*model.Channel
	for _, channel := range channels {
		quota, err := model.GetChannelQuota(userGroup, requestModel, int64(channel.Id))
		if err != nil || quota == nil {
			continue
		}

		// 检查重置时间是否已到
		if quota.IsExpired(channel.Type) {
			channelsA = append(channelsA, channel)
		} else {
			channelsB = append(channelsB, channel)
		}
	}

	// 如果重置时间已到的通道列表不为空，选择第一个通道
	if len(channelsA) > 0 {
		selectedChannel := channelsA[0]
		quota, err := model.GetChannelQuota(userGroup, requestModel, int64(selectedChannel.Id))
		if err == nil && quota != nil {
			logger.Infof(ctx, "当前配额 channel #%d, RemainRPM: %d, RemainTPM: %d, ResetTimeTokens: %d", selectedChannel.Id, quota.RemainingRequests(), quota.RemainingTokens(selectedChannel.Type), quota.RemainingTokensResetTime(selectedChannel.Type))

			// 预扣除配额
			quota.DeductTokens(selectedChannel.Type, int64(config.MinTokenConsumptionThreshold))
			quota.DeductRPM(1)
			// 预估下次重置时间
			resetTime := EstimateNextResetTime(selectedChannel.Type, quota, requestModel)
			quota.UpdateResetTime(selectedChannel.Type, resetTime)
			logger.Infof(ctx, "预扣除配额结果 channel #%d, RemainRPM: %d, RemainTPM: %d, ResetTimeTokens: %d", selectedChannel.Id, quota.RemainingRequests(), quota.RemainingTokens(selectedChannel.Type), quota.RemainingTokensResetTime(selectedChannel.Type))
			// 更新内存中的配额信息
			model.UpdateChannelQuota(userGroup, requestModel, quota)
			return selectedChannel, nil
		}
	}

	// 对channelsB按剩余OTPM/TPM排序
	sort.Slice(channelsB, func(i, j int) bool {
		quotaI, _ := model.GetChannelQuota(userGroup, requestModel, int64(channelsB[i].Id))
		quotaJ, _ := model.GetChannelQuota(userGroup, requestModel, int64(channelsB[j].Id))
		if quotaI == nil || quotaJ == nil {
			return false
		}
		return quotaI.RemainingTokens(channelsB[i].Type) > quotaJ.RemainingTokens(channelsB[j].Type)
	})

	// 遍历排序后的通道列表，第一个通道剩余OTPM/TPM最大
	for index, channel := range channelsB {
		quota, err := model.GetChannelQuota(userGroup, requestModel, int64(channel.Id))
		if err != nil || quota == nil {
			continue
		}

		// 如果第一个通道的配额已经小于最小消耗token阈值，则没有可用通道
		if index == 0 && quota.RemainingTokens(channel.Type) <= int64(config.MinTokenConsumptionThreshold) {
			break
		}

		// 通道配额大于等于最小消耗token阈值 并且 当前并发量小于等于最大并发量
		if quota.RemainingTokens(channel.Type) >= int64(config.MinTokenConsumptionThreshold) && (quota.TotalRequests()-quota.RemainingRequests()) <= int64(config.GetChannelTypeConcurrentLimit(channel.Type)) {
			logger.Infof(ctx, "当前配额 channel #%d, RemainRPM: %d, RemainTPM: %d, ResetTimeTokens: %d", channel.Id, quota.RemainingRequests(), quota.RemainingTokens(channel.Type), quota.RemainingTokensResetTime(channel.Type))

			// 预扣除配额
			quota.DeductTokens(channel.Type, int64(config.MinTokenConsumptionThreshold))
			quota.DeductRPM(1)
			// 预估下次重置时间
			resetTime := EstimateNextResetTime(channel.Type, quota, requestModel)
			quota.UpdateResetTime(channel.Type, resetTime)
			logger.Infof(ctx, "预扣除配额结果 channel #%d, RemainRPM: %d, RemainTPM: %d, ResetTimeTokens: %d", channel.Id, quota.RemainingRequests(), quota.RemainingTokens(channel.Type), quota.RemainingTokensResetTime(channel.Type))
			// 更新内存中的配额信息
			model.UpdateChannelQuota(userGroup, requestModel, quota)
			return channel, nil
		}
	}

	return nil, errors.New(errNoAvailableChannel)
}

func EstimateNextResetTime(channelType int, quota *model.ChannelQuota, requestModel string) int64 {
	ResetTimeWindowOpenAI := int64(180)
	ResetTimeWindowClaude := int64(10)
	ResetTimeWindow := int64(60)
	// 根据通道类型使用特定规则
	switch channelType {
	case channeltype.OpenAI:
		// OpenAI 使用滚动窗口
		return quota.ResetTimeTPM + ResetTimeWindowOpenAI // 3分钟窗口
	case channeltype.Anthropic:
		// Claude 可能有更长的窗口
		return quota.ResetTimeOTPM + ResetTimeWindowClaude // 10s窗口
	}
	return quota.ResetTimeTPM + ResetTimeWindow
}
