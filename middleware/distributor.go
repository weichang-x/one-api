package middleware

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/circuitbreaker"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/common/strategy"
	"github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/channeltype"
)

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
		var ChannelStrategyEnabled bool
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

			// Check if channel is available (not in circuit breaker OPEN state)
			cb := circuitbreaker.GetManager().GetBreaker(channel.Id)
			allowed, err := cb.AllowRequest()
			if err != nil {
				logger.SysError(fmt.Sprintf("Failed to check circuit breaker state: %v", err))
			} else if !allowed {
				abortWithMessage(c, http.StatusServiceUnavailable, "该渠道暂时不可用，请稍后重试")
				return
			}
		} else {
			requestModel = c.GetString(ctxkey.RequestModel)
			// 对于 Claude 和 GPT 模型使用配置文件中指定的渠道选择策略
			if strings.HasPrefix(requestModel, "claude") || strings.HasPrefix(requestModel, "gpt") {
				ChannelStrategyEnabled = true
			}

			// 如果请求模型在使用配置策略模型列表中，则使用配置策略
			if config.ChannelSelectorStrategyModels != "" && isModelInList(requestModel, config.ChannelSelectorStrategyModels) {
				ChannelStrategyEnabled = true
			}
			c.Set(ctxkey.KeyChannelStrategyEnabled, ChannelStrategyEnabled)

			if !ChannelStrategyEnabled {
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

				// Check if selected channel is available
				cb := circuitbreaker.GetManager().GetBreaker(channel.Id)
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
			} else {
				// 获取可用渠道
				availableChannels := model.GetAvailableChannels(userGroup, requestModel)
				if len(availableChannels) == 0 {
					message := fmt.Sprintf("当前分组 %s 下对于模型 %s 无可用渠道", userGroup, requestModel)
					abortWithMessage(c, http.StatusServiceUnavailable, message)
					return
				}
				// Filter channels through circuit breaker
				if availableChannels, err := circuitbreaker.GetManager().GetAvailableChannels(availableChannels); err != nil {
					logger.SysError(fmt.Sprintf("Failed to filter channels through circuit breaker: %v", err))
				} else if len(availableChannels) == 0 {
					abortWithMessage(c, http.StatusServiceUnavailable, "所有渠道暂时不可用，请稍后重试")
					return
				}

				currentTime := time.Now().UnixMilli()
				var err error
				channel, err = strategy.GetStrategySelector().SelectChannel(availableChannels, c)
				if err != nil {
					if err.Error() == strategy.ErrNoAvailableChannel {
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

			logger.Infof(ctx, "user id %d, user group: %s, request model: %s, select channel strategy_enable: %v, using channel #%d", userId, userGroup, requestModel, ChannelStrategyEnabled, channel.Id)
		}

		logger.Debugf(ctx, "user id %d, user group: %s, request model: %s, using channel #%d", userId, userGroup, requestModel, channel.Id)

		// 设置上下文并继续处理请求
		SetupContextForSelectedChannel(c, channel, requestModel)

		// 继续处理请求
		c.Next()
		currentTime := time.Now().UnixMilli()
		group := sync.WaitGroup{}
		group.Add(1)

		// 获取响应头中的配额信息
		quota := getQuotaHeader(c, channel)
		// resetTime := quota.RemainingTokensResetTime(channel, requestModel)
		// resetTimeStr := time.Unix(resetTime, 0).Format("2006-01-02 15:04:05")
		// logger.Infof(ctx, "model: %s, using channel #%d, ========>>>>OTPM/TPM reset time: %v", requestModel, channel.Id, resetTimeStr)
		logger.Infof(ctx, "model: %s, using channel #%d, Level: %d, quota OTPM/TPM: %v, quota RPM: %v ", requestModel, channel.Id, channel.KeyLevel, quota.RemainingTokens(channel, requestModel), quota.RemainingRequests())
		// 更新账户等级
		go func() {
			model.UpdateChannelKeyLevel(int64(channel.Id), channel.Type, quota.RPM, quota.TPM, requestModel)
			// 如果使用策略选择渠道，则更新配额信息
			if ChannelStrategyEnabled {
				updateQuotaAfterRequest(c, quota, userGroup, requestModel)
			}
			group.Done()
		}()
		group.Wait()
		duration := time.Now().UnixMilli() - currentTime
		if ChannelStrategyEnabled {
			logger.Infof(ctx, "====>update quota #%d duration: %d ms", channel.Id, duration)
		}
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
func getQuotaHeader(c *gin.Context, channel *model.Channel) *model.ChannelQuota {
	if channel.GetModelType(c.GetString(ctxkey.RequestModel)) == model.ModelTypeClaude {
		return &model.ChannelQuota{
			ChannelId:     int64(channel.Id),
			RemainingTPM:  parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Tokens-Remaining"),
			RemainingRPM:  parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Requests-Remaining"),
			RemainingOTPM: parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Output-Tokens-Remaining"),
			RemainingITPM: parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Input-Tokens-Remaining"),
			TPM:           parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Tokens-Limit"),
			OTPM:          parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Output-Tokens-Limit"),
			ITPM:          parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Input-Tokens-Limit"),
			RPM:           parseQuotaHeaderInt64(c, "Anthropic-Ratelimit-Requests-Limit"),
			ResetTimeOTPM: parseQuotaRFC3339ResetTime(c, "Anthropic-Ratelimit-Output-Tokens-Reset"), //'2025-03-20T01:42:59Z'
		}
	} else if channel.GetModelType(c.GetString(ctxkey.RequestModel)) == model.ModelTypeOpenAI {
		return &model.ChannelQuota{
			ChannelId:    int64(channel.Id),
			RemainingTPM: parseQuotaHeaderInt64(c, "x-ratelimit-remaining-tokens"),
			RemainingRPM: parseQuotaHeaderInt64(c, "x-ratelimit-remaining-requests"),
			TPM:          parseQuotaHeaderInt64(c, "x-ratelimit-limit-tokens"),
			RPM:          parseQuotaHeaderInt64(c, "x-ratelimit-limit-requests"),
			ResetTimeTPM: parseQuotaDurationResetTime(c, "x-ratelimit-reset-tokens"), //'1m6s'
		}
	}

	return &model.ChannelQuota{
		ChannelId:    int64(channel.Id),
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
