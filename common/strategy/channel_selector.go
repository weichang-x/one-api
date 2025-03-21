package strategy

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/ctxkey"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/model"
	"github.com/songquanpeng/one-api/relay/channeltype"
)

var (
	strategySelector      *StrategySelector
	ErrNoAvailableChannel = "no available channel with sufficient quota"
)

func init() {
	// 初始化策略选择器
	strategySelector = &StrategySelector{}
}

func GetStrategySelector() *StrategySelector {
	return strategySelector
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
		return nil, errors.New(ErrNoAvailableChannel)
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

	return nil, errors.New(ErrNoAvailableChannel)
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
