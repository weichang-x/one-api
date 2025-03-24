package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/songquanpeng/one-api/common"
	"github.com/songquanpeng/one-api/common/config"
	"github.com/songquanpeng/one-api/common/logger"
	"github.com/songquanpeng/one-api/common/random"
	"github.com/songquanpeng/one-api/relay/channeltype"
)

var (
	TokenCacheSeconds         = config.SyncFrequency
	UserId2GroupCacheSeconds  = config.SyncFrequency
	UserId2QuotaCacheSeconds  = config.SyncFrequency
	UserId2StatusCacheSeconds = config.SyncFrequency
	GroupModelsCacheSeconds   = config.SyncFrequency
)

func CacheGetTokenByKey(key string) (*Token, error) {
	keyCol := "`key`"
	if common.UsingPostgreSQL {
		keyCol = `"key"`
	}
	var token Token
	if !common.RedisEnabled {
		err := DB.Where(keyCol+" = ?", key).First(&token).Error
		return &token, err
	}
	tokenObjectString, err := common.RedisGet(fmt.Sprintf("token:%s", key))
	if err != nil {
		err := DB.Where(keyCol+" = ?", key).First(&token).Error
		if err != nil {
			return nil, err
		}
		jsonBytes, err := json.Marshal(token)
		if err != nil {
			return nil, err
		}
		err = common.RedisSet(fmt.Sprintf("token:%s", key), string(jsonBytes), time.Duration(TokenCacheSeconds)*time.Second)
		if err != nil {
			logger.SysError("Redis set token error: " + err.Error())
		}
		return &token, nil
	}
	err = json.Unmarshal([]byte(tokenObjectString), &token)
	return &token, err
}

func CacheGetUserGroup(id int) (group string, err error) {
	if !common.RedisEnabled {
		return GetUserGroup(id)
	}
	group, err = common.RedisGet(fmt.Sprintf("user_group:%d", id))
	if err != nil {
		group, err = GetUserGroup(id)
		if err != nil {
			return "", err
		}
		err = common.RedisSet(fmt.Sprintf("user_group:%d", id), group, time.Duration(UserId2GroupCacheSeconds)*time.Second)
		if err != nil {
			logger.SysError("Redis set user group error: " + err.Error())
		}
	}
	return group, err
}

func fetchAndUpdateUserQuota(ctx context.Context, id int) (quota int64, err error) {
	quota, err = GetUserQuota(id)
	if err != nil {
		return 0, err
	}
	err = common.RedisSet(fmt.Sprintf("user_quota:%d", id), fmt.Sprintf("%d", quota), time.Duration(UserId2QuotaCacheSeconds)*time.Second)
	if err != nil {
		logger.Error(ctx, "Redis set user quota error: "+err.Error())
	}
	return
}

func CacheGetUserQuota(ctx context.Context, id int) (quota int64, err error) {
	if !common.RedisEnabled {
		return GetUserQuota(id)
	}
	quotaString, err := common.RedisGet(fmt.Sprintf("user_quota:%d", id))
	if err != nil {
		return fetchAndUpdateUserQuota(ctx, id)
	}
	quota, err = strconv.ParseInt(quotaString, 10, 64)
	if err != nil {
		return 0, nil
	}
	if quota <= config.PreConsumedQuota { // when user's quota is less than pre-consumed quota, we need to fetch from db
		logger.Infof(ctx, "user %d's cached quota is too low: %d, refreshing from db", quota, id)
		return fetchAndUpdateUserQuota(ctx, id)
	}
	return quota, nil
}

func CacheUpdateUserQuota(ctx context.Context, id int) error {
	if !common.RedisEnabled {
		return nil
	}
	quota, err := CacheGetUserQuota(ctx, id)
	if err != nil {
		return err
	}
	err = common.RedisSet(fmt.Sprintf("user_quota:%d", id), fmt.Sprintf("%d", quota), time.Duration(UserId2QuotaCacheSeconds)*time.Second)
	return err
}

func CacheDecreaseUserQuota(id int, quota int64) error {
	if !common.RedisEnabled {
		return nil
	}
	err := common.RedisDecrease(fmt.Sprintf("user_quota:%d", id), int64(quota))
	return err
}

func CacheIsUserEnabled(userId int) (bool, error) {
	if !common.RedisEnabled {
		return IsUserEnabled(userId)
	}
	enabled, err := common.RedisGet(fmt.Sprintf("user_enabled:%d", userId))
	if err == nil {
		return enabled == "1", nil
	}

	userEnabled, err := IsUserEnabled(userId)
	if err != nil {
		return false, err
	}
	enabled = "0"
	if userEnabled {
		enabled = "1"
	}
	err = common.RedisSet(fmt.Sprintf("user_enabled:%d", userId), enabled, time.Duration(UserId2StatusCacheSeconds)*time.Second)
	if err != nil {
		logger.SysError("Redis set user enabled error: " + err.Error())
	}
	return userEnabled, err
}

func CacheGetGroupModels(ctx context.Context, group string) ([]string, error) {
	if !common.RedisEnabled {
		return GetGroupModels(ctx, group)
	}
	modelsStr, err := common.RedisGet(fmt.Sprintf("group_models:%s", group))
	if err == nil {
		return strings.Split(modelsStr, ","), nil
	}
	models, err := GetGroupModels(ctx, group)
	if err != nil {
		return nil, err
	}
	err = common.RedisSet(fmt.Sprintf("group_models:%s", group), strings.Join(models, ","), time.Duration(GroupModelsCacheSeconds)*time.Second)
	if err != nil {
		logger.SysError("Redis set group models error: " + err.Error())
	}
	return models, nil
}

var group2model2channels map[string]map[string][]*Channel
var channelSyncLock sync.RWMutex

func InitChannelCache() {
	newChannelId2channel := make(map[int]*Channel)
	var channels []*Channel
	DB.Where("status = ?", ChannelStatusEnabled).Find(&channels)
	for _, channel := range channels {
		newChannelId2channel[channel.Id] = channel
	}
	var abilities []*Ability
	DB.Find(&abilities)
	groups := make(map[string]bool)
	for _, ability := range abilities {
		groups[ability.Group] = true
	}
	newGroup2model2channels := make(map[string]map[string][]*Channel)
	for group := range groups {
		newGroup2model2channels[group] = make(map[string][]*Channel)
	}
	for _, channel := range channels {
		groups := strings.Split(channel.Group, ",")
		for _, group := range groups {
			models := strings.Split(channel.Models, ",")
			for _, model := range models {
				if _, ok := newGroup2model2channels[group][model]; !ok {
					newGroup2model2channels[group][model] = make([]*Channel, 0)
				}
				newGroup2model2channels[group][model] = append(newGroup2model2channels[group][model], channel)
			}
		}
	}

	// sort by priority
	for group, model2channels := range newGroup2model2channels {
		for model, channels := range model2channels {
			sort.Slice(channels, func(i, j int) bool {
				return channels[i].GetPriority() > channels[j].GetPriority()
			})
			newGroup2model2channels[group][model] = channels
		}
	}

	channelSyncLock.Lock()
	group2model2channels = newGroup2model2channels
	channelSyncLock.Unlock()
	logger.SysLog("channels synced from database")
}

func SyncChannelCache(frequency int) {
	for {
		time.Sleep(time.Duration(frequency) * time.Second)
		logger.SysLog("syncing channels from database")
		InitChannelCache()
		IncrementChannelQuotaStoreFromDB()
	}
}

func CacheGetRandomSatisfiedChannel(group string, model string, ignoreFirstPriority bool) (*Channel, error) {
	if !config.MemoryCacheEnabled {
		// 如果内存缓存未启用，则直接从数据库获取
		fmt.Println("get random satisfied channel from database")
		return GetRandomSatisfiedChannel(group, model, ignoreFirstPriority)
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()
	channels := group2model2channels[group][model]
	if len(channels) == 0 {
		return nil, errors.New("channel not found")
	}
	endIdx := len(channels)
	// choose by priority
	firstChannel := channels[0]
	if firstChannel.GetPriority() > 0 {
		for i := range channels {
			if channels[i].GetPriority() != firstChannel.GetPriority() {
				endIdx = i
				break
			}
		}
	}
	idx := rand.Intn(endIdx)
	if ignoreFirstPriority {
		if endIdx < len(channels) { // which means there are more than one priority
			idx = random.RandRange(endIdx, len(channels))
		}
	}
	return channels[idx], nil
}

// GetAvailableChannels 获取指定用户组和模型下所有可用的渠道
func GetAvailableChannels(userGroup string, requestModel string) []*Channel {
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	// 从缓存中获取该用户组下该模型的所有渠道
	channels := group2model2channels[userGroup][requestModel]
	if len(channels) == 0 {
		return nil
	}

	// 过滤出状态为启用的渠道
	availableChannels := make([]*Channel, 0)
	for _, channel := range channels {
		if channel.Status == ChannelStatusEnabled {
			availableChannels = append(availableChannels, channel)
		}
	}

	return availableChannels
}

//=======================================================
// 以下是通道限速信息的缓存机制
//=======================================================

type ChannelQuota struct {
	ChannelId     int64 // 通道id
	RPM           int64 // 每分钟最大请求数
	TPM           int64 // 每分钟最大令牌数
	OTPM          int64 // 每分钟最大输出令牌数(claude)
	ITPM          int64 // 每分钟最大输入令牌数(claude)
	RemainingTPM  int64 // 剩余每分钟令牌数
	RemainingRPM  int64 // 剩余每分钟请求数
	RemainingOTPM int64 // 剩余每分钟输出令牌数(claude)
	RemainingITPM int64 // 剩余每分钟输入令牌数(claude)
	ResetTimeOTPM int64 // 每分钟输出令牌数重置时间(claude)
	ResetTimeTPM  int64 // 每分钟令牌数重置时间
}

// 初始化通道配额信息
func initChannelQuotaData(channelId int64, channelType int, requestModel string) *ChannelQuota {
	if channelType == channeltype.Anthropic || (channelType == channeltype.Custom && strings.HasPrefix(requestModel, "claude-")) {
		return &ChannelQuota{
			ChannelId:     channelId,
			OTPM:          8000,
			ITPM:          20000,
			RPM:           50,
			TPM:           28000,
			RemainingTPM:  28000,
			RemainingOTPM: 8000,
			RemainingRPM:  50,
			RemainingITPM: 20000,
			ResetTimeOTPM: time.Now().Unix(),
		}
	}
	return &ChannelQuota{
		ChannelId:    channelId,
		TPM:          30000,
		RPM:          500,
		RemainingTPM: 30000,
		RemainingRPM: 500,
		ResetTimeTPM: time.Now().Unix(),
	}
}

// 检查配额是否过期
func (q *ChannelQuota) IsExpired(channel *Channel, requestModel string) bool {
	//输出剩余配额重置时间和当前时间
	// fmt.Printf("channel #%d, remaining tokens reset time: %v, current time: %v \n", channel.Id, time.Unix(q.ResetTimeOTPM, 0).Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02 15:04:05"))

	if channel.GetModelType(requestModel) == ModelTypeClaude {
		return atomic.LoadInt64(&q.ResetTimeOTPM) <= time.Now().Unix()
	} else {
		return atomic.LoadInt64(&q.ResetTimeTPM) <= time.Now().Unix()
	}
}

// 扣除令牌
func (q *ChannelQuota) DeductTokens(channel *Channel, requestModel string, tokens int64) {
	if channel.GetModelType(requestModel) == ModelTypeClaude {
		atomic.AddInt64(&q.RemainingOTPM, -tokens)
	} else {
		atomic.AddInt64(&q.RemainingTPM, -tokens)
	}
}

// 扣除请求数
func (q *ChannelQuota) DeductRPM(rpm int64) {
	atomic.AddInt64(&q.RemainingRPM, -rpm)
}

// 预估下次重置时间
func (q *ChannelQuota) UpdateResetTime(channel *Channel, requestModel string, resetTime int64) {
	if channel.GetModelType(requestModel) == ModelTypeClaude {
		atomic.StoreInt64(&q.ResetTimeOTPM, resetTime)
	} else {
		atomic.StoreInt64(&q.ResetTimeTPM, resetTime)
	}
}

// 剩余令牌
func (q *ChannelQuota) RemainingTokens(channel *Channel, requestModel string) int64 {
	if channel.GetModelType(requestModel) == ModelTypeClaude {
		return atomic.LoadInt64(&q.RemainingOTPM)
	} else {
		return atomic.LoadInt64(&q.RemainingTPM)
	}
}

// 剩余请求数
func (q *ChannelQuota) RemainingRequests() int64 {
	return atomic.LoadInt64(&q.RemainingRPM)
}

// 总的请求数
func (q *ChannelQuota) TotalRequests() int64 {
	return atomic.LoadInt64(&q.RPM)
}

// 剩余令牌重置时间
func (q *ChannelQuota) RemainingTokensResetTime(channel *Channel, requestModel string) int64 {
	if channel.GetModelType(requestModel) == ModelTypeClaude {
		return atomic.LoadInt64(&q.ResetTimeOTPM)
	} else {
		return atomic.LoadInt64(&q.ResetTimeTPM)
	}
}

// ChannelQuotaMemoryStore 使用内存存储渠道配额信息
type ChannelQuotaMemoryStore struct {
	store map[string]map[string][]*ChannelQuota
	mutex sync.RWMutex
}

var (
	channelQuotaStore = &ChannelQuotaMemoryStore{
		store: make(map[string]map[string][]*ChannelQuota),
	}
)

// InitChannelQuotaStore 初始化渠道配额存储
func InitChannelQuotaStore() {
	channelQuotaStore.mutex.Lock()
	defer channelQuotaStore.mutex.Unlock()

	newChannelId2channel := make(map[int]*Channel)
	var channels []*Channel
	DB.Where("status = ?", ChannelStatusEnabled).Find(&channels)
	for _, channel := range channels {
		newChannelId2channel[channel.Id] = channel
	}
	var abilities []*Ability
	DB.Find(&abilities)
	groups := make(map[string]bool)
	for _, ability := range abilities {
		groups[ability.Group] = true
	}

	for group := range groups {
		channelQuotaStore.store[group] = make(map[string][]*ChannelQuota)
	}
	for _, channel := range channels {
		groups := strings.Split(channel.Group, ",")
		for _, group := range groups {
			models := strings.Split(channel.Models, ",")
			for _, model := range models {
				if _, ok := channelQuotaStore.store[group][model]; !ok {
					channelQuotaStore.store[group][model] = make([]*ChannelQuota, 0)
				}
				channelQuotaStore.store[group][model] = append(channelQuotaStore.store[group][model], initChannelQuotaData(int64(channel.Id), channel.Type, model))
			}
		}
	}
}

// 从数据库增量更新渠道配额信息
func IncrementChannelQuotaStoreFromDB() {
	channelQuotaStore.mutex.Lock()
	defer channelQuotaStore.mutex.Unlock()

	newChannelId2channel := make(map[int]*Channel)
	var channels []*Channel
	DB.Where("status = ?", ChannelStatusEnabled).Find(&channels)
	for _, channel := range channels {
		newChannelId2channel[channel.Id] = channel
	}
	var abilities []*Ability
	DB.Find(&abilities)
	groups := make(map[string]bool)
	for _, ability := range abilities {
		groups[ability.Group] = true
	}

	// 增量更新场景: 1.新加渠道 2.禁用渠道 3.启用渠道

	// 遍历现有的渠道配额，处理禁用渠道的情况
	for group, modelMap := range channelQuotaStore.store {
		for model, quotas := range modelMap {
			updatedQuotas := make([]*ChannelQuota, 0)
			for _, quota := range quotas {
				// 检查渠道是否还存在且启用
				if _, exists := newChannelId2channel[int(quota.ChannelId)]; exists {
					// 渠道仍然存在且启用，保留该配额
					updatedQuotas = append(updatedQuotas, quota)
				}
			}
			channelQuotaStore.store[group][model] = updatedQuotas
		}
	}

	// 处理新增渠道和启用渠道的情况
	for _, channel := range channels {
		channelGroups := strings.Split(channel.Group, ",")
		models := strings.Split(channel.Models, ",")

		for _, group := range channelGroups {
			// 确保该用户组存在于store中
			if _, ok := channelQuotaStore.store[group]; !ok {
				channelQuotaStore.store[group] = make(map[string][]*ChannelQuota)
			}

			for _, model := range models {
				if _, ok := channelQuotaStore.store[group][model]; !ok {
					channelQuotaStore.store[group][model] = make([]*ChannelQuota, 0)
				}

				// 检查该渠道是否已经存在于配额列表中
				exists := false
				for _, quota := range channelQuotaStore.store[group][model] {
					if quota.ChannelId == int64(channel.Id) {
						exists = true
						break
					}
				}

				// 如果渠道不存在，则添加新的配额
				if !exists {
					channelQuotaStore.store[group][model] = append(
						channelQuotaStore.store[group][model],
						initChannelQuotaData(int64(channel.Id), channel.Type, model),
					)
				}
			}
		}
	}
}

// GetChannelQuota 从内存中获取渠道配额信息
func GetChannelQuota(userGroup, modelName string, channelId int64) (*ChannelQuota, error) {
	channelQuotaStore.mutex.RLock()
	defer channelQuotaStore.mutex.RUnlock()

	modelMap, exists := channelQuotaStore.store[userGroup]
	if !exists {
		return nil, nil
	}

	quotas, exists := modelMap[modelName]
	if !exists {
		return nil, nil
	}

	for _, quota := range quotas {
		if quota.ChannelId == channelId {
			return quota, nil
		}
	}
	return nil, nil
}

// UpdateChannelQuota 更新内存中的渠道配额信息
func UpdateChannelQuota(userGroup, modelName string, quota *ChannelQuota) error {
	channelQuotaStore.mutex.Lock()
	defer channelQuotaStore.mutex.Unlock()

	// 如果配额为空，则删除该渠道的配额信息
	if quota == nil {
		if modelMap, exists := channelQuotaStore.store[userGroup]; exists {
			if quotas, exists := modelMap[modelName]; exists {
				// 找到并删除对应的配额
				for i, q := range quotas {
					if q.ChannelId == quota.ChannelId {
						quotas = append(quotas[:i], quotas[i+1:]...)
						modelMap[modelName] = quotas
						channelQuotaStore.store[userGroup] = modelMap
						return nil
					}
				}
			}
		}
		return nil
	}

	// 确保用户组和模型名称的map存在
	if _, exists := channelQuotaStore.store[userGroup]; !exists {
		channelQuotaStore.store[userGroup] = make(map[string][]*ChannelQuota)
	}
	if _, exists := channelQuotaStore.store[userGroup][modelName]; !exists {
		channelQuotaStore.store[userGroup][modelName] = make([]*ChannelQuota, 0)
	}

	// 更新或添加配额信息
	quotas := channelQuotaStore.store[userGroup][modelName]
	for i, q := range quotas {
		if q.ChannelId == quota.ChannelId {
			quotas[i] = quota
			channelQuotaStore.store[userGroup][modelName] = quotas
			return nil
		}
	}
	channelQuotaStore.store[userGroup][modelName] = append(quotas, quota)
	return nil
}

// PreDeductChannelQuota 预扣除渠道配额
func PreDeductChannelQuota(userGroup, modelName string, channelId int64, tokens int64, rpm int64) error {
	channelQuotaStore.mutex.Lock()
	defer channelQuotaStore.mutex.Unlock()

	modelMap, exists := channelQuotaStore.store[userGroup]
	if !exists {
		return errors.New("user group not found")
	}

	quotas, exists := modelMap[modelName]
	if !exists {
		return errors.New("model not found")
	}

	var quota *ChannelQuota
	for _, q := range quotas {
		if q.ChannelId == channelId {
			quota = q
			break
		}
	}

	if quota == nil {
		return errors.New("channel quota not found")
	}

	// 检查配额是否足够
	if quota.RemainingOTPM < tokens || quota.RemainingRPM < rpm {
		return errors.New("insufficient quota")
	}

	// 预扣除配额
	quota.RemainingOTPM -= tokens
	quota.RemainingRPM -= rpm
	return nil
}

// GetChannelQuotasByModel 获取指定用户组和模型的所有渠道配额
func GetChannelQuotasByModel(userGroup, modelName string) []*ChannelQuota {
	channelQuotaStore.mutex.RLock()
	defer channelQuotaStore.mutex.RUnlock()

	modelMap, exists := channelQuotaStore.store[userGroup]
	if !exists {
		return nil
	}

	quotas, exists := modelMap[modelName]
	if !exists {
		return nil
	}

	return quotas
}

// 更数据库channels表KeyLevel
func UpdateChannelKeyLevel(channelId int64, channelType int, rpm int64, tpm int64, requestModel string) {
	keyLevel := DetermineAccountLevel(channelType, rpm, tpm, requestModel)
	DB.Model(&Channel{}).Where("id = ? and key_level < ?", channelId, keyLevel).Update("key_level", keyLevel)
}

// 根据配额信息确定账户等级
func DetermineAccountLevel(channelType int, rpm int64, tpm int64, requestModel string) int {
	if channelType == channeltype.Anthropic || (channelType == channeltype.Custom && strings.HasPrefix(requestModel, "claude-")) {
		switch rpm {
		case 50:
			return 1
		case 1000:
			return 2
		case 2000:
			return 3
		case 4000:
			return 4
		default:
			if rpm > 4000 {
				return 4
			} else {
				return 1
			}
		}
	} else if channelType == channeltype.OpenAI {
		if requestModel == "gpt-4o" {
			switch rpm {
			case 500:
				return 1
			case 5000:
				if tpm == 450000 {
					return 2
				} else {
					return 3
				}
			case 10000:
				if tpm == 2000000 {
					return 4
				} else {
					return 5
				}
			default:
				if rpm > 10000 {
					return 5
				} else {
					return 1
				}
			}
		}
		return 1
	}
	return 1
}
