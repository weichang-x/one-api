package config

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/songquanpeng/one-api/common/env"

	"github.com/google/uuid"
)

var SystemName = "One API"
var ServerAddress = "http://localhost:3000"
var Footer = ""
var Logo = ""
var TopUpLink = ""
var ChatLink = ""
var QuotaPerUnit = 500 * 1000.0 // $0.002 / 1K tokens
var DisplayInCurrencyEnabled = true
var DisplayTokenStatEnabled = true

// Any options with "Secret", "Token" in its key won't be return by GetOptions

var SessionSecret = uuid.New().String()

var OptionMap map[string]string
var OptionMapRWMutex sync.RWMutex

var ItemsPerPage = 10
var MaxRecentItems = 100

var PasswordLoginEnabled = true
var PasswordRegisterEnabled = true
var EmailVerificationEnabled = false
var GitHubOAuthEnabled = false
var OidcEnabled = false
var WeChatAuthEnabled = false
var TurnstileCheckEnabled = false
var RegisterEnabled = true

var EmailDomainRestrictionEnabled = false
var EmailDomainWhitelist = []string{
	"gmail.com",
	"163.com",
	"126.com",
	"qq.com",
	"outlook.com",
	"hotmail.com",
	"icloud.com",
	"yahoo.com",
	"foxmail.com",
}

var DebugEnabled = strings.ToLower(os.Getenv("DEBUG")) == "true"
var DebugSQLEnabled = strings.ToLower(os.Getenv("DEBUG_SQL")) == "true"
var MemoryCacheEnabled = strings.ToLower(os.Getenv("MEMORY_CACHE_ENABLED")) == "true"

var LogConsumeEnabled = true

var SMTPServer = ""
var SMTPPort = 587
var SMTPAccount = ""
var SMTPFrom = ""
var SMTPToken = ""

var GitHubClientId = ""
var GitHubClientSecret = ""

var LarkClientId = ""
var LarkClientSecret = ""

var OidcClientId = ""
var OidcClientSecret = ""
var OidcWellKnown = ""
var OidcAuthorizationEndpoint = ""
var OidcTokenEndpoint = ""
var OidcUserinfoEndpoint = ""

var WeChatServerAddress = ""
var WeChatServerToken = ""
var WeChatAccountQRCodeImageURL = ""

var MessagePusherAddress = ""
var MessagePusherToken = ""

var TurnstileSiteKey = ""
var TurnstileSecretKey = ""

var QuotaForNewUser int64 = 0
var QuotaForInviter int64 = 0
var QuotaForInvitee int64 = 0
var ChannelDisableThreshold = 5.0
var AutomaticDisableChannelEnabled = false
var AutomaticEnableChannelEnabled = false
var QuotaRemindThreshold int64 = 1000
var PreConsumedQuota int64 = 500
var ApproximateTokenEnabled = false
var RetryTimes = env.Int("RETRY_TIMES", 3)         // 重试次数
var RetryInterval = env.Int("RETRY_INTERVAL", 100) // 重试间隔（毫秒）

var RootUserEmail = ""

var IsMasterNode = os.Getenv("NODE_TYPE") != "slave"

var requestInterval, _ = strconv.Atoi(os.Getenv("POLLING_INTERVAL"))
var RequestInterval = time.Duration(requestInterval) * time.Second

var SyncFrequency = env.Int("SYNC_FREQUENCY", 10*60) // unit is second

// 通道配额阈值配置
var MinTokenConsumptionThreshold = env.Int("MIN_TOKEN_CONSUMPTION_THRESHOLD", 1000) // 最小消耗token阈值
var MaxConcurrentRequestsLimit = env.Int("MAX_CONCURRENT_REQUESTS_LIMIT", 10)       // 最大并发量

// Anthropic通道类型并发请求限制配置
var AnthropicConcurrentRequestsLimit = env.Int("ANTHROPIC_CONCURRENT_REQUESTS_LIMIT", 3)

// Channel type specific concurrent request limits
var ChannelTypeConcurrentLimits = make(map[int]int)

// GetChannelTypeConcurrentLimit returns the concurrent request limit for a specific channel type
// If no specific limit is set for the channel type, returns the default MaxConcurrentRequestsLimit
func GetChannelTypeConcurrentLimit(channelType int) int {
	if limit, exists := ChannelTypeConcurrentLimits[channelType]; exists {
		return limit
	}
	return MaxConcurrentRequestsLimit
}

// SetChannelTypeConcurrentLimit sets the concurrent request limit for a specific channel type
func SetChannelTypeConcurrentLimit(channelType int, limit int) {
	ChannelTypeConcurrentLimits[channelType] = limit
}

// 请求队列配置
var RequestQueueTimeout = env.Int("REQUEST_QUEUE_TIMEOUT", 30)        // 请求队列超时时间（秒）
var RequestQueueMaxLength = env.Int("REQUEST_QUEUE_MAX_LENGTH", 1000) // 请求队列最大长度

// 通道策略选择配置
var ChannelSelectorStrategyModels = env.String("CHANNEL_SELECTOR_STRATEGY_MODELS", "")

// 熔断器配置
var CircuitBreakerFailureThreshold = env.Int("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5)
var CircuitBreakerErrorRateThreshold = env.Float64("CIRCUIT_BREAKER_ERROR_RATE_THRESHOLD", 0.6)
var CircuitBreakerSlowCallDuration = env.Int("CIRCUIT_BREAKER_SLOW_CALL_DURATION", 2000)
var CircuitBreakerCooldownPeriod = env.Int("CIRCUIT_BREAKER_COOLDOWN_PERIOD", 30)
var CircuitBreakerHalfOpenMaxCalls = env.Int("CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS", 3)

var BatchUpdateEnabled = false
var BatchUpdateInterval = env.Int("BATCH_UPDATE_INTERVAL", 5)

var RelayTimeout = env.Int("RELAY_TIMEOUT", 0) // unit is second

var GeminiSafetySetting = env.String("GEMINI_SAFETY_SETTING", "BLOCK_NONE")

var Theme = env.String("THEME", "default")
var ValidThemes = map[string]bool{
	"default": true,
	"berry":   true,
	"air":     true,
}

// All duration's unit is seconds
// Shouldn't larger then RateLimitKeyExpirationDuration
var (
	GlobalApiRateLimitNum            = env.Int("GLOBAL_API_RATE_LIMIT", 240)
	GlobalApiRateLimitDuration int64 = 3 * 60

	GlobalWebRateLimitNum            = env.Int("GLOBAL_WEB_RATE_LIMIT", 120)
	GlobalWebRateLimitDuration int64 = 3 * 60

	UploadRateLimitNum            = 10
	UploadRateLimitDuration int64 = 60

	DownloadRateLimitNum            = 10
	DownloadRateLimitDuration int64 = 60

	CriticalRateLimitNum            = 20
	CriticalRateLimitDuration int64 = 20 * 60
)

var RateLimitKeyExpirationDuration = 20 * time.Minute

var EnableMetric = env.Bool("ENABLE_METRIC", false)
var MetricQueueSize = env.Int("METRIC_QUEUE_SIZE", 10)
var MetricSuccessRateThreshold = env.Float64("METRIC_SUCCESS_RATE_THRESHOLD", 0.8)
var MetricSuccessChanSize = env.Int("METRIC_SUCCESS_CHAN_SIZE", 1024)
var MetricFailChanSize = env.Int("METRIC_FAIL_CHAN_SIZE", 128)

var InitialRootToken = os.Getenv("INITIAL_ROOT_TOKEN")

var InitialRootAccessToken = os.Getenv("INITIAL_ROOT_ACCESS_TOKEN")

var GeminiVersion = env.String("GEMINI_VERSION", "v1")

var OnlyOneLogFile = env.Bool("ONLY_ONE_LOG_FILE", false)

var RelayProxy = env.String("RELAY_PROXY", "")
var UserContentRequestProxy = env.String("USER_CONTENT_REQUEST_PROXY", "")
var UserContentRequestTimeout = env.Int("USER_CONTENT_REQUEST_TIMEOUT", 30)

var EnforceIncludeUsage = env.Bool("ENFORCE_INCLUDE_USAGE", false)
