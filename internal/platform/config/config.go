package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const ProductionDefault = "production-default"

var allowedProfiles = []string{"local", "test", ProductionDefault}

type Config struct {
	Profile       string              `yaml:"profile"`
	Namespace     string              `yaml:"namespace"`
	HTTP          HTTPConfig          `yaml:"http"`
	Endpoints     EndpointConfig      `yaml:"endpoints"`
	Shutdown      ShutdownConfig      `yaml:"shutdown"`
	Postgres      PostgresConfig      `yaml:"postgres"`
	Redis         RedisConfig         `yaml:"redis"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	Engine        EngineConfig        `yaml:"engine"`
	Scheduler     SchedulerConfig     `yaml:"scheduler"`
	Worker        WorkerConfig        `yaml:"worker"`
	Sandbox       SandboxConfig       `yaml:"sandbox"`
	Outbox        OutboxConfig        `yaml:"outbox"`
	Cache         CacheConfig         `yaml:"cache"`
	Migrations    MigrationConfig     `yaml:"migrations"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type HTTPConfig struct {
	ControlPlaneAddress   string   `yaml:"control_plane_address"`
	BuiltinWorkerAddress  string   `yaml:"builtin_worker_address"`
	SandboxWorkerAddress  string   `yaml:"sandbox_worker_address"`
	SandboxRuntimeAddress string   `yaml:"sandbox_runtime_address"`
	ReadHeaderTimeout     Duration `yaml:"read_header_timeout"`
	IdleTimeout           Duration `yaml:"idle_timeout"`
}

type EndpointConfig struct {
	ControlPlaneURL string `yaml:"control_plane_url"`
}

type ShutdownConfig struct {
	Timeout Duration `yaml:"timeout"`
}

type PostgresConfig struct {
	DSN                           string   `yaml:"dsn"`
	Schema                        string   `yaml:"schema"`
	PoolMin                       int32    `yaml:"pool_min"`
	PoolMax                       int32    `yaml:"pool_max"`
	ServerMaxConnections          int32    `yaml:"server_max_connections"`
	ExpectedControlPlaneInstances int32    `yaml:"expected_control_plane_instances"`
	StatementTimeout              Duration `yaml:"statement_timeout"`
	LockTimeout                   Duration `yaml:"lock_timeout"`
	IdleInTransactionTimeout      Duration `yaml:"idle_in_transaction_timeout"`
}

type RedisConfig struct {
	Scheduling RedisEndpointConfig `yaml:"scheduling"`
	Cache      RedisEndpointConfig `yaml:"cache"`
}

type RedisEndpointConfig struct {
	Address          string   `yaml:"address"`
	Password         string   `yaml:"password"`
	DB               int      `yaml:"db"`
	KeyPrefix        string   `yaml:"key_prefix"`
	OperationTimeout Duration `yaml:"operation_timeout"`
	MaxRetries       int      `yaml:"max_retries"`
	EvictionPolicy   string   `yaml:"eviction_policy"`
}

type KafkaConfig struct {
	Brokers               []string    `yaml:"brokers"`
	TopicPrefix           string      `yaml:"topic_prefix"`
	ReplicationFactor     int         `yaml:"replication_factor"`
	MinISR                int         `yaml:"min_isr"`
	BrokerMaxMessageBytes int         `yaml:"broker_max_message_bytes"`
	EnvelopeMaxBytes      int         `yaml:"envelope_max_bytes"`
	MaxManualReplayWindow Duration    `yaml:"max_manual_replay_window"`
	TaskMaxPollRecords    int         `yaml:"task_max_poll_records"`
	RuntimeMaxPollRecords int         `yaml:"runtime_max_poll_records"`
	SessionTimeout        Duration    `yaml:"session_timeout"`
	HeartbeatInterval     Duration    `yaml:"heartbeat_interval"`
	MaxPollInterval       Duration    `yaml:"max_poll_interval"`
	RequestTimeout        Duration    `yaml:"request_timeout"`
	DeliveryTimeout       Duration    `yaml:"delivery_timeout"`
	Linger                Duration    `yaml:"linger"`
	BatchBytes            int         `yaml:"batch_bytes"`
	Topics                KafkaTopics `yaml:"topics"`
}

type KafkaTopics struct {
	BuiltinTask  KafkaTopicConfig `yaml:"builtin_task"`
	SandboxTask  KafkaTopicConfig `yaml:"sandbox_task"`
	RuntimeEvent KafkaTopicConfig `yaml:"runtime_event"`
	DLQ          KafkaTopicConfig `yaml:"dlq"`
}

type KafkaTopicConfig struct {
	Name       string   `yaml:"name"`
	Partitions int      `yaml:"partitions"`
	Retention  Duration `yaml:"retention"`
}

type EngineConfig struct {
	MaxInflight int `yaml:"max_inflight"`
}

type SchedulerConfig struct {
	RedisCandidateBatch         int      `yaml:"redis_candidate_batch"`
	AdmissionConcurrency        int      `yaml:"admission_concurrency"`
	CapacityCalibrationInterval Duration `yaml:"capacity_calibration_interval"`
	ReadyReconcileInterval      Duration `yaml:"ready_reconcile_interval"`
	ReconcileLease              Duration `yaml:"reconcile_lease"`
	ReservationTTL              Duration `yaml:"reservation_ttl"`
	TopicQueueBuffer            Duration `yaml:"topic_queue_buffer"`
	TopicEWMAAlpha              float64  `yaml:"topic_ewma_alpha"`
	BuiltinMinQueue             int      `yaml:"builtin_min_queue"`
	BuiltinMaxQueue             int      `yaml:"builtin_max_queue"`
	SandboxMinQueue             int      `yaml:"sandbox_min_queue"`
	SandboxMaxQueue             int      `yaml:"sandbox_max_queue"`
	MemoryHighWatermark         float64  `yaml:"memory_high_watermark"`
	MemoryResumeWatermark       float64  `yaml:"memory_resume_watermark"`
	IdlePoll                    Duration `yaml:"idle_poll"`
	IdlePollMax                 Duration `yaml:"idle_poll_max"`
}

type WorkerConfig struct {
	BuiltinSlots                  int        `yaml:"builtin_slots"`
	SandboxSlots                  int        `yaml:"sandbox_slots"`
	ExpectedBuiltinConsumers      int        `yaml:"expected_builtin_consumers"`
	ExpectedSandboxConsumers      int        `yaml:"expected_sandbox_consumers"`
	ExpectedRuntimeEventConsumers int        `yaml:"expected_runtime_event_consumers"`
	TaskPollLimit                 int        `yaml:"task_poll_limit"`
	HeartbeatInterval             Duration   `yaml:"heartbeat_interval"`
	LeaseDuration                 Duration   `yaml:"lease_duration"`
	LostAfter                     Duration   `yaml:"lost_after"`
	RecoveryScannerInterval       Duration   `yaml:"recovery_scanner_interval"`
	MaxRecoveries                 int        `yaml:"max_recoveries"`
	ClaimTimeout                  Duration   `yaml:"claim_timeout"`
	CompleteTimeout               Duration   `yaml:"complete_timeout"`
	RecoveryBackoff               []Duration `yaml:"recovery_backoff"`
}

// SandboxConfig is deployment configuration, not workflow configuration. The
// fixed resource envelope remains in sandbox.DefaultProfile so neither a
// project nor an Agent can relax it.
type SandboxConfig struct {
	Image        string `yaml:"image"`
	Runtime      string `yaml:"runtime"`
	Command      string `yaml:"command"`
	RuntimeURL   string `yaml:"runtime_url"`
	RuntimeToken string `yaml:"runtime_token"`
}

type OutboxConfig struct {
	Batch                   int      `yaml:"batch"`
	ActivePoll              Duration `yaml:"active_poll"`
	IdlePollMax             Duration `yaml:"idle_poll_max"`
	ClaimLease              Duration `yaml:"claim_lease"`
	PublishConcurrency      int      `yaml:"publish_concurrency"`
	PublishedRetention      Duration `yaml:"published_retention"`
	InboxRetention          Duration `yaml:"inbox_retention"`
	CleanupInterval         Duration `yaml:"cleanup_interval"`
	RetryTimerInterval      Duration `yaml:"retry_timer_interval"`
	DeadlineScannerInterval Duration `yaml:"deadline_scanner_interval"`
	ReconcilerInterval      Duration `yaml:"reconciler_interval"`
	ScanBatch               int      `yaml:"scan_batch"`
}

type CacheConfig struct {
	ExecutionSnapshotTTL    Duration `yaml:"execution_snapshot_ttl"`
	ActiveRunContextTTL     Duration `yaml:"active_run_context_ttl"`
	TerminalRunContextTTL   Duration `yaml:"terminal_run_context_ttl"`
	ActiveRunReadModelTTL   Duration `yaml:"active_run_read_model_ttl"`
	TerminalRunReadModelTTL Duration `yaml:"terminal_run_read_model_ttl"`
	DefinitionNegativeTTL   Duration `yaml:"definition_negative_ttl"`
}

type MigrationConfig struct {
	Directory   string   `yaml:"directory"`
	LockTimeout Duration `yaml:"lock_timeout"`
}

type ObservabilityConfig struct {
	LogLevel string `yaml:"log_level"`
}

type LoadOptions struct {
	Directory string
	Profile   string
	LookupEnv func(string) (string, bool)
}

func Load(options LoadOptions) (Config, error) {
	lookup := options.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	profile := strings.TrimSpace(options.Profile)
	if profile == "" {
		if value, ok := lookup("EVALFROG_PROFILE"); ok {
			profile = strings.TrimSpace(value)
		}
	}
	if profile == "" {
		profile = "local"
	}
	if !slices.Contains(allowedProfiles, profile) {
		return Config{}, fmt.Errorf("unsupported profile %q", profile)
	}
	directory := options.Directory
	if directory == "" {
		if value, ok := lookup("EVALFROG_CONFIG_DIR"); ok {
			directory = value
		}
	}
	if directory == "" {
		directory = "configs"
	}

	base, err := readYAMLMap(filepath.Join(directory, ProductionDefault+".yaml"))
	if err != nil {
		return Config{}, err
	}
	merged := base
	if profile != ProductionDefault {
		override, readErr := readYAMLMap(filepath.Join(directory, profile+".yaml"))
		if readErr != nil {
			return Config{}, readErr
		}
		merged = mergeMaps(base, override)
	}
	encoded, err := yaml.Marshal(merged)
	if err != nil {
		return Config{}, fmt.Errorf("marshal merged profile: %w", err)
	}
	var result Config
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	if err := decoder.Decode(&result); err != nil {
		return Config{}, fmt.Errorf("decode profile %q: %w", profile, err)
	}
	if result.Profile != profile {
		return Config{}, fmt.Errorf("profile mismatch: selected %q but merged config declares %q", profile, result.Profile)
	}
	applyEnvironment(&result, lookup)
	if err := result.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate profile %q: %w", profile, err)
	}
	return result, nil
}

func readYAMLMap(path string) (map[string]any, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var value map[string]any
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}
	if err := ensureSingleDocument(decoder, path); err != nil {
		return nil, err
	}
	return value, nil
}

func ensureSingleDocument(decoder *yaml.Decoder, path string) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("decode trailing document %s: %w", path, err)
	}
	return fmt.Errorf("config %s contains multiple YAML documents", path)
}

func mergeMaps(base, override map[string]any) map[string]any {
	result := make(map[string]any, len(base)+len(override))
	for key, value := range base {
		result[key] = value
	}
	for key, value := range override {
		baseMap, baseOK := result[key].(map[string]any)
		overrideMap, overrideOK := value.(map[string]any)
		if baseOK && overrideOK {
			result[key] = mergeMaps(baseMap, overrideMap)
			continue
		}
		result[key] = value
	}
	return result
}

func applyEnvironment(config *Config, lookup func(string) (string, bool)) {
	setString := func(name string, target *string) {
		if value, ok := lookup(name); ok && strings.TrimSpace(value) != "" {
			*target = strings.TrimSpace(value)
		}
	}
	setString("EVALFROG_HTTP_ADDRESS", &config.HTTP.ControlPlaneAddress)
	setString("EVALFROG_CONTROL_PLANE_URL", &config.Endpoints.ControlPlaneURL)
	setString("EVALFROG_POSTGRES_DSN", &config.Postgres.DSN)
	setString("EVALFROG_SCHEDULING_REDIS_ADDRESS", &config.Redis.Scheduling.Address)
	setString("EVALFROG_CACHE_REDIS_ADDRESS", &config.Redis.Cache.Address)
	setString("EVALFROG_MIGRATIONS_DIR", &config.Migrations.Directory)
	setString("EVALFROG_SANDBOX_IMAGE", &config.Sandbox.Image)
	setString("EVALFROG_SANDBOX_RUNTIME", &config.Sandbox.Runtime)
	setString("EVALFROG_SANDBOX_COMMAND", &config.Sandbox.Command)
	setString("EVALFROG_SANDBOX_RUNTIME_URL", &config.Sandbox.RuntimeURL)
	setString("EVALFROG_SANDBOX_RUNTIME_TOKEN", &config.Sandbox.RuntimeToken)
	if value, ok := lookup("EVALFROG_KAFKA_BROKERS"); ok && strings.TrimSpace(value) != "" {
		parts := strings.Split(value, ",")
		config.Kafka.Brokers = config.Kafka.Brokers[:0]
		for _, part := range parts {
			if broker := strings.TrimSpace(part); broker != "" {
				config.Kafka.Brokers = append(config.Kafka.Brokers, broker)
			}
		}
	}
}

func (config Config) Validate() error {
	var problems []error
	add := func(condition bool, format string, args ...any) {
		if condition {
			problems = append(problems, fmt.Errorf(format, args...))
		}
	}

	add(!slices.Contains(allowedProfiles, config.Profile), "profile must be one of %v", allowedProfiles)
	add(config.Namespace != config.Profile, "namespace %q must equal profile %q", config.Namespace, config.Profile)
	add(config.Shutdown.Timeout.Duration() <= 0, "shutdown.timeout must be positive")
	add(config.HTTP.ReadHeaderTimeout.Duration() <= 0, "http.read_header_timeout must be positive")
	add(config.HTTP.IdleTimeout.Duration() <= 0, "http.idle_timeout must be positive")
	for name, address := range map[string]string{
		"control_plane_address":   config.HTTP.ControlPlaneAddress,
		"builtin_worker_address":  config.HTTP.BuiltinWorkerAddress,
		"sandbox_worker_address":  config.HTTP.SandboxWorkerAddress,
		"sandbox_runtime_address": config.HTTP.SandboxRuntimeAddress,
	} {
		_, err := ParsePort(address)
		add(err != nil, "http.%s is invalid: %v", name, err)
	}
	controlPlaneURL, endpointErr := url.ParseRequestURI(config.Endpoints.ControlPlaneURL)
	add(endpointErr != nil || controlPlaneURL.Scheme == "" || controlPlaneURL.Host == "", "endpoints.control_plane_url must be an absolute HTTP URL")
	add(config.Postgres.DSN == "", "postgres.dsn is required")
	add(!validIdentifier(config.Postgres.Schema), "postgres.schema must be a safe SQL identifier")
	profileToken := strings.ReplaceAll(config.Profile, "-", "_")
	add(!strings.Contains(config.Postgres.Schema, profileToken), "postgres.schema must be isolated for profile %q", config.Profile)
	add(config.Postgres.PoolMin < 0 || config.Postgres.PoolMax <= 0 || config.Postgres.PoolMin > config.Postgres.PoolMax, "postgres pool bounds are invalid")
	allowedConnections := int32(math.Floor(float64(config.Postgres.ServerMaxConnections) * 0.70))
	usedConnections := config.Postgres.PoolMax * config.Postgres.ExpectedControlPlaneInstances
	add(config.Postgres.ServerMaxConnections <= 0 || config.Postgres.ExpectedControlPlaneInstances <= 0, "postgres capacity values must be positive")
	add(usedConnections > allowedConnections, "postgres pool capacity %d exceeds 70%% safety budget %d", usedConnections, allowedConnections)

	validateRedis := func(name string, value RedisEndpointConfig) {
		add(value.Address == "", "redis.%s.address is required", name)
		_, err := ParsePort(value.Address)
		add(err != nil, "redis.%s.address is invalid: %v", name, err)
		add(value.DB < 0, "redis.%s.db cannot be negative", name)
		add(value.OperationTimeout.Duration() <= 0, "redis.%s.operation_timeout must be positive", name)
		add(!strings.Contains(value.KeyPrefix, ":"+config.Profile+":"), "redis.%s.key_prefix must be isolated for profile %q", name, config.Profile)
	}
	validateRedis("scheduling", config.Redis.Scheduling)
	validateRedis("cache", config.Redis.Cache)
	add(config.Redis.Scheduling.EvictionPolicy != "noeviction", "scheduling Redis must use noeviction")
	if config.Redis.Scheduling.Address == config.Redis.Cache.Address && config.Redis.Scheduling.DB == config.Redis.Cache.DB {
		add(config.Profile != "local", "only local profile may share Redis endpoints")
		add(config.Redis.Cache.EvictionPolicy != "noeviction", "shared local Redis must use noeviction for all keys")
	}

	add(len(config.Kafka.Brokers) == 0, "kafka.brokers is required")
	for _, broker := range config.Kafka.Brokers {
		_, err := ParsePort(broker)
		add(err != nil, "Kafka broker %q is invalid: %v", broker, err)
	}
	add(!strings.Contains(config.Kafka.TopicPrefix, config.Profile), "kafka.topic_prefix must be isolated for profile %q", config.Profile)
	add(config.Kafka.ReplicationFactor < config.Kafka.MinISR || config.Kafka.MinISR <= 0, "kafka replication_factor must be >= min_isr > 0")
	add(config.Kafka.EnvelopeMaxBytes <= 0 || config.Kafka.BrokerMaxMessageBytes < config.Kafka.EnvelopeMaxBytes, "Kafka message size limits are invalid")
	validateTopic := func(name string, topic KafkaTopicConfig) {
		add(topic.Name == "", "kafka topic %s name is required", name)
		add(topic.Partitions <= 0, "kafka topic %s partitions must be positive", name)
		add(topic.Retention.Duration() <= 0, "kafka topic %s retention must be positive", name)
	}
	validateTopic("builtin_task", config.Kafka.Topics.BuiltinTask)
	validateTopic("sandbox_task", config.Kafka.Topics.SandboxTask)
	validateTopic("runtime_event", config.Kafka.Topics.RuntimeEvent)
	validateTopic("dlq", config.Kafka.Topics.DLQ)
	add(config.Kafka.Topics.BuiltinTask.Partitions < config.Worker.ExpectedBuiltinConsumers, "builtin task partitions must cover expected consumers")
	add(config.Kafka.Topics.SandboxTask.Partitions < config.Worker.ExpectedSandboxConsumers, "sandbox task partitions must cover expected consumers")
	add(config.Kafka.Topics.RuntimeEvent.Partitions < config.Worker.ExpectedRuntimeEventConsumers, "runtime event partitions must cover expected consumers")
	add(config.Engine.MaxInflight <= 0 || int32(config.Engine.MaxInflight) >= config.Postgres.PoolMax, "engine.max_inflight must be positive and leave PostgreSQL pool capacity for other control-plane modules")

	add(config.Scheduler.RedisCandidateBatch <= 0 || config.Scheduler.AdmissionConcurrency <= 0, "scheduler batch and concurrency values must be positive")
	add(config.Scheduler.CapacityCalibrationInterval.Duration() <= 0 || config.Scheduler.ReadyReconcileInterval.Duration() < config.Scheduler.CapacityCalibrationInterval.Duration(), "scheduler reconciliation interval must cover the positive capacity calibration interval")
	add(config.Scheduler.ReconcileLease.Duration() <= 0 || config.Scheduler.ReconcileLease.Duration() >= config.Scheduler.ReadyReconcileInterval.Duration(), "scheduler reconcile lease must be positive and shorter than reconciliation interval")
	add(config.Scheduler.ReservationTTL.Duration() <= 0, "scheduler reservation TTL must be positive")
	add(config.Scheduler.TopicQueueBuffer.Duration() <= 0 || config.Scheduler.TopicEWMAAlpha <= 0 || config.Scheduler.TopicEWMAAlpha > 1, "scheduler topic queue buffer and EWMA alpha are invalid")
	add(config.Scheduler.BuiltinMinQueue <= 0 || config.Scheduler.BuiltinMaxQueue < config.Scheduler.BuiltinMinQueue || config.Scheduler.SandboxMinQueue <= 0 || config.Scheduler.SandboxMaxQueue < config.Scheduler.SandboxMinQueue, "scheduler topic queue bounds are invalid")
	add(config.Scheduler.MemoryResumeWatermark <= 0 || config.Scheduler.MemoryHighWatermark <= config.Scheduler.MemoryResumeWatermark || config.Scheduler.MemoryHighWatermark >= 1, "scheduler memory watermarks are invalid")
	add(config.Scheduler.IdlePoll.Duration() <= 0 || config.Scheduler.IdlePollMax.Duration() < config.Scheduler.IdlePoll.Duration(), "scheduler idle polling bounds are invalid")

	heartbeat := config.Worker.HeartbeatInterval.Duration()
	lease := config.Worker.LeaseDuration.Duration()
	lostAfter := config.Worker.LostAfter.Duration()
	recoveryScan := config.Worker.RecoveryScannerInterval.Duration()
	add(heartbeat <= 0 || lease <= 0 || heartbeat >= lease/3, "worker heartbeat_interval must be less than lease_duration/3")
	add(lostAfter < lease+recoveryScan, "worker lost_after must be >= lease_duration + recovery_scanner_interval")
	add(config.Worker.BuiltinSlots <= 0 || config.Worker.SandboxSlots <= 0, "worker slots must be positive")
	add(config.Worker.TaskPollLimit <= 0 || config.Worker.TaskPollLimit > 32, "worker task_poll_limit must be in [1,32]")
	add(config.Worker.MaxRecoveries < 0, "worker max_recoveries cannot be negative")
	add(len(config.Worker.RecoveryBackoff) == 0, "worker recovery_backoff is required")
	for _, backoff := range config.Worker.RecoveryBackoff {
		add(backoff.Duration() <= 0, "worker recovery_backoff values must be positive")
	}
	add(config.Sandbox.Image == "" || config.Sandbox.Command == "", "sandbox image and command are required")
	add(config.Sandbox.Runtime != "runc" && config.Sandbox.Runtime != "runsc", "sandbox runtime must be runc or runsc")
	sandboxURL, sandboxURLErr := url.ParseRequestURI(config.Sandbox.RuntimeURL)
	add(sandboxURLErr != nil || (sandboxURL.Scheme != "http" && sandboxURL.Scheme != "https") || sandboxURL.Host == "" || sandboxURL.User != nil, "sandbox runtime_url must be an absolute HTTP(S) URL")
	if config.Profile != ProductionDefault {
		add(config.Sandbox.RuntimeToken == "", "sandbox runtime_token is required")
	}
	if config.Profile == ProductionDefault {
		add(config.Sandbox.Runtime != "runsc", "production sandbox runtime must be runsc")
		add(!strings.Contains(config.Sandbox.Image, "@sha256:"), "production sandbox image must be pinned by digest")
		add(sandboxURLErr != nil || sandboxURL.Scheme != "https" || sandboxURL.Host == "" || sandboxURL.User != nil, "production sandbox runtime_url must be an absolute HTTPS URL")
	}

	minimumInboxRetention := config.Kafka.Topics.RuntimeEvent.Retention.Duration() + config.Kafka.MaxManualReplayWindow.Duration()
	add(config.Outbox.InboxRetention.Duration() <= minimumInboxRetention, "outbox inbox_retention must exceed runtime event retention + manual replay window")
	add(config.Outbox.Batch <= 0 || config.Outbox.PublishConcurrency <= 0 || config.Outbox.ScanBatch <= 0, "outbox batch and concurrency values must be positive")
	add(config.Outbox.RetryTimerInterval.Duration() <= 0 || config.Outbox.DeadlineScannerInterval.Duration() <= 0 || config.Outbox.ReconcilerInterval.Duration() <= 0, "recovery timer, deadline scanner and reconciler intervals must be positive")
	add(config.Cache.ExecutionSnapshotTTL.Duration() <= 0 || config.Cache.ActiveRunContextTTL.Duration() <= 0, "cache TTL values must be positive")
	add(config.Migrations.Directory == "", "migrations.directory is required")
	add(config.Migrations.LockTimeout.Duration() <= 0, "migrations.lock_timeout must be positive")
	add(!slices.Contains([]string{"debug", "info", "warn", "error"}, config.Observability.LogLevel), "observability.log_level is invalid")

	return errors.Join(problems...)
}

var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func validIdentifier(value string) bool {
	return identifierPattern.MatchString(value)
}

func (config Config) TopicName(topic KafkaTopicConfig) string {
	return config.Kafka.TopicPrefix + "." + topic.Name
}

func ParsePort(value string) (int, error) {
	index := strings.LastIndex(value, ":")
	if index < 0 || index == len(value)-1 {
		return 0, fmt.Errorf("address %q has no port", value)
	}
	port, err := strconv.Atoi(value[index+1:])
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("address %q has invalid port", value)
	}
	return port, nil
}
