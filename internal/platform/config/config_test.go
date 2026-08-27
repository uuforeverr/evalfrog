package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadProfiles(t *testing.T) {
	t.Parallel()
	for _, profile := range []string{"local", "test", ProductionDefault} {
		profile := profile
		t.Run(profile, func(t *testing.T) {
			t.Parallel()
			value, err := Load(LoadOptions{Directory: configDirectory(t), Profile: profile, LookupEnv: emptyEnvironment})
			if err != nil {
				t.Fatalf("load %s: %v", profile, err)
			}
			if value.Profile != profile || value.Namespace != profile {
				t.Fatalf("unexpected identity: profile=%q namespace=%q", value.Profile, value.Namespace)
			}
		})
	}
}

func TestLoadRejectsProfileMixing(t *testing.T) {
	t.Parallel()
	directory := copyConfigs(t)
	path := filepath.Join(directory, "local.yaml")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content = []byte(strings.Replace(string(content), "evalfrog:local:sched:", "evalfrog:test:sched:", 1))
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Load(LoadOptions{Directory: directory, Profile: "local", LookupEnv: emptyEnvironment})
	if err == nil || !strings.Contains(err.Error(), "key_prefix") {
		t.Fatalf("expected profile isolation error, got %v", err)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	directory := copyConfigs(t)
	path := filepath.Join(directory, "local.yaml")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("\nunknown_m0_field: true\n"); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Load(LoadOptions{Directory: directory, Profile: "local", LookupEnv: emptyEnvironment})
	if err == nil || !strings.Contains(err.Error(), "field unknown_m0_field not found") {
		t.Fatalf("expected strict YAML error, got %v", err)
	}
}

func TestValidateCapacityConstraints(t *testing.T) {
	t.Parallel()
	base, err := Load(LoadOptions{Directory: configDirectory(t), Profile: "local", LookupEnv: emptyEnvironment})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		message string
	}{
		{"missing required DSN", func(value *Config) { value.Postgres.DSN = "" }, "postgres.dsn is required"},
		{"invalid HTTP port", func(value *Config) { value.HTTP.ControlPlaneAddress = ":0" }, "control_plane_address"},
		{"heartbeat", func(value *Config) { value.Worker.HeartbeatInterval = Duration(4 * time.Second) }, "heartbeat_interval"},
		{"lost threshold", func(value *Config) { value.Worker.LostAfter = Duration(10 * time.Second) }, "lost_after"},
		{"reservation", func(value *Config) { value.Scheduler.ReservationTTL = 0 }, "reservation TTL"},
		{"topic bounds", func(value *Config) { value.Scheduler.BuiltinMaxQueue = 0 }, "topic queue bounds"},
		{"memory hysteresis", func(value *Config) { value.Scheduler.MemoryResumeWatermark = value.Scheduler.MemoryHighWatermark }, "memory watermarks"},
		{"consumer partitions", func(value *Config) { value.Worker.ExpectedBuiltinConsumers = 2 }, "partitions"},
		{"engine database budget", func(value *Config) { value.Engine.MaxInflight = int(value.Postgres.PoolMax) }, "engine.max_inflight"},
		{"database budget", func(value *Config) { value.Postgres.ExpectedControlPlaneInstances = 100 }, "70% safety budget"},
		{"inbox retention", func(value *Config) { value.Outbox.InboxRetention = Duration(time.Hour) }, "inbox_retention"},
		{"recovery scanner interval", func(value *Config) { value.Outbox.ReconcilerInterval = 0 }, "recovery timer, deadline scanner and reconciler intervals"},
		{"sandbox runtime", func(value *Config) { value.Sandbox.Runtime = "unsafe" }, "sandbox runtime"},
		{"sandbox runtime token", func(value *Config) { value.Sandbox.RuntimeToken = "" }, "runtime_token"},
		{"sandbox runtime URL", func(value *Config) { value.Sandbox.RuntimeURL = "" }, "runtime_url"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := base
			test.mutate(&value)
			err := value.Validate()
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %q error, got %v", test.message, err)
			}
		})
	}
}

func TestProductionRuntimeURLRequiresHTTPS(t *testing.T) {
	t.Parallel()
	value, err := Load(LoadOptions{Directory: configDirectory(t), Profile: ProductionDefault, LookupEnv: emptyEnvironment})
	if err != nil {
		t.Fatal(err)
	}
	value.Sandbox.RuntimeURL = "http://sandbox-runtime.internal/v1"
	if err := value.Validate(); err == nil || !strings.Contains(err.Error(), "runtime_url") {
		t.Fatalf("expected HTTPS runtime URL validation error, got %v", err)
	}
}

func TestEnvironmentOverrides(t *testing.T) {
	t.Parallel()
	environment := map[string]string{
		"EVALFROG_POSTGRES_DSN":  "postgres://override/db",
		"EVALFROG_KAFKA_BROKERS": "kafka-a:9092, kafka-b:9092",
	}
	value, err := Load(LoadOptions{
		Directory: configDirectory(t),
		Profile:   "local",
		LookupEnv: func(key string) (string, bool) { result, ok := environment[key]; return result, ok },
	})
	if err != nil {
		t.Fatal(err)
	}
	if value.Postgres.DSN != environment["EVALFROG_POSTGRES_DSN"] {
		t.Fatalf("postgres override not applied")
	}
	if len(value.Kafka.Brokers) != 2 || value.Kafka.Brokers[1] != "kafka-b:9092" {
		t.Fatalf("Kafka override not applied: %v", value.Kafka.Brokers)
	}
}

func emptyEnvironment(string) (string, bool) { return "", false }

func configDirectory(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func copyConfigs(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	for _, name := range []string{"production-default.yaml", "local.yaml", "test.yaml"} {
		content, err := os.ReadFile(filepath.Join(configDirectory(t), name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(directory, name), content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return directory
}
