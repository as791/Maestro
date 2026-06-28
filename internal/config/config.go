package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Temporal  TemporalConfig `yaml:"temporal"`
	HTTP      HTTPConfig     `yaml:"http"`
	Auth      AuthConfig     `yaml:"auth"`
	Actor     ActorConfig    `yaml:"actor"`
	Simulator SimConfig      `yaml:"simulator"`
}

type TemporalConfig struct {
	Address   string `yaml:"address"`
	Namespace string `yaml:"namespace"`
}

type HTTPConfig struct {
	Address string `yaml:"address"`
}

type AuthConfig struct {
	GoogleClientID     string `yaml:"googleClientId"`
	GoogleClientSecret string `yaml:"googleClientSecret"`
	RedirectURL        string `yaml:"redirectUrl"`
}

type ActorConfig struct {
	TaskQueue         string `yaml:"taskQueue"`
	ActivityTaskQueue string `yaml:"activityTaskQueue"`
	Shards            int    `yaml:"shards"`
	ContinueAfter     int    `yaml:"continueAfter"`
}

type SimConfig struct {
	Delay time.Duration `yaml:"delay"`
}

func Load() Config {
	cfg := defaults()
	loadFile(&cfg)
	applyEnv(&cfg)
	return cfg
}

func defaults() Config {
	return Config{
		Temporal: TemporalConfig{
			Address:   "localhost:7233",
			Namespace: "default",
		},
		HTTP: HTTPConfig{Address: ":8080"},
		Auth: AuthConfig{
			RedirectURL: "http://localhost:8080/auth/callback",
		},
		Actor: ActorConfig{
			TaskQueue:         "flink-control-actors",
			ActivityTaskQueue: "flink-control-activities",
			Shards:            1,
			ContinueAfter:     500,
		},
		Simulator: SimConfig{Delay: 100 * time.Millisecond},
	}
}

func loadFile(cfg *Config) {
	path := os.Getenv("MAESTRO_CONFIG")
	if path == "" {
		path = "config.yaml"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("config file read failed", "path", path, "error", err)
		}
		return
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		slog.Warn("config file parse failed", "path", path, "error", err)
	}
}

// applyEnv lets env vars override YAML values (12-factor).
func applyEnv(cfg *Config) {
	envStr(&cfg.Temporal.Address, "TEMPORAL_ADDRESS")
	envStr(&cfg.Temporal.Namespace, "TEMPORAL_NAMESPACE")
	envStr(&cfg.HTTP.Address, "HTTP_ADDRESS")
	envStr(&cfg.Auth.GoogleClientID, "GOOGLE_CLIENT_ID")
	envStr(&cfg.Auth.GoogleClientSecret, "GOOGLE_CLIENT_SECRET")
	envStr(&cfg.Auth.RedirectURL, "OAUTH_REDIRECT_URL")
	envStr(&cfg.Actor.TaskQueue, "ACTOR_TASK_QUEUE")
	envStr(&cfg.Actor.ActivityTaskQueue, "ACTIVITY_TASK_QUEUE")
	envInt(&cfg.Actor.Shards, "ACTOR_TASK_QUEUE_SHARDS")
	envInt(&cfg.Actor.ContinueAfter, "CONTINUE_AS_NEW_AFTER")
	envDuration(&cfg.Simulator.Delay, "SIMULATION_DELAY")
}

func envStr(dst *string, key string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

func envInt(dst *int, key string) {
	if v := os.Getenv(key); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			*dst = parsed
		}
	}
}

func envDuration(dst *time.Duration, key string) {
	if v := os.Getenv(key); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			*dst = parsed
		}
	}
}
