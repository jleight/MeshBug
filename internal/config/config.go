package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Broker is one MQTT broker the ingest pipeline should subscribe to.
type Broker struct {
	Name        string `json:"name"`
	URL         string `json:"url"`         // wss://host:port, mqtts://, mqtt://, ws://, tcp://
	Username    string `json:"username,omitempty"`
	Password    string `json:"password,omitempty"`
	TopicPrefix string `json:"topicPrefix,omitempty"` // default "meshcore/"
}

// Config holds everything that the two services share or each one needs.
// LoadIngest / LoadWeb populate the appropriate subset and validate it.
type Config struct {
	HTTPAddr    string // web only
	DatabaseURL string // both
	AutoMigrate bool   // ingest only
	LogLevel    slog.Level
	Brokers     []Broker // ingest only
}

// LoadIngest reads the env vars the ingest service needs (DB URL + brokers).
func LoadIngest() (*Config, error) {
	c, err := loadCommon()
	if err != nil {
		return nil, err
	}
	c.AutoMigrate = envBool("MESHBUG_AUTO_MIGRATE", true)
	if err := loadBrokers(c); err != nil {
		return nil, err
	}
	if len(c.Brokers) == 0 {
		return nil, fmt.Errorf("ingest: no brokers configured (set MESHBUG_BROKERS_JSON, MESHBUG_BROKERS_CONFIG, or MQTT_BROKER)")
	}
	return c, nil
}

// LoadWeb reads the env vars the web service needs (DB URL + listen addr).
// Brokers are not required.
func LoadWeb() (*Config, error) {
	c, err := loadCommon()
	if err != nil {
		return nil, err
	}
	c.HTTPAddr = envDefault("MESHBUG_HTTP_ADDR", ":8080")
	return c, nil
}

func loadCommon() (*Config, error) {
	c := &Config{
		DatabaseURL: os.Getenv("MESHBUG_DATABASE_URL"),
		LogLevel:    parseLevel(envDefault("MESHBUG_LOG_LEVEL", "info")),
	}
	if c.DatabaseURL == "" {
		return nil, fmt.Errorf("MESHBUG_DATABASE_URL is required")
	}
	return c, nil
}

func loadBrokers(c *Config) error {
	if raw := os.Getenv("MESHBUG_BROKERS_JSON"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.Brokers); err != nil {
			return fmt.Errorf("parse MESHBUG_BROKERS_JSON: %w", err)
		}
	} else if path := os.Getenv("MESHBUG_BROKERS_CONFIG"); path != "" {
		// Helm-style: a ConfigMap-mounted JSON file with the structural broker
		// list (no creds), creds injected as MESHBUG_BROKER_<NAME>_USERNAME /
		// _PASSWORD env vars from Secrets.
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read brokers config %s: %w", path, err)
		}
		if err := json.Unmarshal(raw, &c.Brokers); err != nil {
			return fmt.Errorf("parse brokers config %s: %w", path, err)
		}
		for i, b := range c.Brokers {
			envKey := strings.ToUpper(strings.ReplaceAll(b.Name, "-", "_"))
			if u := os.Getenv("MESHBUG_BROKER_" + envKey + "_USERNAME"); u != "" {
				c.Brokers[i].Username = u
			}
			if p := os.Getenv("MESHBUG_BROKER_" + envKey + "_PASSWORD"); p != "" {
				c.Brokers[i].Password = p
			}
		}
	}
	// Convenience fallback for local dev: MQTT_BROKER/USER/PASSWORD.
	if len(c.Brokers) == 0 {
		if url := os.Getenv("MQTT_BROKER"); url != "" {
			c.Brokers = []Broker{{
				Name: "default", URL: url,
				Username:    os.Getenv("MQTT_USER"),
				Password:    os.Getenv("MQTT_PASSWORD"),
				TopicPrefix: "meshcore/",
			}}
		}
	}
	for i, b := range c.Brokers {
		if b.TopicPrefix == "" {
			c.Brokers[i].TopicPrefix = "meshcore/"
		}
		if !strings.HasSuffix(c.Brokers[i].TopicPrefix, "/") {
			c.Brokers[i].TopicPrefix += "/"
		}
	}
	return nil
}

func envDefault(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envBool(k string, d bool) bool {
	v := strings.ToLower(os.Getenv(k))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return d
	}
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
