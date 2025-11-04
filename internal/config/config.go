package config

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/rs/zerolog/log"
)

type Config struct {
	MqttHost              string
	MqttPort              int
	SubscriptionTopic     []string
	NATSHost              string
	NATSPort              int
	Subject               string
	LogLevel              string
	User                  string
	Password              string
	LogFilePath           string
	RedisConnectionString string
	DynatraceEnabled      bool
}

func Load() *Config {
	mqttPort, _ := strconv.Atoi(getEnvOrDefault("MQTT_PORT", "8883"))
	natsPort, _ := strconv.Atoi(getEnvOrDefault("NATS_PORT", "4222"))
	dynatraceEnabled, _ := strconv.ParseBool(getEnvOrDefault("DYNATRACE_ENABLED", "false"))
	subscriptionTopicRaw := getEnvOrDefault("SUBSCRIPTION_TOPIC", `["cs/v1/data/#","cs/v1/state/#", "loadtest/data"]`)
	topics := getTopics(subscriptionTopicRaw)

	return &Config{
		MqttHost:              getEnvOrDefault("MQTT_HOST", "backend.christophe.engineering"),
		MqttPort:              mqttPort,
		LogLevel:              getEnvOrDefault("LOG_LEVEL", "info"),
		SubscriptionTopic:     topics,
		NATSHost:              "nats://" + getEnvOrDefault("NATS_HOST", "backend.christophe.engineering"),
		NATSPort:              natsPort,
		Subject:               getEnvOrDefault("SUBJECT", "test_stream."),
		User:                  getEnvOrDefault("USER", ""),
		Password:              getEnvOrDefault("PASSWORD", ""),
		LogFilePath:           getEnvOrDefault("LOG_FILE_PATH", "logs"),
		RedisConnectionString: getEnvOrDefault("REDIS_CONNECTION_STRING", "redis://localhost:6379"),
		DynatraceEnabled:      dynatraceEnabled,
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getTopics(s string) []string {
	var raw interface{}

	if len(s) == 0 {
		return []string{}
	}
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		log.Panic().Err(err).Msgf("Error unmarshalling topics: %s", s)
	}
	switch raw := raw.(type) {
	case []interface{}:
		topics := make([]string, len(raw))
		for i, topic := range raw {
			topics[i] = topic.(string)
		}
		return topics
	case string:
		return []string{raw}
	default:
		log.Panic().Msgf("Error unexpected topics type: %s", s)
	}
	return []string{}
}
