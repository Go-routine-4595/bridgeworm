package config

import (
	"os"
	"strconv"
)

type Config struct {
	MqttHost              string
	MqttPort              int
	SubscriptionTopic     string
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

	return &Config{
		MqttHost:              getEnvOrDefault("MQTT_HOST", "backend.christophe.engineering"),
		MqttPort:              mqttPort,
		LogLevel:              getEnvOrDefault("LOG_LEVEL", "debug"),
		SubscriptionTopic:     getEnvOrDefault("SUBSCRIPTION_TOPIC", "cs/v1/state/cr6/#"),
		NATSHost:              "nats://" + getEnvOrDefault("NATS_HOST", "backend.christophe.engineering"),
		NATSPort:              natsPort,
		Subject:               getEnvOrDefault("SUBJECT", "test_stream.fcts"),
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
