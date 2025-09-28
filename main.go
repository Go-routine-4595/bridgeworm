package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/Go-routine-4595/bridgeworm/adapters/controller"
	"github.com/Go-routine-4595/bridgeworm/internal/config"
	"github.com/Go-routine-4595/bridgeworm/usecase"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Setup context for cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)

	// Setup logger
	logger := setupLogger(cfg.LogLevel, cfg.LogFilePath)

	// print config parameters
	printConfig(*cfg, &logger)

	// use case
	useCase := usecase.NewWorkerPool(*cfg, 5, 100, &logger)
	useCase.Start(ctx)

	// Setup MQTT controller
	ctl := controller.NewMqttController(cfg, useCase, &logger)

	err := ctl.Start(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start controller")
	}

	<-ctx.Done()
	cancel()
	logger.Info().Msg("Received signal SIGINIT. Shutting down gracefully...")
	time.Sleep(5 * time.Second)
}

func setupLogger(level string, logDir string) zerolog.Logger {
	// Create logs directory if it doesn't exist
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatal().Err(err).Msg("Failed to create log directory")
	}

	// Configure lumberjack for log rotation
	fileWriter := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, "DataEnricher.log"),
		MaxSize:    4,  // megabytes
		MaxBackups: 5,  // number of backups
		MaxAge:     30, // days
		LocalTime:  true,
		Compress:   false, // compress rotated files
	}

	// Create multi-writer (console + file)
	multi := zerolog.MultiLevelWriter(
		zerolog.ConsoleWriter{Out: os.Stdout},
		fileWriter,
	)

	// Set global log level
	switch strings.ToLower(level) {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn", "warning":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Set global logger
	return zerolog.New(multi).With().Timestamp().Logger()
}

func printConfig(cfg config.Config, logger *zerolog.Logger) {
	logger.Info().Msg("Configuration:")
	logger.Info().Str("LOG_LEVEL", cfg.LogLevel).Msg("Log level")
	logger.Info().Str("MQTT_HOST", cfg.MqttHost).Msg("MQTT host")
	logger.Info().Int("MQTT_PORT", cfg.MqttPort).Msg("MQTT port")
	logger.Info().Str("MQTT_HOST", cfg.MqttHost).Msg("MQTT host")
	logger.Info().Str("NATS_HOST", cfg.NATSHost).Msg("NATS host")
	logger.Info().Int("NATS_PORT", cfg.NATSPort).Msg("NATS port")
	logger.Info().Str("REDIS_CONNECTION_STRING", cfg.RedisConnectionString).Msg("Redis connection string")
	logger.Info().Str("SUBJECT", cfg.Subject).Msg("SUBJECT")
	logger.Info().Str("USER", cfg.User).Msg("MQTT user")
	logger.Info().Str("PASSWORD", cfg.Password).Msg("MQTT password")
	logger.Info().Str("LOG_FILE_PATH", cfg.LogFilePath).Msg("Log file path")
	logger.Info().Str("SUBSCRIPTION_TOPIC", cfg.SubscriptionTopic).Msg("Subscription topic")
	logger.Info().Bool("DYNATRACE_ENABLED", cfg.DynatraceEnabled).Msg("Dynatrace enabled")
}
