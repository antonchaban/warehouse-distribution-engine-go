package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all configuration parameters for the application.
// We use a separate struct to keep main.go clean.
type Config struct {
	DatabaseURL     string
	RabbitMQURL     string
	JavaServiceAddr string
	QueueName       string
	MockOutput      bool
}

// Load reads configuration from environment variables.
// It returns an error if critical configuration is missing.
func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:     getEnv("DATABASE_URL", ""),
		RabbitMQURL:     getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		JavaServiceAddr: getEnv("JAVA_SERVICE_ADDR", "localhost:50051"),
		QueueName:       getEnv("RABBITMQ_QUEUE", "calculation.requests"),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	mockOut, _ := strconv.ParseBool(getEnv("MOCK_OUTPUT", "false"))

	return &Config{
		DatabaseURL:     getEnv("DATABASE_URL", ""),
		RabbitMQURL:     getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		JavaServiceAddr: getEnv("JAVA_SERVICE_ADDR", "localhost:50051"),
		QueueName:       getEnv("RABBITMQ_QUEUE", "calculation.requests"),
		MockOutput:      mockOut, // <-- ASSIGN
	}, nil
}

// getEnv is a helper to read an environment variable with a default fallback.
func getEnv(key, defaultVal string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}
	return defaultVal
}
