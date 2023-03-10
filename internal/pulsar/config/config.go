package config

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/viper"
)

type Config struct {
	Pulsar           PulsarConfig           `mapstructure:"pulsar"`
	InputTopic       TopicConfig            `mapstructure:"input_topic"`
	OutputTopic      TopicConfig            `mapstructure:"output_topic"`
	MessageTransform MessageTransformConfig `mapstructure:"message_transform"`
	ErrorHandling    ErrorHandlingConfig    `mapstructure:"error_handling"`
}

type PulsarConfig struct {
	ServiceURL string     `mapstructure:"service_url"`
	Auth       AuthConfig `mapstructure:"auth"`
}

type AuthConfig struct {
	Type  string `mapstructure:"type"`
	Token string `mapstructure:"token"`
}

type TopicConfig struct {
	Name             string                 `mapstructure:"name"`
	SubscriptionName string                 `mapstructure:"subscription_name"`
	ProducerSettings ProducerSettingsConfig `mapstructure:"producer_settings"`
}

type ProducerSettingsConfig struct {
	LingerTimeoutMs    int `mapstructure:"linger_timeout_ms"`
	MaxPendingMessages int `mapstructure:"max_pending_messages"`
}

type MessageTransformConfig struct {
	InputFormat          string `mapstructure:"input_format"`
	OutputFormat         string `mapstructure:"output_format"`
	SchemaRegistryURL    string `mapstructure:"schema_registry_url"`
	TransformationScript string `mapstructure:"transformation_script"`
}

type ErrorHandlingConfig struct {
	RetryAttempts int  `mapstructure:"retry_attempts"`
	RetryDelayMs  int  `mapstructure:"retry_delay_ms"`
	LogErrors     bool `mapstructure:"log_errors"`
}

func LoadConfig() (*Config, error) {
	// Get the absolute path of the configuration file
	configPath, err := filepath.Abs("config/config.yaml")
	if err != nil {
		fmt.Println("Failed to get absolute path of configuration file:", err)
		return nil, err
	}

	// Set up viper to read the configuration file
	viper.SetConfigFile(configPath)

	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
