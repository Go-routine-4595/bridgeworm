package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/Go-routine-4595/bridgeworm/usecase"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Message struct {
	mqtt.Message
}

// MQTTConfig holds configuration for MQTT connection
type MQTTConfig struct {
	Host           string
	Port           int
	Keepalive      int
	Username       *string
	Password       *string
	SubscribeTopic *string
	ClientID       string
}

// NewMQTTConfig creates a default MQTT configuration
func NewMQTTConfig(host string, port int, clientid string) *MQTTConfig {
	return &MQTTConfig{
		Host:           host,
		Port:           port,
		Keepalive:      60,
		Username:       nil,
		Password:       nil,
		SubscribeTopic: nil,
		ClientID:       clientid,
	}
}

func (c *MQTTConfig) WithUsername(username string) *MQTTConfig {
	c.Username = &username
	return c
}

func (c *MQTTConfig) WithPassword(password string) *MQTTConfig {
	c.Password = &password
	return c
}

func (c *MQTTConfig) WithSubscribeTopic(topic string) *MQTTConfig {
	c.SubscribeTopic = &topic
	return c
}

// MQTTConnector main class for processing MQTT messages and converting to FCTS format
type MQTTConnector struct {
	config      *MQTTConfig
	client      mqtt.Client
	logger      *zerolog.Logger
	processData usecase.ISubmit
}

// NewMQTTConnector creates a new MQTT connector instance
func NewMQTTConnector(config *MQTTConfig, l *zerolog.Logger) *MQTTConnector {
	var logger zerolog.Logger

	if l == nil {
		logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		logger = *l
	}

	connector := &MQTTConnector{
		config: config,
		logger: &logger,
	}

	connector.setupClient()
	return connector
}

func (m *MQTTConnector) WithSubscription(usecase usecase.ISubmit) *MQTTConnector {
	m.processData = usecase
	return m
}

func (m *MQTTConnector) WithLogger(logger *zerolog.Logger) *MQTTConnector {
	m.logger = logger
	return m
}

// setupClient configures MQTT client with callbacks and authentication
func (m *MQTTConnector) setupClient() {
	// Generate unique client ID
	clientID := fmt.Sprintf("%s.%s", m.config.ClientID, uuid.New().String())

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("ssl://%s:%d", m.config.Host, m.config.Port))
	opts.SetClientID(clientID)
	opts.SetCleanSession(true)
	opts.SetKeepAlive(time.Duration(m.config.Keepalive) * time.Second)

	// Set TLS configuration (insecure)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	opts.SetTLSConfig(tlsConfig)

	// Set authentication if provided
	if m.config.Username != nil && m.config.Password != nil {
		opts.SetUsername(*m.config.Username)
		opts.SetPassword(*m.config.Password)
	}

	// Set callbacks
	opts.SetOnConnectHandler(m.onConnect)
	opts.SetConnectionLostHandler(m.onDisconnect)

	m.client = mqtt.NewClient(opts)
}

// onConnect callback for MQTT connection
func (m *MQTTConnector) onConnect(client mqtt.Client) {
	m.logger.Info().Msg("Connected to MQTT broker successfully")

	if m.config.SubscribeTopic == nil {
		return
	}
	token := client.Subscribe(*m.config.SubscribeTopic, 0, m.onMessage)
	if token.Wait() && token.Error() != nil {
		m.logger.Error().Msgf("Failed to subscribe to %s: %v", *m.config.SubscribeTopic, token.Error())
		return
	}

	m.logger.Info().Msgf("Subscribed to %s", *m.config.SubscribeTopic)
}

func (m *MQTTConnector) onMessage(client mqtt.Client, msg mqtt.Message) {
	err := m.processData.Submit(msg.Topic(), msg.Payload())
	if err != nil {
		m.logger.Error().Msgf("Error sending channel full message: %v message: %s", err, string(msg.Payload()))
	}
}

// onDisconnect callback for MQTT disconnection
func (m *MQTTConnector) onDisconnect(client mqtt.Client, err error) {
	if err != nil {
		m.logger.Warn().Msgf("Unexpected disconnection from MQTT broker: %v", err)
	} else {
		m.logger.Info().Msg("Disconnected from MQTT broker")
	}
}

// Connect establishes connection to MQTT broker
func (m *MQTTConnector) Connect() error {
	token := m.client.Connect()
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to connect to MQTT broker: %v", token.Error())
	}
	return nil
}

// Publish sends data point to MQTT broker
func (m *MQTTConnector) Publish(topic string, dataPoint []byte) error {
	token := m.client.Publish(topic, 0, false, dataPoint)
	if token.Wait() && token.Error() != nil {
		m.logger.Warn().Msgf("Failed to publish to %s: %v", topic, token.Error())
		return token.Error()
	}

	m.logger.Debug().Msgf("Published data to %s", topic)
	return nil
}

// Start begins the MQTT client and starts processing messages
func (m *MQTTConnector) Start(ctx context.Context) error {
	m.logger.Info().Msgf("Connecting to MQTT broker at %s:%d", m.config.Host, m.config.Port)

	if err := m.Connect(); err != nil {
		return fmt.Errorf("error starting MQTT client: %v", err)
	}

	res := <-ctx.Done()
	m.logger.Info().Msgf("Received signal %v. Shutting down gracefully...", res)

	m.Stop()
	return nil

}

// Stop gracefully stops the MQTT client
func (m *MQTTConnector) Stop() {
	m.logger.Info().Msg("Stopping MQTT client...")
	m.client.Disconnect(250) // 250ms timeout for graceful disconnect
}
