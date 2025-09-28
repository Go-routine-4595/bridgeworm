package controller

import (
	"context"
	"os"

	"github.com/Go-routine-4595/bridgeworm/internal/config"
	"github.com/Go-routine-4595/bridgeworm/internal/mqtt"
	"github.com/Go-routine-4595/bridgeworm/usecase"
	"github.com/google/uuid"

	"github.com/rs/zerolog"
)

type MqttController struct {
	controller *mqtt.MQTTConnector
	useCase    usecase.ISubmit
	logger     *zerolog.Logger
}

func NewMqttController(config *config.Config, useCase usecase.ISubmit, logger *zerolog.Logger) *MqttController {
	var (
		user  *string
		passw *string
		sub   *string
	)

	if config.User != "" {
		user = &config.User
	} else {
		user = nil
	}
	if config.Password != "" {
		passw = &config.Password
	} else {
		passw = nil
	}
	if config.SubscriptionTopic != "" {
		sub = &config.SubscriptionTopic
	} else {
		sub = nil
	}
	cfg := &mqtt.MQTTConfig{
		Host:           config.MqttHost,
		Port:           config.MqttPort,
		Keepalive:      60,
		Username:       user,
		Password:       passw,
		SubscribeTopic: sub,
		ClientID:       "bridgework-" + uuid.New().String(),
	}

	var l zerolog.Logger

	if logger == nil {
		l = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		l = *logger
	}

	c := &MqttController{
		useCase: useCase,
		logger:  &l,
	}
	controller := mqtt.NewMQTTConnector(cfg, logger).WithLogger(logger).WithSubscription(c)
	c.controller = controller

	return c
}

func (c *MqttController) OnConnect(topic string, msg []byte) {
	err := c.useCase.Submit(topic, msg)
	if err != nil {
		c.logger.Error().Err(err).Str("topic", topic).Msg("error on submitting message")
	}
}

func (c *MqttController) Start(ctx context.Context) error {
	var err error

	go func() {
		err = c.controller.Start(ctx)
	}()

	return err
}
