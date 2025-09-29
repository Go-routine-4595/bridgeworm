package service

import (
	"strings"

	"github.com/Go-routine-4595/bridgeworm/domain"
)

type IService interface {
	ProcessMessage(msg domain.MQTTMessage) domain.NATSMessage
}

type Service struct {
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) ProcessMessage(msg domain.MQTTMessage) domain.NATSMessage {

	var subject string = "unknown"

	if strings.Contains(msg.SourceTopic, "data") {
		subject = "data"
	}
	if strings.Contains(msg.SourceTopic, "state") {
		subject = "state"
	}
	return domain.NATSMessage{
		Byte:    []byte(msg.Byte),
		Subject: subject,
	}
}
