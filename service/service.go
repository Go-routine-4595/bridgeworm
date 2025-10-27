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

func (s *Service) ProcessMessage(msg domain.MQTTMessage, natsMsg *domain.NATSMessage) {

	var subject string = "unknown"

	if strings.Contains(msg.SourceTopic, "data") {
		subject = "data"
	}
	if strings.Contains(msg.SourceTopic, "state") {
		subject = "state"
	}

	natsMsg.Subject = subject

	// Reuse the byte slice capacity if possible
	natsMsg.Byte = append(natsMsg.Byte, msg.Byte...)
}
