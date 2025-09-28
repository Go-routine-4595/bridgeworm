package service

import "github.com/Go-routine-4595/bridgeworm/domain"

type IService interface {
	ProcessMessage(msg domain.MQTTMessage) domain.NATSMessage
}

type Service struct {
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) ProcessMessage(msg domain.MQTTMessage) domain.NATSMessage {
	return domain.NATSMessage{
		Byte: []byte(msg.Byte),
	}
}
