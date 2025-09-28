package domain

import "encoding/json"

type MQTTMessage struct {
	SourceTopic string
	Byte        json.RawMessage
}

type NATSMessage struct {
	Byte []byte
}
