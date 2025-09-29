package gateways

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// nats connection string example
// "nats://localhost:4222"

const (
	messageBatchSize    = 1000
	messageBatchTimeout = time.Millisecond * 250
)

type NatsConnector struct {
	nats          *nats.Conn
	logger        zerolog.Logger
	batchSize     int
	lastBatchTime time.Time
}

func NewNatsConnector(url string, l *zerolog.Logger) *NatsConnector {
	var (
		logger zerolog.Logger
	)

	if l == nil {
		logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		logger = *l
	}

	nc, err := nats.Connect(url)
	if err != nil {
		logger.Fatal().Err(err).Msg("nats connect failed")
	}

	return &NatsConnector{
		logger: logger,
		nats:   nc,
	}
}

func (n *NatsConnector) Publish(subject string, b []byte) error {
	msg := &nats.Msg{Subject: subject, Data: b}
	err := n.nats.PublishMsg(msg)
	if err != nil {
		return fmt.Errorf("nats publish failed: %w", err)
	}
	n.batchSize += 1

	return n.Flush()
}

func (n *NatsConnector) Flush() error {
	if n.batchSize < messageBatchSize && time.Now().Sub(n.lastBatchTime).Milliseconds() < messageBatchTimeout.Milliseconds() {
		return nil
	}

	n.batchSize = 0
	err := n.nats.Flush()
	n.lastBatchTime = time.Now()
	if err != nil {
		return fmt.Errorf("nats flush failed: %w", err)
	}
	return nil
}

func (n *NatsConnector) Close() {
	n.nats.Close()
}
