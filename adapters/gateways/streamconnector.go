package gateways

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
)

type StreamConnector struct {
	js            jetstream.JetStream
	nc            *nats.Conn
	logger        zerolog.Logger
	batchSize     int
	lastBatchTime time.Time
}

func NewStreamConnector(url string, l *zerolog.Logger) *StreamConnector {
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

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Fatal().Err(err).Msg("jetstream connect failed")
	}
	return &StreamConnector{
		logger: logger,
		js:     js,
		nc:     nc,
	}
}

func (s *StreamConnector) PublishAsync(subject string, b []byte) error {
	_, err := s.js.PublishAsync(subject, b)

	if err != nil {
		return errors.Join(errors.New("streamconnector failed to publish async"), err)
	}
	return nil
}

func (s *StreamConnector) PublishAsyncWithCheck(subject string, b []byte, ctx context.Context) error {
	ack, err := s.js.PublishAsync(subject, b)

	if err != nil {
		return errors.Join(errors.New("streamconnector failed to publish async"), err)
	}
	select {
	case <-ack.Ok():
	// success
	case err := <-ack.Err():
		return errors.Join(errors.New("streamconnector failed to receive publich ack"), err)
	case <-ctx.Done():
		return errors.New("streamconnector context cancelled")
	}
	return nil
}

func (s *StreamConnector) Close() {
	s.nc.Close()
}
