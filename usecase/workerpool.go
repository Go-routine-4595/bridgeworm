package usecase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Go-routine-4595/bridgeworm/adapters/gateways"
	"github.com/Go-routine-4595/bridgeworm/domain"
	"github.com/Go-routine-4595/bridgeworm/internal/config"
	"github.com/Go-routine-4595/bridgeworm/service"

	"github.com/rs/zerolog"
)

const (
	flushMessageTimeout = time.Millisecond * 250
)

type ISubmit interface {
	Submit(topic string, msg []byte) error
}

type WorkerPool struct {
	workerCount int
	jobQueue    chan domain.MQTTMessage
	wg          sync.WaitGroup
	logger      zerolog.Logger
	subject     string
	natsUrl     string
}

func NewWorkerPool(cfg config.Config, workerCount int, queueSize int, l *zerolog.Logger) *WorkerPool {
	var (
		logger zerolog.Logger
	)

	if l == nil {
		logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		logger = *l
	}

	return &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan domain.MQTTMessage, queueSize),
		logger:      logger,
		subject:     cfg.Subject,
		natsUrl:     cfg.NATSHost + fmt.Sprintf(":%d", cfg.NATSPort),
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

func (wp *WorkerPool) worker(ctx context.Context, id int) {
	worker := service.NewService()
	natsConn := gateways.NewNatsConnector(wp.natsUrl, &wp.logger)
	c := time.NewTicker(flushMessageTimeout)

	defer func() {
		c.Stop()
		err := natsConn.Flush()
		if err != nil {
			wp.logger.Error().Err(err).Msgf("nats flush failed -- worker id: %d -- on exiting", id)
		}
		natsConn.Close()
		wp.wg.Done()
	}()

	for {
		select {
		case job, ok := <-wp.jobQueue:
			if !ok {
				wp.logger.Info().Msgf("Worker %d: shutting down", id)
				return
			}
			wp.processMessage(job, worker, natsConn, id)

		case <-c.C:
			wp.logger.Debug().Msgf("Worker %d: Flushing message", id)
			err := natsConn.Flush()
			if err != nil {
				wp.logger.Error().Err(err).Msgf("nats flush failed -- worker id: %d", id)
			}
		case <-ctx.Done():
			wp.logger.Info().Msgf("Worker %d: shutting down context cancelled", id)
			return
		}
	}
}

func (wp *WorkerPool) processMessage(job domain.MQTTMessage, worker *service.Service, con *gateways.NatsConnector, id int) {
	var err error

	defer func(now time.Time) {
		elapsed := time.Since(now)
		if err != nil {
			wp.logger.Error().Err(err).Msgf("message processing took %s -- worker id: %d ", elapsed, id)
		} else {
			wp.logger.Info().Msgf("message processing took %s -- worker id: %d ", elapsed, id)
		}
	}(time.Now())

	NatsMsg := worker.ProcessMessage(job)
	if strings.Contains(NatsMsg.Subject, "unknown") {
		err = con.Publish(wp.subject+"fcts", NatsMsg.Byte)
	}
	err = con.Publish(wp.subject+NatsMsg.Subject, NatsMsg.Byte)

}

func (wp *WorkerPool) Submit(topic string, msg []byte) error {
	mqttMsg := domain.MQTTMessage{
		SourceTopic: topic,
		Byte:        msg,
	}
	select {
	case wp.jobQueue <- mqttMsg:
		return nil
	default:
		return errors.New("job queue is full")
	}
}

func (wp *WorkerPool) Close() {
	close(wp.jobQueue)
	wp.wg.Wait()
	wp.logger.Info().Msg("UseCase Worker pool Shutting down gracefully...")
}
