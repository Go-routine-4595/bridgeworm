package usecase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Go-routine-4595/bridgeworm/adapters/gateways"
	"github.com/Go-routine-4595/bridgeworm/domain"
	"github.com/Go-routine-4595/bridgeworm/internal/config"
	"github.com/Go-routine-4595/bridgeworm/service"
	"github.com/rs/zerolog"
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
	defer wp.wg.Done()

	worker := service.NewService()
	natsConn := gateways.NewNatsConnector(wp.natsUrl, &wp.logger)
	for {
		select {
		case job, ok := <-wp.jobQueue:
			if !ok {
				wp.logger.Info().Msgf("Worker %d: shutting down", id)
				return
			}
			wp.processMessage(job, worker, natsConn)

		case <-ctx.Done():
			wp.logger.Info().Msgf("Worker %d: shutting down context cancelled", id)
			natsConn.Close()
			return
		}
	}
}

func (wp *WorkerPool) processMessage(job domain.MQTTMessage, worker *service.Service, con *gateways.NatsConnector) {
	var err error

	defer func(now time.Time) {
		elapsed := time.Since(now)
		if err != nil {
			wp.logger.Error().Err(err).Msgf("message processing took %s", elapsed)
		} else {
			wp.logger.Info().Msgf("message processing took %s", elapsed)
		}
	}(time.Now())

	NatsMsg := worker.ProcessMessage(job)
	err = con.Publish(wp.subject, NatsMsg.Byte)
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
