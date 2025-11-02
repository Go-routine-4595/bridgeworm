package usecase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Go-routine-4595/bridgeworm/adapters/gateways"
	"github.com/Go-routine-4595/bridgeworm/domain"
	"github.com/Go-routine-4595/bridgeworm/internal/config"
	"github.com/Go-routine-4595/bridgeworm/service"

	"github.com/rs/zerolog"
)

const (
	flushMessageTimeout = time.Millisecond * 250
	metricsInterval     = time.Second * 30
	maxPoolBufferSize   = 64 * 1024 // 64KB
)

type ISubmit interface {
	Submit(topic string, msg []byte) error
}

type WorkerPool struct {
	workerCount     int
	jobQueue        chan domain.MQTTMessage
	wg              sync.WaitGroup
	logger          zerolog.Logger
	subject         string
	natsUrl         string
	natsMessagePool *trackedPool
	// Pool metrics
	poolGets      atomic.Uint64 // Total Get() calls
	poolPuts      atomic.Uint64 // Total Put() calls
	poolDrops     atomic.Uint64 // Buffers too large to return to pool
	poolCreations atomic.Uint64 // New objects created (proxy for misses)
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

	wp := &WorkerPool{
		workerCount: workerCount,
		jobQueue:    make(chan domain.MQTTMessage, queueSize),
		logger:      logger,
		subject:     cfg.Subject,
		natsUrl:     cfg.NATSHost + fmt.Sprintf(":%d", cfg.NATSPort),
	}
	wp.natsMessagePool = newTrackedPool(&wp.poolCreations)
	return wp
}

func (wp *WorkerPool) Start(ctx context.Context) {

	// Pool for NATSMessage structs
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		// using nats connector
		// go wp.worker(ctx, i)
		// using jetstream connector
		go wp.workerJet(ctx, i)
	}

	// Start metrics reporter
	wp.wg.Add(1)
	go wp.metricsReporter(ctx)
}

func (wp *WorkerPool) metricsReporter(ctx context.Context) {
	defer wp.wg.Done()

	ticker := time.NewTicker(metricsInterval)
	defer ticker.Stop()

	var lastGets, lastCreations uint64

	for {
		select {
		case <-ticker.C:
			gets := wp.poolGets.Load()
			puts := wp.poolPuts.Load()
			drops := wp.poolDrops.Load()
			creations := wp.poolCreations.Load()

			// Calculate metrics since last report
			getsInPeriod := gets - lastGets
			creationsInPeriod := creations - lastCreations

			// Calculate hit rate
			var hitRate float64
			if getsInPeriod > 0 {
				hits := getsInPeriod - creationsInPeriod
				hitRate = (float64(hits) / float64(getsInPeriod)) * 100
			}

			// Log comprehensive metrics
			wp.logger.Info().
				Uint64("pool_gets", gets).
				Uint64("pool_puts", puts).
				Uint64("pool_drops", drops).
				Uint64("pool_creations", creations).
				Float64("hit_rate_percent", hitRate).
				Uint64("period_gets", getsInPeriod).
				Uint64("period_creations", creationsInPeriod).
				Msg("Pool metrics")

			// Warnings for potential issues
			if hitRate < 50 && getsInPeriod > 100 {
				wp.logger.Warn().
					Float64("hit_rate", hitRate).
					Msg("Low pool hit rate - consider increasing worker count or investigating GC pressure")
			}

			if drops > gets/10 { // More than 10% dropped
				wp.logger.Warn().
					Uint64("drops", drops).
					Uint64("total_operations", gets).
					Msg("High buffer drop rate - messages may be larger than expected")
			}

			lastGets = gets
			lastCreations = creations

		case <-ctx.Done():
			// Final metrics on shutdown
			wp.logFinalMetrics()
			return
		}
	}
}

func (wp *WorkerPool) logFinalMetrics() {
	gets := wp.poolGets.Load()
	creations := wp.poolCreations.Load()

	var overallHitRate float64
	if gets > 0 {
		hits := gets - creations
		overallHitRate = (float64(hits) / float64(gets)) * 100
	}

	wp.logger.Info().
		Uint64("total_gets", gets).
		Uint64("total_puts", wp.poolPuts.Load()).
		Uint64("total_drops", wp.poolDrops.Load()).
		Uint64("total_creations", creations).
		Float64("overall_hit_rate_percent", overallHitRate).
		Msg("Final pool metrics")
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
			wp.logger.Debug().Msgf("message processing took %s -- worker id: %d ", elapsed, id)
		}
	}(time.Now())

	natsMsg := wp.natsMessagePool.Get()
	// Track pool Get
	wp.poolGets.Add(1)

	defer func() {
		bufferSize := cap(natsMsg.Byte)

		if bufferSize <= maxPoolBufferSize {
			// Reset and return to pool
			natsMsg.Byte = natsMsg.Byte[:0]
			natsMsg.Subject = ""
			wp.natsMessagePool.Put(natsMsg)
			wp.poolPuts.Add(1)
		} else {
			// Buffer too large, don't return to pool
			wp.poolDrops.Add(1)
			wp.logger.Debug().
				Int("buffer_size", bufferSize).
				Int("max_size", maxPoolBufferSize).
				Msgf("Dropping oversized buffer from pool -- worker id: %d", id)
		}
	}()

	// Reset message
	natsMsg.Byte = natsMsg.Byte[:0]
	natsMsg.Subject = ""

	worker.ProcessMessage(job, natsMsg)
	// Publish
	if strings.Contains(natsMsg.Subject, "unknown") {
		err = con.Publish(wp.subject+"fcts", natsMsg.Byte)
		if err != nil {
			wp.logger.Error().Err(err).Msgf("failed to publish to fcts topic -- worker id: %d", id)
			return
		}
	} else {
		err = con.Publish(wp.subject+natsMsg.Subject, natsMsg.Byte)
		if err != nil {
			wp.logger.Error().Err(err).Msgf("failed to publish to %s topic -- worker id: %d", natsMsg.Subject, id)
			return
		}
	}

}

// JetStream version for testing
func (wp *WorkerPool) workerJet(ctx context.Context, id int) {
	worker := service.NewService()
	jetConn := gateways.NewStreamConnector(wp.natsUrl, &wp.logger)

	defer func() {
		jetConn.Close()
		wp.wg.Done()
	}()

	for {
		select {
		case job, ok := <-wp.jobQueue:
			if !ok {
				wp.logger.Info().Msgf("Worker %d: shutting down", id)
				return
			}
			wp.processMessageJet(job, worker, jetConn, id)
		case <-ctx.Done():
			wp.logger.Info().Msgf("Worker %d: shutting down context cancelled", id)
			return
		}
	}
}

func (wp *WorkerPool) processMessageJet(job domain.MQTTMessage, worker *service.Service, con *gateways.StreamConnector, id int) {
	var err error

	defer func(now time.Time) {
		elapsed := time.Since(now)
		if err != nil {
			wp.logger.Error().Err(err).Msgf("message processing took %s -- worker id: %d ", elapsed, id)
		} else {
			wp.logger.Debug().Msgf("message processing took %s -- worker id: %d ", elapsed, id)
		}
	}(time.Now())

	natsMsg := wp.natsMessagePool.Get()
	// Track pool Get
	wp.poolGets.Add(1)

	defer func() {
		bufferSize := cap(natsMsg.Byte)

		if bufferSize <= maxPoolBufferSize {
			// Reset and return to pool
			natsMsg.Byte = natsMsg.Byte[:0]
			natsMsg.Subject = ""
			wp.natsMessagePool.Put(natsMsg)
			wp.poolPuts.Add(1)
		} else {
			// Buffer too large, don't return to pool
			wp.poolDrops.Add(1)
			wp.logger.Debug().
				Int("buffer_size", bufferSize).
				Int("max_size", maxPoolBufferSize).
				Msgf("Dropping oversized buffer from pool -- worker id: %d", id)
		}
	}()

	// Reset message
	natsMsg.Byte = natsMsg.Byte[:0]
	natsMsg.Subject = ""

	worker.ProcessMessage(job, natsMsg)
	// Publish
	if strings.Contains(natsMsg.Subject, "unknown") {
		err = con.PublishAsync(wp.subject+"fcts", natsMsg.Byte)
		if err != nil {
			wp.logger.Error().Err(err).Msgf("failed to publish to fcts topic -- worker id: %d", id)
			return
		}
	} else {
		err = con.PublishAsync(wp.subject+natsMsg.Subject, natsMsg.Byte)
		if err != nil {
			wp.logger.Error().Err(err).Msgf("failed to publish to %s topic -- worker id: %d", natsMsg.Subject, id)
			return
		}
	}

}

// -----------------------------
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
	// just to make sure that the MQTT controller is done closing the MQTT connection and doesn't send any more messages
	time.Sleep(time.Millisecond * 250)

	close(wp.jobQueue)
	wp.wg.Wait()
	wp.logger.Info().Msg("UseCase Worker pool Shutting down gracefully...")
}
