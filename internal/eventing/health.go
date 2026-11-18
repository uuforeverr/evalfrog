package eventing

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// OutboxAgeReader reads a MySQL-derived health fact. It is deliberately
// not inferred from Kafka offsets: an unpublished Runtime or Task Outbox
// record is the only authority for relay backlog before Kafka receives it.
type OutboxAgeReader interface {
	OldestUnpublishedOutboxAge(context.Context) (time.Duration, error)
}

type OutboxAgeObserver interface {
	ObserveOutboxOldestAge(time.Duration)
}

type OutboxAgeService struct {
	reader   OutboxAgeReader
	observer OutboxAgeObserver
	interval time.Duration
	logger   *slog.Logger
	stop     context.CancelFunc
}

func NewOutboxAgeService(reader OutboxAgeReader, observer OutboxAgeObserver, interval time.Duration, logger *slog.Logger) (*OutboxAgeService, error) {
	if reader == nil || observer == nil || interval <= 0 || logger == nil {
		return nil, fmt.Errorf("outbox age service dependencies and interval are required")
	}
	return &OutboxAgeService{reader: reader, observer: observer, interval: interval, logger: logger}, nil
}

func (service *OutboxAgeService) Name() string { return "outbox-age-observer" }

func (service *OutboxAgeService) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	service.stop = cancel
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	for {
		age, err := service.reader.OldestUnpublishedOutboxAge(runCtx)
		if err != nil && runCtx.Err() == nil {
			service.logger.Warn("outbox age sampling failed", "component", service.Name(), "error", err)
		} else if err == nil {
			service.observer.ObserveOutboxOldestAge(age)
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
	}
}

func (service *OutboxAgeService) Shutdown(context.Context) error {
	if service.stop != nil {
		service.stop()
	}
	return nil
}

// ConsumerLag is a bounded broker-derived diagnostic value. It is never used
// to advance a Run: offsets describe transport progress, not Runtime state.
type ConsumerLag struct {
	Group string
	Topic string
	Value int64
}

type ConsumerLagReader interface {
	ConsumerLag(context.Context) ([]ConsumerLag, error)
}

type ConsumerLagObserver interface {
	ObserveKafkaConsumerLag(group, topic string, value int64)
}

type ConsumerLagService struct {
	reader   ConsumerLagReader
	observer ConsumerLagObserver
	interval time.Duration
	logger   *slog.Logger
	stop     context.CancelFunc
}

func NewConsumerLagService(reader ConsumerLagReader, observer ConsumerLagObserver, interval time.Duration, logger *slog.Logger) (*ConsumerLagService, error) {
	if reader == nil || observer == nil || interval <= 0 || logger == nil {
		return nil, fmt.Errorf("Kafka lag service dependencies and interval are required")
	}
	return &ConsumerLagService{reader: reader, observer: observer, interval: interval, logger: logger}, nil
}

func (service *ConsumerLagService) Name() string { return "kafka-consumer-lag-observer" }

func (service *ConsumerLagService) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	service.stop = cancel
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	for {
		values, err := service.reader.ConsumerLag(runCtx)
		if err != nil && runCtx.Err() == nil {
			service.logger.Warn("Kafka consumer lag sampling failed", "component", service.Name(), "error", err)
		} else if err == nil {
			for _, value := range values {
				service.observer.ObserveKafkaConsumerLag(value.Group, value.Topic, value.Value)
			}
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
	}
}

func (service *ConsumerLagService) Shutdown(context.Context) error {
	if service.stop != nil {
		service.stop()
	}
	return nil
}
