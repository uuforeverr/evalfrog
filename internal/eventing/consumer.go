package eventing

import (
	"context"
	"fmt"
	"log/slog"
)

type Delivery interface {
	Topic() string
	Key() string
	Payload() []byte
	Ack(context.Context) error
	Nack()
	DeadLetter(context.Context, string) error
}

type Consumer interface {
	Receive(context.Context) (Delivery, error)
}

type BatchMessage struct {
	Topic   string
	Key     string
	Payload []byte
}

type DeliveryBatch interface {
	Messages() []BatchMessage
	Ack(context.Context) error
	Nack()
	DeadLetter(context.Context, int, string) error
}

type BatchConsumer interface {
	ReceiveBatch(context.Context) (DeliveryBatch, error)
}

type RuntimeEventHandler interface {
	Consume(context.Context, RuntimeEvent) error
}

type BatchRuntimeEventHandler interface {
	ConsumeBatch(context.Context, []RuntimeEvent) error
}

type RuntimeConsumerService struct {
	consumer Consumer
	handler  RuntimeEventHandler
	logger   *slog.Logger
}

func NewRuntimeConsumerService(consumer Consumer, handler RuntimeEventHandler, logger *slog.Logger) (*RuntimeConsumerService, error) {
	if consumer == nil || handler == nil || logger == nil {
		return nil, fmt.Errorf("runtime consumer dependencies are required")
	}
	return &RuntimeConsumerService{consumer: consumer, handler: handler, logger: logger}, nil
}

func (service *RuntimeConsumerService) Name() string { return "runtime-event-consumer" }

func (service *RuntimeConsumerService) Run(ctx context.Context) error {
	if consumer, consumerOK := service.consumer.(BatchConsumer); consumerOK {
		if handler, handlerOK := service.handler.(BatchRuntimeEventHandler); handlerOK {
			return service.runBatches(ctx, consumer, handler)
		}
	}
	for {
		delivery, err := service.consumer.Receive(ctx)
		if err != nil {
			return err
		}
		event, err := ParseRuntimeEvent(delivery.Payload())
		if err != nil {
			service.logger.Warn("runtime event sent to dead letter", "error", err)
			if deadErr := delivery.DeadLetter(ctx, "INVALID_RUNTIME_EVENT"); deadErr != nil {
				return deadErr
			}
			continue
		}
		if err = service.handler.Consume(ctx, event); err != nil {
			service.logger.Warn("runtime event processing deferred", "component", service.Name(),
				"event_type", event.EventType, "project_id", event.ProjectID,
				"run_id", event.RunID, "aggregate_id", event.AggregateID,
				"trace_id", event.TraceID, "error", err)
			delivery.Nack()
			return err
		}
		if err = delivery.Ack(ctx); err != nil {
			return err
		}
	}
}

func (service *RuntimeConsumerService) runBatches(ctx context.Context, consumer BatchConsumer, handler BatchRuntimeEventHandler) error {
	for {
		batch, err := consumer.ReceiveBatch(ctx)
		if err != nil {
			return err
		}
		messages := batch.Messages()
		events := make([]RuntimeEvent, 0, len(messages))
		for index, message := range messages {
			event, parseErr := ParseRuntimeEvent(message.Payload)
			if parseErr != nil {
				service.logger.Warn("runtime event sent to dead letter", "error", parseErr)
				if deadErr := batch.DeadLetter(ctx, index, "INVALID_RUNTIME_EVENT"); deadErr != nil {
					batch.Nack()
					return deadErr
				}
				continue
			}
			events = append(events, event)
		}
		if len(events) > 0 {
			if err = handler.ConsumeBatch(ctx, events); err != nil {
				service.logger.Warn("runtime event batch processing deferred", "component", service.Name(), "events", len(events), "error", err)
				batch.Nack()
				return err
			}
		}
		if err = batch.Ack(ctx); err != nil {
			return err
		}
	}
}

func (service *RuntimeConsumerService) Shutdown(context.Context) error { return nil }
