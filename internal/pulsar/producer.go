package pulsar

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/tradeface/pulsarETL/internal/config"
)

type Producer struct {
	client   pulsar.Client
	topic    string
	options  pulsar.ProducerOptions
	producer pulsar.Producer
}

func NewProducer(client pulsar.Client, topic string, settings config.ProducerSettingsConfig) (*Producer, error) {
	options := pulsar.ProducerOptions{
		MaxPendingMessages: settings.MaxPendingMessages,
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %v", err)
	}

	return &Producer{
		client:   client,
		topic:    topic,
		options:  options,
		producer: producer,
	}, nil
}

func (p *Producer) ProduceTransformedMessages(ctx context.Context, transformedMessages <-chan []byte) {

	defer p.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-transformedMessages:
			if err := p.ProduceMessage(ctx, msg); err != nil {
				log.Printf("Failed to produce message: %v", err)
			}
		}
	}
}

func (p *Producer) ProduceMessage(ctx context.Context, message []byte) error {
	msg := &pulsar.ProducerMessage{
		Payload: message,
	}

	_, err := p.producer.Send(ctx, msg)

	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	return nil
}

func (p *Producer) Close() {
	p.producer.Close()
}
