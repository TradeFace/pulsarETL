package pulsar

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Producer struct {
	client  pulsar.Client
	topic   string
	options pulsar.ProducerOptions
}

func NewProducer(client pulsar.Client, topic string, settings ProducerSettingsConfig) (*Producer, error) {
	options := pulsar.ProducerOptions{
		MaxPendingMessages: settings.MaxPendingMessages,
	}

	return &Producer{
		client:  client,
		topic:   topic,
		options: options,
	}, nil
}

func (p *Producer) ProduceMessage(ctx context.Context, message []byte) error {
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{
		Topic: p.topic,
	})
	if err != nil {
		return fmt.Errorf("failed to create producer: %v", err)
	}

	defer producer.Close()

	msg := &pulsar.ProducerMessage{
		Payload: message,
	}

	_, err = producer.Send(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	return nil
}
