package pulsar

import (
	"context"
	"fmt"

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

func (p *Producer) ProduceMessage(ctx context.Context, message []byte) error {
	msg := &pulsar.ProducerMessage{
		Payload: message,
	}

	_, err := p.producer.Send(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}
	//fmt.Info("failed to send message: %v", err)

	return nil
}

func (p *Producer) Close() {
	p.producer.Close()
}
