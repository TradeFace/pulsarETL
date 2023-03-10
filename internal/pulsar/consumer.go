package pulsar

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Consumer struct {
	client   pulsar.Client
	topic    string
	subName  string
	callback func([]byte) error
}

func NewConsumer(client pulsar.Client, topic string, subName string, callback func([]byte) error) *Consumer {
	return &Consumer{client: client, topic: topic, subName: subName, callback: callback}
}

func (c *Consumer) ConsumeMessages(ctx context.Context) error {
	consumer, err := c.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            c.topic,
		SubscriptionName: c.subName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		return fmt.Errorf("failed to create consumer: %v", err)
	}

	defer consumer.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := consumer.Receive(ctx)
			if err != nil {
				return fmt.Errorf("failed to receive message: %v", err)
			}

			if err := c.callback(msg.Payload()); err != nil {
				return fmt.Errorf("failed to process message: %v", err)
			}

			if err := consumer.Ack(msg); err != nil {
				return fmt.Errorf("failed to acknowledge message: %v", err)
			}
		}
	}
}
