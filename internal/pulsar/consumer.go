package pulsar

import (
	"context"
	"log"

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

func (c *Consumer) ConsumeMessages(ctx context.Context) {
	consumer, err := c.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            c.topic,
		SubscriptionName: c.subName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := consumer.Receive(ctx)
			if err != nil {
				continue
			}

			if err := c.callback(msg.Payload()); err != nil {
				log.Printf("Failed to process message: %v", err)
			}

			if err := consumer.Ack(msg); err != nil {
				log.Printf("Failed to acknowledge message: %v", err)
			}
		}
	}
}
