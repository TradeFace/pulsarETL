package etl

import (
	"context"
	"log"

	"github.com/tradeface/pulsarETL/internal/pulsar"
)

func ProduceTransformedMessages(ctx context.Context, producer *pulsar.Producer, transformedMessages <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-transformedMessages:
			if err := producer.ProduceMessage(ctx, msg); err != nil {
				log.Printf("Failed to produce message: %v", err)
			}
		}
	}
}
