package etl

import (
	"context"
	"log"

	"github.com/tradeface/pulsarETL/internal/pulsar"
)

func ConsumeMessages(ctx context.Context, consumer *pulsar.Consumer) {
	if err := consumer.ConsumeMessages(ctx); err != nil {
		log.Fatalf("Error while consuming messages: %v", err)
	}
}
