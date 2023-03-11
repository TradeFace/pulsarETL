package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tradeface/pulsarETL/internal/pulsar"

	"github.com/tradeface/pulsarETL/internal/config"
	"github.com/tradeface/pulsarETL/internal/etl"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	client, err := pulsar.NewPulsarClient(&config.Pulsar)
	if err != nil {
		log.Fatalf("Failed to create pulsar client: %v", err)
	}

	// Define a channel for transformed messages
	transformedMessages := make(chan []byte)

	consumer := pulsar.NewConsumer(client, config.InputTopic.Name, config.InputTopic.SubscriptionName, func(msg []byte) error {
		err := etl.ProcessAvroMessage(msg, &config.MessageTransform, transformedMessages)
		if err != nil {
			log.Printf("Error while transforming message: %v", err)
		}
		return nil
	})

	producer, err := pulsar.NewProducer(client, config.OutputTopic.Name, config.OutputTopic.ProducerSettings)
	if err != nil {
		log.Fatalf("Failed to create pulsar producer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go etl.ConsumeMessages(ctx, consumer)
	go etl.ProduceTransformedMessages(ctx, producer, transformedMessages)

	waitForCancelSignal(cancel)
}

func waitForCancelSignal(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
}
