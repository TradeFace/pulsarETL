package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tradeface/pulsarETL/internal/config"
	"github.com/tradeface/pulsarETL/internal/etl"
	"github.com/tradeface/pulsarETL/internal/pulsar"
)

func main() {
	// Load the configuration file
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize the Pulsar client
	client, err := pulsar.NewPulsarClient(&cfg.Pulsar)
	if err != nil {
		log.Fatalf("Failed to create pulsar client: %v", err)
	}
	defer client.Close()

	// Define a channel for transformed messages
	transformedMessages := make(chan []byte)
	failedMessages := make(chan etl.FailedMessage)

	// Create a Pulsar consumer
	consumer := pulsar.NewConsumer(client, cfg.InputTopic.Name, cfg.InputTopic.SubscriptionName, func(msg []byte) error {
		err := etl.ProcessAvroMessage(msg, &cfg.MessageTransform, transformedMessages, failedMessages)
		if err != nil {
			log.Printf("Error while transforming message: %v", err)
		}
		return nil
	})

	// Create a Pulsar producer
	producer, err := pulsar.NewProducer(client, cfg.OutputTopic.Name, cfg.OutputTopic.ProducerSettings)
	if err != nil {
		log.Fatalf("Failed to create pulsar producer: %v", err)
	}
	defer producer.Close()

	// Create a context and a cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Start the consumer and producer goroutines
	go etl.ConsumeMessages(ctx, consumer)
	go etl.ProcessFailedMessages(ctx, failedMessages)
	go etl.ProduceTransformedMessages(ctx, producer, transformedMessages)

	// Wait for the cancel signal
	waitForCancelSignal(cancel)
}

func waitForCancelSignal(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
}
