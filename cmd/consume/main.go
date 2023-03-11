package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/linkedin/goavro/v2"
	"github.com/tradeface/pulsarETL/internal/config"
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

	// Create a Pulsar consumer
	consumer := pulsar.NewConsumer(client, cfg.OutputTopic.Name, cfg.InputTopic.SubscriptionName, func(msg []byte) error {

		ocfr, err := goavro.NewOCFReader(bytes.NewReader(msg))
		if err != nil {
			log.Fatalf("Failed to create producer: %v", err)
		}

		for ocfr.Scan() {
			v, err := ocfr.Read()
			if err != nil {
				log.Fatalf("Failed to create producer: %v", err)
			}
			fmt.Println("row:", v)
		}
		return nil
	})

	// Create a context and a cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Start the consumer and producer goroutines
	go consumer.ConsumeMessages(ctx)

	// Wait for the cancel signal
	waitForCancelSignal(cancel)
}

func waitForCancelSignal(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
}
