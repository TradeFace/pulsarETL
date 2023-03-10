package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"pulsarETL/internal/pulsar"

	"../../internal/config"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil {
		fmt.Println("Failed to load config:", err)
		return
	}

	client, err := pulsar.NewPulsarClient(&config.Pulsar)
	if err != nil {
		fmt.Println("Failed to create pulsar client:", err)
		return
	}

	// Define a channel for transformed messages
	transformedMessages := make(chan []byte)

	consumer := pulsar.NewConsumer(client, config.InputTopic.Name, config.InputTopic.SubscriptionName, func(msg []byte) error {
		return transformMessage(msg, &config.MessageTransform, transformedMessages)
	})

	producer, err := pulsar.NewProducer(client, config.OutputTopic.Name, config.OutputTopic.ProducerSettings)
	if err != nil {
		fmt.Println("Failed to create pulsar producer:", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := consumer.ConsumeMessages(ctx); err != nil {
			log.Fatalf("Error while consuming messages: %v", err)
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-transformedMessages:
				err := producer.ProduceMessage(ctx, msg)
				if err != nil {
					fmt.Println("Failed to produce message:", err)
					// Handle error
				}
			}
		}
	}()

	// Wait for a signal to cancel the context
	waitForCancelSignal(cancel)
}

func waitForCancelSignal(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
}

func transformMessage(msg []byte, transformConfig *MessageTransformConfig, transformedMessages chan<- []byte) error {

	transformedMsg, err := transform(msg, transformConfig)
	if err != nil {
		return err
	}
	transformedMessages <- transformedMsg
	return nil
}

func transform(msg []byte, transformConfig *MessageTransformConfig) ([]byte, error) {
	// TODO: Implement message transformation
	return msg, nil
}
