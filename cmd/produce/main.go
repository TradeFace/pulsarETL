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
	intpulsar "github.com/tradeface/pulsarETL/internal/pulsar"
)

func main() {
	// Load the configuration file
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Define the Avro schema
	schemaJSON := `
        {
            "type": "record",
            "name": "SensorReading",
            "fields": [
                {"name": "timestamp", "type": "long"},
                {"name": "temperature", "type": "float"},
                {"name": "humidity", "type": "float"},
                {"name": "pressure", "type": "float"}
            ]
        }
    `

	// Serialize data into the OCF format using the Avro schema
	buf := bytes.NewBuffer(nil)
	encoder, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      buf,
		Schema: schemaJSON,
	})
	if err != nil {
		log.Fatalf("Failed to create OCF encoder: %v", err)
	}
	datum := []map[string]interface{}{{
		"timestamp":   int64(1647051538),
		"temperature": float32(24.5),
		"humidity":    float32(65.2),
		"pressure":    float32(1013.2),
	}, {
		"timestamp":   int64(1647051539),
		"temperature": float32(24.6),
		"humidity":    float32(65.1),
		"pressure":    float32(1013.2),
	}, {
		"timestamp":   int64(1647051540),
		"temperature": float32(24.7),
		"humidity":    float32(65),
		"pressure":    float32(1013.2),
	}, {
		"timestamp":   int64(1647051541),
		"temperature": float32(24.8),
		"humidity":    float32(64.9),
		"pressure":    float32(1013.2),
	}, {
		"timestamp":   int64(1647051542),
		"temperature": float32(24.9),
		"humidity":    float32(64.8),
		"pressure":    float32(1013.2),
	}, {
		"timestamp":   int64(1647051543),
		"temperature": float32(25),
		"humidity":    float32(64.7),
		"pressure":    float32(1013.2),
	}}
	if err := encoder.Append(datum); err != nil {
		log.Fatalf("Failed to encode datum: %v", err)
	}

	// Read back what was put into the buffer
	ocfr, err := goavro.NewOCFReader(bytes.NewReader(buf.Bytes()))
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

	// Initialize the Pulsar client
	client, err := intpulsar.NewPulsarClient(&cfg.Pulsar)
	if err != nil {
		log.Fatalf("Failed to create pulsar client: %v", err)
	}
	defer client.Close()

	// Create a Pulsar producer
	producer, err := intpulsar.NewProducer(client, cfg.InputTopic.Name, cfg.OutputTopic.ProducerSettings)
	if err != nil {
		log.Fatalf("Failed to create pulsar producer: %v", err)
	}
	defer producer.Close()

	// Create a context and a cancel function
	ctx, cancel := context.WithCancel(context.Background())
	//ctx, _ := context.WithCancel(context.Background())

	// Start the consumer and producer goroutines
	go producer.ProduceMessage(ctx, buf.Bytes())

	// Wait for the cancel signal
	waitForCancelSignal(cancel)
}

func waitForCancelSignal(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
}
