package pulsar

import (
	"fmt"
	"time"

	"github.com/tradeface/pulsarETL/internal/config"

	"github.com/apache/pulsar-client-go/pulsar"
)

func NewPulsarClient(config *config.PulsarConfig) (pulsar.Client, error) {
	var (
		client pulsar.Client
		err    error
	)

	// Set up exponential backoff algorithm
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		client, err = pulsar.NewClient(pulsar.ClientOptions{
			URL: config.ServiceURL,
		})

		if err == nil {
			return client, nil
		}

		fmt.Printf("failed to create pulsar client: %v, retrying in %s\n", err, backoff)

		// Wait for the backoff time before retrying
		time.Sleep(backoff)

		// Increase the backoff time exponentially up to the maxBackoff time
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}
