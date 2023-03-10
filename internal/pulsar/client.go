package pulsar

import (
	"fmt"

	"github.com/tradeface/pulsarETL/internal/config"

	"github.com/apache/pulsar-client-go/pulsar"
)

func NewPulsarClient(config *config.PulsarConfig) (pulsar.Client, error) {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: config.ServiceURL,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create pulsar client: %v", err)
	}

	return client, nil
}
