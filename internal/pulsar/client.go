package pulsar

import (
	"fmt"

	"pulsarETL/internal/config"

	"github.com/apache/pulsar-client-go/pulsar"
)

func NewPulsarClient(config *config.PulsarConfig) (pulsar.Client, error) {
	auth := pulsar.Authentication{}
	if config.Auth.Type == "token" {
		auth = pulsar.NewAuthenticationToken(config.Auth.Token)
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            config.ServiceURL,
		Authentication: auth,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create pulsar client: %v", err)
	}

	return client, nil
}
