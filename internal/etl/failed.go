package etl

import (
	"context"
	"fmt"
)

func ProcessFailedMessages(ctx context.Context, FailedMessages <-chan FailedMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-FailedMessages:

			fmt.Printf("Failed records:\n")
			for _, record := range msg.Data {
				fmt.Printf("%v\n", record)
			}
			fmt.Printf("Errors:\n")
			for _, err := range msg.Errors {
				fmt.Printf("%v\n", err)
			}
		}
	}
}
