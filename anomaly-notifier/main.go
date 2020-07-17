package main

import (
	"aws-iot-assignment/anomalynotifier/notifier"
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)


func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, event events.KinesisFirehoseEvent) (string, error) {

	for _, record := range event.Records {
		payload := string(record.Data)
		fmt.Println("payload ==>", payload)
		err := notifier.ProcessEvent(ctx, payload)
		if err != nil {
			fmt.Errorf("error processing kinesis events: %v", err)
		}
	}

	return fmt.Sprintf("successfully processed event"), nil
}

