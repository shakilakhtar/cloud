package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"log"
	"smarthome-transformer/transformer"
	"strings"
)

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, event events.KinesisFirehoseEvent) (events.KinesisFirehoseResponse, error) {
	var response events.KinesisFirehoseResponse
	var err error
	for _, record := range event.Records {
		payload := string(record.Data)
		log.Print("firehose payload", payload)
		data := new(transformer.EventData)
		err = json.Unmarshal([]byte(payload), data)
		if err != nil {
			log.Fatalln("json marshalling  error  ", err)
		}
		//check temperature value to find anomalies
		//Anomaly is defined as +/-5ºF delta between the top floor temperature and the thermostat set temperature
		currentTemp := data.Temperature
		desiredTemp := transformer.GetTemperature()
		//filter record
		if currentTemp-desiredTemp >= 5 || currentTemp-desiredTemp <= -5 {
			transformedRecord, err := buildResponseRecord(record)
			if err != nil {
				log.Fatalln(err)
			}

			response.Records = append(response.Records, transformedRecord)
		}

	}

	return response, nil
}

func buildResponseRecord(record events.KinesisFirehoseEventRecord) (events.KinesisFirehoseResponseRecord, error) {
	var transformedRecord events.KinesisFirehoseResponseRecord
	transformedRecord.RecordID = record.RecordID
	transformedRecord.Result = events.KinesisFirehoseTransformedStateOk
	transformedRecord.Data = record.Data

	return transformedRecord, nil
}

func decode(dataBytes []byte) ([]*transformer.EventData, error) {
	decoder := json.NewDecoder(strings.NewReader(string(dataBytes)))

	var records []*transformer.EventData
	for decoder.More() {
		var d transformer.EventData
		err := decoder.Decode(&d)
		if err != nil {
			return nil, err
		}
		records = append(records, &d)
	}

	return records, nil
}

func encode(objects []*transformer.EventData) ([]byte, error) {
	var outBytes bytes.Buffer
	encoder := json.NewEncoder(&outBytes)

	for _, obj := range objects {
		err := encoder.Encode(&obj)
		if err != nil {
			return nil, err
		}
	}

	return outBytes.Bytes(), nil
}
