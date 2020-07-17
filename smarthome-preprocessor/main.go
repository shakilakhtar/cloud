package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"io/ioutil"
	"log"
	"smarthome-preprocessor/preprocessor"
)

//var fileName = flag.String("eventfile", "events.json", "file location containing events")

func main() {
	//rm := getTestEvent(*fileName)
	//event := new(events.KinesisEvent)
	//e := json.Unmarshal([]byte(rm), event)
	//if e != nil {
	//	fmt.Println("Marshalling error for message ", rm)
	//	fmt.Errorf("Marshalling message error : %v", e)
	//}
	//or, err := Handler(context.Background(), *event)
	//if err != nil {
	//	fmt.Errorf("got error: %v", err)
	//	return
	//}
	//fmt.Println("successfully processed kinesis event",or)
	lambda.Start(Handler)
}

func Handler(ctx context.Context, event events.KinesisEvent) (events.KinesisFirehoseResponse, error) {
	var response events.KinesisFirehoseResponse
	var err error
	for _, record := range event.Records {
		payload := string(record.Kinesis.Data)
		log.Print("Firehose payload ==>", payload)
		data := new(preprocessor.EventData)
		err = json.Unmarshal([]byte(payload), data)
		if err != nil {
			log.Fatalln("json marshalling  error  ", err)
		}
		//check temperature value to find anomalies
		//Anomaly is defined as +/-5ºF delta between the top floor temperature and the thermostat set temperature
		currentTemp := data.Temperature
		desiredTemp := preprocessor.GetTemperature()
		//filter record
		if currentTemp-desiredTemp >= 5 || currentTemp-desiredTemp <= -5 {
			transformedRecord, err := buildResponseRecord(record)
			if err != nil {
				log.Fatalln(err)
			}

			response.Records = append(response.Records, transformedRecord)
		}

	}

	return response, err
}

func buildResponseRecord(record events.KinesisEventRecord) (events.KinesisFirehoseResponseRecord, error) {
	var respRecord events.KinesisFirehoseResponseRecord

	//data, err := transform(record.Data, record.ApproximateArrivalTimestamp)
	//if err != nil {
	//	return respRecord, err
	//}

	respRecord.RecordID = record.EventID
	respRecord.Result = events.KinesisFirehoseTransformedStateOk
	respRecord.Data = record.Data //[]byte(string(data))

	return respRecord, nil
}

//Test function from local events
func getTestEvent(fileName string) (s string) {

	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Errorf("Error in getting event from file", err)
	}
	text := string(content)

	return text
}
