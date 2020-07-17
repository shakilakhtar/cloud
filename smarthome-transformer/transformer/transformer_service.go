package transformer

import (
	"context"
	"encoding/json"
	"fmt"
)

// Delegate - call to thermostat mock service
func Transform(ctx context.Context, event interface{}) error {
	payload := event.(string)
	msg := new(EventData)
	err := json.Unmarshal([]byte(payload), msg)
	if err != nil {
		fmt.Errorf("Marshalling message error : %v", err)
	}
	//check if the thermostat(groud floor) temperature is +/-5 from top floor temperature
	currentTemp := msg.Temperature
	t := GetTemperature()
	if currentTemp-t >= 5 {
		SetTemperature(t-5)
	}
	if currentTemp-t <= -5 {
		//send call to thermostat api
		SetTemperature(t-(-5))
	}
	return err
}
