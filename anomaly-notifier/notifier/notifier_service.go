package notifier

import (
	"context"
	"encoding/json"
	"fmt"
)

// Delegate - call to thermostat mock service
func ProcessEvent(ctx context.Context, event interface{}) error {
	payload := event.(string)
	msg := new(ThermostatEvent)
	e := json.Unmarshal([]byte(payload), msg)
	if e != nil {
		fmt.Errorf("Marshalling message error : %v", e)
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
	return nil
}
