package preprocessor

import (
	"fmt"
	"os"
	"strconv"
)

//Get default temperature from configuration
var defaultTemp = 65.0
var current = os.Getenv("THERMOSTAT_TEMPERATURE")

//Function will give thermostat current temperature. The default value should be from groud floor thermostat
func GetTemperature() float64 {
	temp, _ := strconv.ParseFloat(current, 64)
	if temp == 0.0 {
		temp = defaultTemp
	}

	return temp
}

//Update temperature to desired value
func SetTemperature(temp float64) error {
	current = fmt.Sprintf("%f", temp)
	return nil
}
