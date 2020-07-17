package notifier

//Get default temperature from configuration
var defaultTemp = 65.0
var current = 0.0

//Function will give thermostat current temperature. The default value should be from groud floor thermostat
func GetTemperature() float64 {
	temp := current
	if temp == 0.0 {
		temp = defaultTemp
	}

	return temp
}

//Update temperature to desired value
func SetTemperature(temp float64) error {
	current = temp
	return nil
}
