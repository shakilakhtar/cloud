package notifier

type ThermostatEvent struct {
	Temperature float64
	Humidity    float64
	Timestamp   string
	Location    string
}
