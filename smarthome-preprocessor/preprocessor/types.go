package preprocessor

type EventData struct {
	Timestamp   string  `json:"timestamp"`
	Humidity    float64 `json:"humidity"`
	Location    string  `json:"location"`
	Temperature float64 `json:"temperature"`
}

type OutputRecord struct {
	RecordId string `json:"recordId"`
	Result   string `json:"result"`
	Data     []byte `json:"data"`
}
