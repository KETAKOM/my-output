package etl

import "time"

type Record struct {
	ID         int64
	Date       time.Time
	Timestamp  time.Time
	DeviceType string
	EventType  string
	Message    string
}

type Records []Record
