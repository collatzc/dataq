// +build darwin

package dataq

import "time"

const (
	// `parseTime=false`
	// DateTimeFormat = "2006-01-02 15:04:05"
	// `parseTime=true`
	DateTimeFormat = time.RFC3339
)
