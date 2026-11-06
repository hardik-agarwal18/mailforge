package smtp

import "time"

type MetricsRecorder interface {
	RecordConnectionOpened()
	RecordConnectionClosed()
	RecordSubmission(time.Duration, bool)
	RecordAuthentication(bool)
}
