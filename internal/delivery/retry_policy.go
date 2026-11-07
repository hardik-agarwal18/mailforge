package delivery

import (
	"fmt"
	"math/rand"
	"time"
)

// RetryPolicy controls how long temporary delivery failures remain queued.
// Values are configurable because SMTP retry behaviour is an operational
// policy, not a property of an individual SMTP response code.
type RetryPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	MaxQueueAge  time.Duration
	MaxAttempts  int
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		InitialDelay: 30 * time.Minute,
		MaxDelay:     3 * time.Hour,
		MaxQueueAge:  5 * 24 * time.Hour,
		MaxAttempts:  100,
	}
}

func (p RetryPolicy) Validate() error {
	if p.InitialDelay <= 0 {
		return fmt.Errorf("retry initial delay must be positive")
	}
	if p.MaxDelay < p.InitialDelay {
		return fmt.Errorf("retry max delay must be greater than or equal to initial delay")
	}
	if p.MaxQueueAge <= 0 {
		return fmt.Errorf("retry maximum queue age must be positive")
	}
	if p.MaxAttempts < 1 {
		return fmt.Errorf("retry maximum attempts must be at least 1")
	}
	return nil
}

// DelayForAttempt returns exponential backoff with bounded 10% jitter. The
// first retry only adds jitter so it never falls below InitialDelay; later
// retries use +/- jitter but remain between InitialDelay and MaxDelay.
func (p RetryPolicy) DelayForAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := p.InitialDelay
	for i := 1; i < attempt && delay < p.MaxDelay; i++ {
		if delay >= p.MaxDelay/2 {
			delay = p.MaxDelay
			break
		}
		delay *= 2
	}
	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}

	spread := delay / 10
	if spread <= 0 {
		return delay
	}

	if attempt == 1 {
		// Keep the first retry at or above the RFC 5321 general guidance of
		// 30 minutes while still spreading simultaneous first retries.
		delay += time.Duration(rand.Int63n(int64(spread) + 1))
	} else {
		// Later retries use bounded +/- jitter. The initial interval remains
		// the absolute floor and MaxDelay remains the absolute ceiling.
		offset := time.Duration(rand.Int63n(int64(spread)*2+1)) - spread
		delay += offset
	}

	if delay < p.InitialDelay {
		delay = p.InitialDelay
	}
	if delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	return delay
}
