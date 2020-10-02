package messageprocessor

import (
	"context"
	"math"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/rs/xstats"
)

// MaxRetries is the maximum number of time's we'd like to retry processing a record before quitting
const consumerRetriesExceeded = "kinesis.consumer_error.retries_exceeded"

// RetryableMessageProcessor is a `MessageProcessor` decorator that re-attempts
// processing of messages 'maxAttempts' number of times in case of failures
// 'maxAttempts' is a configurable parameter which can be set by consumer of this lib
// Exponential backoff has been implemented as a retry mechanism
type RetryableMessageProcessor struct {
	maxAttempts int
	wrapped     MessageProcessor
}

// ProcessMessage invokes the wrapped `MessageProcessor`. Attempts retries using exponential backoff
// if underlying 'MessageProcessor' returns an error.
// If 'maxAttempts' are exceeded without successful processing, it emits a stat indicating the same
func (t *RetryableMessageProcessor) ProcessMessage(ctx context.Context, record *kinesis.Record) MessageProcessError {
	stat := xstats.FromContext(ctx)
	var messageProcErr MessageProcessError
	var attemptNum int
	for attemptNum < t.maxAttempts {
		messageProcErr = t.wrapped.ProcessMessage(ctx, record)
		if messageProcErr != nil && messageProcErr.Error() != nil && messageProcErr.IsRetryable() {
			waitToRetry(attemptNum)
		} else {
			break
		}
		attemptNum++
	}

	if attemptNum >= t.maxAttempts {
		stat.Count(consumerRetriesExceeded, 1)
	}
	return messageProcErr
}

// NewRetryableMessageProcessor returns a function that wraps a `messageprocessor.MessageProcessor` in a
// `RetryableMessageProcessor` `messageprocessor.MessageProcessor`.
func NewRetryableMessageProcessor(attempts int) func(MessageProcessor) MessageProcessor {
	return func(next MessageProcessor) MessageProcessor {
		return &RetryableMessageProcessor{maxAttempts: attempts, wrapped: next}
	}
}

// waitToRetry is used to perform an exponential backoff for http calls
// that need to be retried. It waits for 2^attemptNum seconds
func waitToRetry(attemptNum int) {
	timeToWait := math.Pow(2, float64(attemptNum))
	time.Sleep(time.Duration(timeToWait) * time.Second)
}
