package messageprocessor

import (
	"context"
	"math"
	"time"

	"github.com/pkg/errors"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/rs/xstats"
)

// MaxRetries is the maximum number of time's we'd like to retry processing a record before quitting
const consumerRetriesExceeded = "kinesis.consumer_error.retries_exceeded"

// MaxRetriesExceededError implements MessageProcessorError and is used to indicate to upstream application/
// decorators that retries have been attempted and exhausted
type MaxRetriesExceededError struct {
	Retryable bool
	OrigErr   error
	Wait      int
}

func (t MaxRetriesExceededError) Error() string {
	return t.OrigErr.Error()
}

// IsRetryable indicates whether or not the JiraClient Error that was returned should be retried
func (t MaxRetriesExceededError) IsRetryable() bool {
	return t.Retryable
}

// RetryAfter is not relevant in context of MaxRetriesExceededError as IsRetryable is set to false
func (t MaxRetriesExceededError) RetryAfter() int {
	return t.Wait
}

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
func (t *RetryableMessageProcessor) ProcessMessage(ctx context.Context, record *kinesis.Record) MessageProcessorError {
	stat := xstats.FromContext(ctx)
	var messageProcErr MessageProcessorError
	var attemptNum int
	for attemptNum < t.maxAttempts {
		messageProcErr = t.wrapped.ProcessMessage(ctx, record)
		if messageProcErr != nil && messageProcErr.IsRetryable() {
			if messageProcErr.RetryAfter() > 0 {
				// Wait for duration specified in 'Retry-After'
				waitRetryAfter(messageProcErr.RetryAfter())
			} else {
				// Or perform exponential backoff
				waitToRetry(attemptNum)
			}
		} else {
			break
		}
		attemptNum++
	}

	if attemptNum >= t.maxAttempts {
		stat.Count(consumerRetriesExceeded, 1)
		maxRetriesExceededErr := MaxRetriesExceededError{
			Retryable: false,
			OrigErr:   errors.Wrap(messageProcErr, "Max retries exceeded"),
			Wait:      0,
		}
		return maxRetriesExceededErr
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

// waitRetryAfter is used to support a wait for exact duration as specified in 'Retry-After' header.
// The consumer of this library is responsible to obtain/compute duration of wait time
// by parsing underlying HTTP response and storing that info in
// MessageProcessorError.RetryAfter field
func waitRetryAfter(retryAfter int) {
	time.Sleep(time.Duration(retryAfter) * time.Second)
}
