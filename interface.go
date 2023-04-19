package messageprocessor

import (
	"context"

	"github.com/aws/aws-sdk-go/service/kinesis"
)

// MessageProcessor processes a consumed message. Implementors are responsible for unmarshalling the data
type MessageProcessor interface {
	ProcessMessage(ctx context.Context, record *kinesis.Record) Error
}

// Error represents an error that can be used to indicate to the consumer that an error should be retried
type Error interface {
	IsRetryable() bool
	Error() string
	RetryAfter() int
}
