package messageprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestRetryableMessageProcessor_ProcessMessageSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageProcessor := NewMockMessageProcessor(ctrl)

	retryableMessageProcessor := RetryableMessageProcessor{
		maxAttempts: 3,
		wrapped:     mockMessageProcessor,
	}

	incomingDataRecord := []byte(`{
		"type":"CHANNEL",
		"rawpayload":"{\"channel\":\"channelbob\",\"text\":\"the message\"}"
	}`)
	currentTime := time.Now()
	sequenceNumber := "12345"
	kinesisRecord := kinesis.Record{
		Data:                        incomingDataRecord,
		ApproximateArrivalTimestamp: &currentTime,
		SequenceNumber:              &sequenceNumber,
	}

	mockMessageProcessor.EXPECT().ProcessMessage(gomock.Any(), gomock.Any()).Return(nil)
	e := retryableMessageProcessor.ProcessMessage(context.Background(), &kinesisRecord)
	assert.Nil(t, e)
}

func TestRetryableMessageProcessor_ProcessMessageFailure_ExponentialBackOff(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageProcessor := NewMockMessageProcessor(ctrl)

	attempts := 4
	retryableMessageProcessor := RetryableMessageProcessor{
		maxAttempts: attempts,
		wrapped:     mockMessageProcessor,
	}

	testError := TestError{
		Retryable: true,
		OrigErr:   fmt.Errorf("TestError"),
	}

	incomingDataRecord := []byte(`{
		"type":"CHANNEL",
		"rawpayload":"{\"channel\":\"channelbob\",\"text\":\"the message\"}"
	}`)
	currentTime := time.Now()
	sequenceNumber := "12345"
	kinesisRecord := kinesis.Record{
		Data:                        incomingDataRecord,
		ApproximateArrivalTimestamp: &currentTime,
		SequenceNumber:              &sequenceNumber,
	}

	mockMessageProcessor.EXPECT().ProcessMessage(gomock.Any(), gomock.Any()).Return(testError).Times(attempts)
	e := retryableMessageProcessor.ProcessMessage(context.Background(), &kinesisRecord)
	assert.NotNil(t, e)
}


func TestRetryableMessageProcessor_ProcessMessageFailure_RetryAfter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageProcessor := NewMockMessageProcessor(ctrl)

	attempts := 4
	retryableMessageProcessor := RetryableMessageProcessor{
		maxAttempts: attempts,
		wrapped:     mockMessageProcessor,
	}

	testError := TestError{
		Retryable: true,
		OrigErr:   fmt.Errorf("TestError"),
		Wait:      2,
	}

	incomingDataRecord := []byte(`{
		"type":"CHANNEL",
		"rawpayload":"{\"channel\":\"channelbob\",\"text\":\"the message\"}"
	}`)
	currentTime := time.Now()
	sequenceNumber := "12345"
	kinesisRecord := kinesis.Record{
		Data:                        incomingDataRecord,
		ApproximateArrivalTimestamp: &currentTime,
		SequenceNumber:              &sequenceNumber,
	}

	mockMessageProcessor.EXPECT().ProcessMessage(gomock.Any(), gomock.Any()).Return(testError).Times(attempts)
	e := retryableMessageProcessor.ProcessMessage(context.Background(), &kinesisRecord)
	assert.NotNil(t, e)
}

// TestError implements MessageProcessError and contains a Retryable flag for retryable errors
type TestError struct {
	Retryable bool
	OrigErr   error
	Wait	   int
}

func (t TestError) Error() error {
	return t.OrigErr
}

// IsRetryable indicates whether or not the JiraClient Error that was returned should be retried
func (t TestError) IsRetryable() bool {
	return t.Retryable
}

func (t TestError) RetryAfter() int {
	return t.Wait
}
