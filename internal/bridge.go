package internal

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

type FlinkLog struct {
	ApplicationARN       string `json:"applicationARN"`
	ApplicationVersionId string `json:"applicationVersionId"`
	LocationInformation  string `json:"locationInformation"`
	Logger               string `json:"logger"`
	Message              string `json:"message"`
	MessageType          string `json:"messageType"`
	ThreadName           string `json:"threadName"`
	ThrowableInformation string `json:"throwableInformation"`
}

type Payload struct {
	Timestamp int64  `json:"timestamp"`
	LogGroup  string `json:"log_group"`
	LogStream string `json:"log_stream"`
	Message   string `json:"message"`
	Env       string `json:"env"`
}

func StartLogWatcher(ctx context.Context, client *cloudwatchlogs.Client, gc GroupConfig, global Config) {
	lastTimestamp := time.Now().UnixMilli()
	ticker := time.NewTicker(global.PollInterval)
	defer ticker.Stop()

	for range ticker.C {
		input := &cloudwatchlogs.FilterLogEventsInput{
			LogGroupName: &gc.Name,
			StartTime:    &lastTimestamp,
		}

		output, err := client.FilterLogEvents(ctx, input)
		if err != nil {
			log.Printf("[%s] Fetch error: %v", gc.Name, err)
			continue
		}

		for _, event := range output.Events {
			if *event.Timestamp >= lastTimestamp {
				lastTimestamp = *event.Timestamp + 1
			}

			msg := *event.Message
			stream := *event.LogStreamName
			ts := *event.Timestamp

			switch gc.Type {
			case LogTypeFlink:
				// Sentry: only ERROR/Exception
				if gc.SentryEnabled && isErrorMessage(msg) {
					sendFlinkLogToSentry(msg, global.Env, gc.Name, stream)
				}

				// Vector: all
				if gc.VectorEnabled {
					sendFlinkLogToVector(ctx, global, gc, msg, stream, ts)
				}
			default:
			}
		}
	}
}

func isErrorMessage(msg string) bool {
	upper := strings.ToUpper(msg)
	return strings.Contains(upper, "ERROR") ||
		strings.Contains(upper, "EXCEPTION") ||
		strings.Contains(upper, "FATAL")
}
