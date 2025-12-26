package internal

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

func StartLogWatcher(
	ctx context.Context, cm *CheckpointManager, client *cloudwatchlogs.Client,
	gc GroupConfig, conf Config,
) {
	lastTS := cm.Get(gc.Name)
	log.Printf("[%s] Watcher started at: %v", gc.Name, time.UnixMilli(lastTS))

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] LogWatcher stopping... %v", gc.Name, time.UnixMilli(lastTS))
			return
		default:
			// 1. Perform the sync work
			// This call is synchronous. The loop waits here until it's finished.
			newTS, err := pollAndForwardLog(ctx, cm, client, gc, conf, lastTS)
			if err != nil {
				log.Printf("[%s] Sync failed: %v", gc.Name, err)
				// If AWS throttles or network is down, wait a bit longer before retrying
				time.Sleep(time.Second * 5)
			} else {
				lastTS = newTS
			}

			// 2. Sequential Gap: Wait for the interval AFTER the work is done
			// This is the "Breath Period" that prevents overlapping.
			select {
			case <-time.After(conf.PollInterval):
				// Just continue to the next loop
			case <-ctx.Done():
				log.Printf("[%s] LogWatcher pollAndForwardLog stopping... %v",
					gc.Name, time.UnixMilli(lastTS))
				return
			}
		}
	}
}

func pollAndForwardLog(
	ctx context.Context, cm *CheckpointManager, client *cloudwatchlogs.Client,
	gc GroupConfig, conf Config, lastTS int64,
) (int64, error) {
	input := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName: &gc.Name,
		StartTime:    &lastTS,
	}

	// If a single batch is huge, CloudWatch returns a NextToken.
	// We should handle pagination to ensure we don't skip data in one tick.
	currentTS := lastTS

	log.Printf("[%s] pollAndForwardLog: %v", gc.Name, time.UnixMilli(currentTS))
	processed := 0
	for {
		output, err := client.FilterLogEvents(ctx, input)
		if err != nil {
			return currentTS, fmt.Errorf("[%s] pollAndForwardLog FilterLogEvents: %w", gc.Name, err)
		}

		if len(output.Events) == 0 {
			return currentTS + 1, nil
		}

		for _, event := range output.Events {
			if *event.Timestamp >= currentTS {
				// Add 1ms to avoid re-reading the same event next time
				currentTS = *event.Timestamp + 1
			}

			processLogEvent(ctx, event, gc, conf)
		}

		processed += len(output.Events)
		cm.Save(gc.Name, currentTS)
		log.Printf("[%s] Log events processed: %d@%s, next: %v",
			gc.Name, processed, time.UnixMilli(currentTS), output.NextToken != nil)
		processed = 0

		// Pagination: if batch is large, follow the token
		if output.NextToken == nil {
			break
		}
		input.NextToken = output.NextToken

		// Monitor context during long pagination
		select {
		case <-ctx.Done():
			return currentTS, ctx.Err()
		default:
			continue
		}
	}

	return currentTS, nil
}

func processLogEvent(ctx context.Context, event types.FilteredLogEvent, gc GroupConfig, conf Config) {
	raw := *event.Message
	ts := *event.Timestamp
	stream := *event.LogStreamName

	switch gc.Type {
	case LogTypeFlink:
		// Sentry: only ERROR/Exception
		if gc.SentryEnabled && conf.SentryDSN != "" && isErrorMessage(raw) {
			sendFlinkLogToSentry(raw, conf.Env, gc.Name, stream)
		}

		// Vector: all
		if gc.VectorEnabled && conf.VectorEndpoint != "" {
			sendFlinkLogToVector(ctx, conf, gc, raw, stream, ts)
		}
	default:
	}
}

func isErrorMessage(msg string) bool {
	upper := strings.ToUpper(msg)
	return strings.Contains(upper, "ERROR") ||
		strings.Contains(upper, "EXCEPTION") ||
		strings.Contains(upper, "FATAL")
}
