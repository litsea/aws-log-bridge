package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

type workerQueue struct {
	mu                sync.RWMutex
	EventChan         chan types.FilteredLogEvent
	CheckpointManager *CheckpointManager
	Config            Config
	GroupConfig       GroupConfig
	LastSaveTS        int64
	Processed         int64
}

func StartLogWatcher(
	ctx context.Context, cm *CheckpointManager, client *cloudwatchlogs.Client,
	gc GroupConfig, conf Config,
) {
	lastTS := cm.Get(gc.Name)
	log.Printf("[%s] Watcher started at: %v", gc.Name, time.UnixMilli(lastTS))

	wq := &workerQueue{
		EventChan:         make(chan types.FilteredLogEvent, 10000),
		CheckpointManager: cm,
		Config:            conf,
		GroupConfig:       gc,
		LastSaveTS:        lastTS,
	}
	for range 20 {
		go processLogWorker(ctx, wq)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] LogWatcher stopping... %v", gc.Name, time.UnixMilli(lastTS))
			return
		default:
			// 1. Perform the sync work
			// This call is synchronous. The loop waits here until it's finished.
			newTS, err := pollAndForwardLog(ctx, cm, client, wq, lastTS)
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
	ctx context.Context, _ *CheckpointManager, client *cloudwatchlogs.Client,
	wq *workerQueue, lastTS int64,
) (int64, error) {
	input := &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName: &wq.GroupConfig.Name,
		StartTime:    &lastTS,
	}

	// If a single batch is huge, CloudWatch returns a NextToken.
	// We should handle pagination to ensure we don't skip data in one tick.
	currentTS := lastTS

	log.Printf("[%s] pollAndForwardLog: %v", wq.GroupConfig.Name, time.UnixMilli(currentTS))
	for {
		output, err := client.FilterLogEvents(ctx, input)
		if err != nil {
			return currentTS, fmt.Errorf("[%s] pollAndForwardLog FilterLogEvents: %w",
				wq.GroupConfig.Name, err)
		}

		if len(output.Events) == 0 {
			return currentTS + 1, nil
		}

		for _, event := range output.Events {
			if *event.Timestamp >= currentTS {
				// Add 1ms to avoid re-reading the same event next time
				currentTS = *event.Timestamp + 1
			}

			select {
			case wq.EventChan <- event:
			case <-ctx.Done():
				return currentTS, ctx.Err()
			}
		}

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

func processLogWorker(ctx context.Context, wq *workerQueue) {
	var (
		flinkBatch []string
		lastTS     int64
	)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event := <-wq.EventChan:
			raw := *event.Message
			ts := *event.Timestamp
			stream := *event.LogStreamName

			switch wq.GroupConfig.Type {
			case LogTypeFlink:
				// Sentry: only ERROR/Exception
				if wq.GroupConfig.SentryEnabled && wq.Config.SentryDSN != "" && isErrorMessage(raw) {
					sendFlinkLogToSentry(raw, wq.Config.Env, wq.GroupConfig.Name, stream)
				}

				// Vector: all
				if wq.GroupConfig.VectorEnabled && wq.Config.VectorEndpoint != "" {
					m := buildFlinkLogToVector(wq.GroupConfig, raw, stream, ts)
					data, _ := json.Marshal(m)
					flinkBatch = append(flinkBatch, string(data))
					lastTS = ts

					if len(flinkBatch) >= 200 {
						sendFlinkLogToVector(ctx, wq.Config, strings.Join(flinkBatch, "\n"))
						saveCheckpoint(wq, int64(len(flinkBatch)), lastTS)
						flinkBatch = nil
					}
				}
			default:
			}

		case <-ticker.C:
			if len(flinkBatch) > 0 {
				sendFlinkLogToVector(ctx, wq.Config, strings.Join(flinkBatch, "\n"))
				saveCheckpoint(wq, int64(len(flinkBatch)), lastTS)
				flinkBatch = []string{}
			}

		case <-ctx.Done():
			if len(flinkBatch) > 0 {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				sendFlinkLogToVector(shutdownCtx, wq.Config, strings.Join(flinkBatch, "\n"))
				cancel()

				saveCheckpoint(wq, int64(len(flinkBatch)), lastTS)
			}
			return
		}
	}
}

func saveCheckpoint(wq *workerQueue, processed, lastTS int64) {
	wq.mu.Lock()
	defer wq.mu.Unlock()

	wq.Processed += processed

	if lastTS > wq.LastSaveTS {
		wq.LastSaveTS = lastTS
		wq.CheckpointManager.Save(wq.GroupConfig.Name, wq.LastSaveTS)
		log.Printf("[%s] Log event processed: %d@%v",
			wq.GroupConfig.Name, wq.Processed,
			time.UnixMilli(wq.LastSaveTS))
	}
}

func isErrorMessage(msg string) bool {
	upper := strings.ToUpper(msg)
	return strings.Contains(upper, "ERROR") ||
		strings.Contains(upper, "EXCEPTION") ||
		strings.Contains(upper, "FATAL")
}
