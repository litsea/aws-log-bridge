package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/getsentry/sentry-go"
)

var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

func sendFlinkLogToSentry(raw, env, group, stream string) {
	var f FlinkLog
	err := json.Unmarshal([]byte(raw), &f)

	event := sentry.NewEvent()
	event.Level = sentry.LevelError
	event.Environment = env
	event.Tags = map[string]string{
		"log_group":  group,
		"log_stream": stream,
	}

	if err == nil {
		// Structured JSON Logic
		event.Message = f.Message
		event.Tags["logger"] = f.Logger
		event.Tags["app_arn"] = f.ApplicationARN

		// Custom Fingerprint to prevent issue explosion
		// Group by Logger + Code Location
		event.Fingerprint = []string{f.Logger, f.LocationInformation}

		event.Exception = []sentry.Exception{{
			Type:  f.Logger,
			Value: f.Message,
			Stacktrace: &sentry.Stacktrace{
				Frames: []sentry.Frame{{
					Function: f.LocationInformation,
					Module:   f.Logger,
				}},
			},
		}}

		event.Contexts = map[string]sentry.Context{
			"Flink Metadata": {
				"thread":   f.ThreadName,
				"location": f.LocationInformation,
				"stack":    f.ThrowableInformation, // Full stack in context
			},
		}
	} else {
		// Plain text fallback
		event.Message = raw
		event.Fingerprint = []string{"raw-text-fallback", group}
	}

	sentry.CaptureEvent(event)
}

func sendFlinkLogToVector(
	ctx context.Context, conf Config, gc GroupConfig, rawMsg, stream string, timestamp int64,
) {
	// 1. Prepare the base map for final JSON
	finalMap := make(map[string]interface{})

	// 2. Try to parse Flink original JSON
	var flinkRaw map[string]interface{}
	err := json.Unmarshal([]byte(rawMsg), &flinkRaw)

	// Fixed fields: time (RFC3339 format)
	finalMap["time"] = time.UnixMilli(timestamp).UTC().Format("2006-01-02T15:04:05.000000Z")

	if err == nil {
		// --- IF IT IS JSON: Map Flink fields to Vector fields ---

		// Fixed fields: level (mapping from Flink's messageType)
		if l, ok := flinkRaw["messageType"]; ok {
			finalMap["level"] = l
		} else {
			finalMap["level"] = "INFO"
		}

		// Fixed fields: msg (mapping from Flink's message)
		if m, ok := flinkRaw["message"]; ok {
			finalMap["log"] = m
		} else {
			finalMap["log"] = rawMsg
		}

		// Move all other Flink fields into finalMap
		for k, v := range flinkRaw {
			if k != "messageType" && k != "message" {
				finalMap[k] = v
			}
		}
	} else {
		// --- IF IT IS PLAIN TEXT ---
		finalMap["level"] = "INFO"
		finalMap["log"] = rawMsg
	}

	// 3. Add Metadata fields
	finalMap["log_group"] = gc.Name
	finalMap["log_stream"] = stream
	finalMap["container_name"] = gc.Service

	// 4. Marshal and Send
	data, _ := json.Marshal(finalMap)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		conf.VectorEndpoint, bytes.NewBuffer(data))
	if err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")
	if conf.VectorAuth.Username != "" {
		req.SetBasicAuth(conf.VectorAuth.Username, conf.VectorAuth.Password)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("[Vector] connection error: %v", err)
		return
	}

	// FIX (errcheck): properly handle body close
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 400 {
		log.Printf("[Vector] rejected with status: %d", resp.StatusCode)
	}
}
