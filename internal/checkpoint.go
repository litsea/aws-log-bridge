package internal

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const checkpointFile = "data/checkpoints.json"

// CheckpointManager handles persistent state
type CheckpointManager struct {
	mu          sync.RWMutex
	LastOffsets map[string]int64
}

func NewCheckpointManager() *CheckpointManager {
	cm := &CheckpointManager{
		LastOffsets: make(map[string]int64),
	}

	data, err := os.ReadFile(checkpointFile)
	if err == nil {
		if err := json.Unmarshal(data, &cm.LastOffsets); err != nil {
			log.Printf("Warning: failed to parse checkpoint file: %v", err)
		}
	}
	return cm
}

func (cm *CheckpointManager) Save(group string, ts int64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.LastOffsets[group] = ts

	// Ensure the data directory exists
	dir := filepath.Dir(checkpointFile)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		_ = os.MkdirAll(dir, 0o750)
	}

	// Fix: Save only the map to keep the JSON flat and readable
	data, _ := json.MarshalIndent(cm.LastOffsets, "", "  ")

	// Atomic write: write to .tmp then rename
	tmpFile := checkpointFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o600); err == nil {
		_ = os.Rename(tmpFile, checkpointFile)
	}
}

func (cm *CheckpointManager) Get(group string) int64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// 1. Calculate the boundary for "too old" (e.g., 1 hour ago)
	maxBacklog := time.Now().Add(-1 * time.Hour).UnixMilli()

	ts, ok := cm.LastOffsets[group]

	// 2. If no checkpoint exists, use the default (1 minute ago)
	if !ok {
		return time.Now().Add(-1 * time.Minute).UnixMilli()
	}

	// 3. If checkpoint is too old, cap it to the max backlog boundary
	if ts < maxBacklog {
		log.Printf("[%s] Checkpoint is too old (%v), capping to 1 hour ago", group, time.UnixMilli(ts))
		return maxBacklog
	}

	return ts
}
