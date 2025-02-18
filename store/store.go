package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// --------------------- Data Types ---------------------

// KeyData represents the stored data for a key.
type KeyData struct {
	Value            int   `json:"value"`
	Expires          int64 `json:"expires"`
	UnpublishedDelta int   `json:"unpublishedDelta"`
}

// KeyPatch is passed to update a key.
type KeyPatch struct {
	Increment  int `json:"increment"`
	TTLSeconds int `json:"ttlSeconds"`
}

// NodeUpdateRecord is used when merging broadcast update packages.
type NodeUpdateRecord struct {
	CurrentServerCount int   `json:"currentServerCount"`
	Delta              int   `json:"delta"`
	Expires            int64 `json:"expires"`
}

// NodeUpdatePatch represents a set of node updates keyed by key id.
type NodeUpdatePatch map[string]NodeUpdateRecord

// --------------------- Internal Structures ---------------------

// KeyRecord wraps a KeyData along with a per‑key mutex.
type KeyRecord struct {
	mu   sync.Mutex
	data *KeyData
}

// --------------------- The Store Component ---------------------

// Store encapsulates all the business logic for updating keys,
// processing node updates and broadcasting updates via a callback.
type Store struct {
	// dataStore holds a mapping from key to KeyRecord.
	dataStore sync.Map // map[string]*KeyRecord

	// publishSet holds keys that require publication.
	publishSet sync.Map // map[string]bool

	// postUpdateQueues holds, for each target, the accumulated NodeUpdatePatch.
	postUpdateQueues   map[string]NodeUpdatePatch
	postUpdateQueuesMu sync.Mutex

	// activeTargets contains the list of broadcast target identifiers (e.g. URLs).
	activeTargets []string
	// BroadcastCallback is invoked with a target and a batch of updates.
	BroadcastCallback func(target string, updates NodeUpdatePatch) error

	// Metrics counters.
	TotalRequests           int64
	TotalKeyGetRequests     int64
	TotalKeyPatchRequests   int64
	TotalNodeUpdateRequests int64
	TotalUpdateAttempts     int64
	TotalUpdateSent         int64
}

// NewStore constructs a new Store—activeTargets is the list of broadcast targets,
// and cb is the callback function that should be invoked during broadcast.
func NewStore(activeTargets []string, cb func(target string, updates NodeUpdatePatch) error) *Store {
	return &Store{
		activeTargets:           activeTargets,
		BroadcastCallback:       cb,
		postUpdateQueues:        make(map[string]NodeUpdatePatch),
		TotalRequests:           0,
		TotalKeyGetRequests:     0,
		TotalKeyPatchRequests:   0,
		TotalNodeUpdateRequests: 0,
		TotalUpdateAttempts:     0,
		TotalUpdateSent:         0,
	}
}

// --------------------- Public Methods ---------------------

// Get returns the key data for the given key using an eventually consistent read.
func (s *Store) Get(key string) (*KeyData, bool) {
	atomic.AddInt64(&s.TotalRequests, 1)
	v, ok := s.dataStore.Load(key)
	if !ok {
		return nil, false
	}
	rec := v.(*KeyRecord)
	// Note: For eventual consistency, we are not locking here.
	if rec.data == nil || rec.data.Expires < time.Now().UnixMilli() {
		s.dataStore.Delete(key)
		return nil, false
	}
	return rec.data, true
}

// Patch processes a patch (update) to a given key.
func (s *Store) Patch(key string, patch KeyPatch) *KeyData {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalKeyPatchRequests, 1)
	now := time.Now().UnixMilli()
	var rec *KeyRecord
	v, ok := s.dataStore.Load(key)
	if !ok {
		rec = &KeyRecord{
			data: &KeyData{
				Value:            0,
				Expires:          now + int64(patch.TTLSeconds)*1000,
				UnpublishedDelta: 0,
			},
		}
		s.dataStore.Store(key, rec)
	} else {
		rec = v.(*KeyRecord)
	}
	// Lock this key for update.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.data == nil || rec.data.Expires < now {
		rec.data = &KeyData{
			Value:            0,
			Expires:          now + int64(patch.TTLSeconds)*1000,
			UnpublishedDelta: 0,
		}
	}
	rec.data.Value += patch.Increment
	rec.data.UnpublishedDelta += patch.Increment
	// Mark the key as pending for network broadcast.
	s.publishSet.Store(key, true)
	return rec.data
}

// NodeUpdate processes an incoming node update patch.
func (s *Store) NodeUpdate(patch NodeUpdatePatch) {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalNodeUpdateRequests, 1)
	now := time.Now().UnixMilli()
	for key, update := range patch {
		if update.Expires < now {
			continue
		}
		var rec *KeyRecord
		v, ok := s.dataStore.Load(key)
		if !ok {
			rec = &KeyRecord{
				data: &KeyData{
					Value:            update.CurrentServerCount,
					Expires:          update.Expires,
					UnpublishedDelta: 0,
				},
			}
			s.dataStore.Store(key, rec)
		} else {
			rec = v.(*KeyRecord)
		}
		rec.mu.Lock()
		if rec.data == nil || rec.data.Expires < now {
			rec.data = &KeyData{
				Value:            update.CurrentServerCount,
				Expires:          update.Expires,
				UnpublishedDelta: 0,
			}
		} else {
			if rec.data.Expires > update.Expires {
				rec.data.Expires = update.Expires
			}
			rec.data.Value += update.Delta
			if rec.data.Value < update.CurrentServerCount {
				rec.data.Value = update.CurrentServerCount
			}
		}
		rec.mu.Unlock()
	}
}

// broadcastOnce processes the publishSet (collecting per-key updates),
// merges these updates into per-target queues, and then calls the broadcast callback.
func (s *Store) broadcastOnce() {
	now := time.Now().UnixMilli()
	updates := make(NodeUpdatePatch)

	// Process keys pending publication.
	s.publishSet.Range(func(key, _ interface{}) bool {
		keyStr := key.(string)
		v, ok := s.dataStore.Load(keyStr)
		if ok {
			rec := v.(*KeyRecord)
			rec.mu.Lock()
			if rec.data != nil && rec.data.Expires >= now {
				updates[keyStr] = NodeUpdateRecord{
					CurrentServerCount: rec.data.Value,
					Delta:              rec.data.UnpublishedDelta,
					Expires:            rec.data.Expires,
				}
				// Reset the unpublished delta.
				rec.data.UnpublishedDelta = 0
			}
			rec.mu.Unlock()
		}
		s.publishSet.Delete(key)
		return true
	})

	// Merge these updates into the per-target update queues.
	s.postUpdateQueuesMu.Lock()
	for _, target := range s.activeTargets {
		existing := s.postUpdateQueues[target]
		merged := mergeNodeUpdates(existing, updates)
		s.postUpdateQueues[target] = merged
	}
	s.postUpdateQueuesMu.Unlock()

	// For each target, if there are queued updates then call the callback.
	for _, target := range s.activeTargets {
		s.postUpdateQueuesMu.Lock()
		queue, ok := s.postUpdateQueues[target]
		if !ok || len(queue) == 0 {
			s.postUpdateQueuesMu.Unlock()
			continue
		}
		s.postUpdateQueuesMu.Unlock()
		atomic.AddInt64(&s.TotalUpdateAttempts, 1)
		if s.BroadcastCallback != nil {
			if err := s.BroadcastCallback(target, queue); err == nil {
				atomic.AddInt64(&s.TotalUpdateSent, 1)
				s.postUpdateQueuesMu.Lock()
				s.postUpdateQueues[target] = make(NodeUpdatePatch)
				s.postUpdateQueuesMu.Unlock()
			} else {
				// In a real system, you might log or retry.
				fmt.Printf("Broadcast error for target %s: %v\n", target, err)
			}
		}
	}
}

// RunBroadcastLoop runs the broadcast loop every interval.
// It can be stopped by closing stopCh.
func (s *Store) RunBroadcastLoop(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.broadcastOnce()
		case <-stopCh:
			return
		}
	}
}

// GetQueueSizes returns (for each target) the number of updates currently queued.
func (s *Store) GetQueueSizes() map[string]int {
	result := make(map[string]int)
	s.postUpdateQueuesMu.Lock()
	for target, queue := range s.postUpdateQueues {
		result[target] = len(queue)
	}
	s.postUpdateQueuesMu.Unlock()
	return result
}

// GetMetrics returns a snapshot of the internal metrics.
func (s *Store) GetMetrics() (totalReq, getReq, patchReq, nodeUpdateReq, updateAttempts, updateSent int64) {
	totalReq = atomic.LoadInt64(&s.TotalRequests)
	getReq = atomic.LoadInt64(&s.TotalKeyGetRequests)
	patchReq = atomic.LoadInt64(&s.TotalKeyPatchRequests)
	nodeUpdateReq = atomic.LoadInt64(&s.TotalNodeUpdateRequests)
	updateAttempts = atomic.LoadInt64(&s.TotalUpdateAttempts)
	updateSent = atomic.LoadInt64(&s.TotalUpdateSent)
	return
}

// --------------------- Helpers ---------------------

// mergeNodeUpdates merges two NodeUpdatePatch maps.
func mergeNodeUpdates(oldUpdate, newUpdate NodeUpdatePatch) NodeUpdatePatch {
	result := make(NodeUpdatePatch)
	now := time.Now().UnixMilli()
	// Add updates from newUpdate that are not expired.
	for k, update := range newUpdate {
		if update.Expires > now {
			result[k] = update
		}
	}
	// Merge with existing oldUpdate.
	if oldUpdate != nil {
		for k, oldVal := range oldUpdate {
			if oldVal.Expires <= now {
				continue
			}
			newVal, exists := newUpdate[k]
			if !exists {
				result[k] = oldVal
			} else {
				result[k] = NodeUpdateRecord{
					CurrentServerCount: max(oldVal.CurrentServerCount, newVal.CurrentServerCount),
					Delta:              oldVal.Delta + newVal.Delta,
					Expires:            minInt64(oldVal.Expires, newVal.Expires),
				}
			}
		}
	}
	return result
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
