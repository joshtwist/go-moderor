package store

import (
	"sync/atomic"
	"testing"
	"time"
)

// dummyBroadcastCallback is an example callback that succeeds.
func dummyBroadcastCallback(target string, updates NodeUpdatePatch) error {
	return nil
}

func TestPatchAndGet(t *testing.T) {
	s := NewStore([]string{"dummy"}, dummyBroadcastCallback)

	// Patch a key; value should be updated to 5.
	patch := KeyPatch{Increment: 5, TTLSeconds: 10}
	data := s.Patch("testKey", patch)
	if data.Value != 5 {
		t.Errorf("Expected value 5; got %d", data.Value)
	}

	// Get should return the same value.
	got, ok := s.Get("testKey")
	if !ok || got.Value != 5 {
		t.Errorf("Get did not return expected value; got %v (ok=%v)", got, ok)
	}
}

func TestNodeUpdate(t *testing.T) {
	s := NewStore([]string{"dummy"}, dummyBroadcastCallback)

	// First patch a key.
	s.Patch("nodeKey", KeyPatch{Increment: 10, TTLSeconds: 10})
	// Then simulate a node update that adds a delta.
	nodeUpdate := NodeUpdatePatch{
		"nodeKey": {
			CurrentServerCount: 15,
			Delta:              5,
			Expires:            time.Now().UnixMilli() + 5000,
		},
	}
	s.NodeUpdate(nodeUpdate)

	// Expect the value to become 15.
	got, ok := s.Get("nodeKey")
	if !ok || got.Value != 15 {
		t.Errorf("Expected value 15 after NodeUpdate; got %v (ok=%v)", got, ok)
	}
}

func TestBroadcast(t *testing.T) {
	// Create a channel to capture broadcast callback calls.
	callbackCh := make(chan struct {
		target  string
		updates NodeUpdatePatch
	}, 10)

	// Create a callback that sends data into the channel.
	cb := func(target string, updates NodeUpdatePatch) error {
		callbackCh <- struct {
			target  string
			updates NodeUpdatePatch
		}{target, updates}
		return nil
	}

	// Create a store with two active targets.
	targets := []string{"target1", "target2"}
	s := NewStore(targets, cb)

	// Patch some keys.
	s.Patch("key1", KeyPatch{Increment: 3, TTLSeconds: 10})
	s.Patch("key2", KeyPatch{Increment: 7, TTLSeconds: 10})

	// Run one round of broadcast.
	s.broadcastOnce()

	// Expect two callback invocations (one per target).
	count := 0
	timeout := time.After(1 * time.Second)
	for count < 2 {
		select {
		case msg := <-callbackCh:
			if _, ok := msg.updates["key1"]; !ok {
				t.Errorf("Expected update for key1 missing for target %s", msg.target)
			}
			if _, ok := msg.updates["key2"]; !ok {
				t.Errorf("Expected update for key2 missing for target %s", msg.target)
			}
			count++
		case <-timeout:
			t.Fatal("Timed out waiting for broadcast callback")
		}
	}
	// Verify that the update queues are cleared.
	queues := s.GetQueueSizes()
	for target, size := range queues {
		if size != 0 {
			t.Errorf("Expected queue for target %s to be 0; got %d", target, size)
		}
	}

	// Verify that metrics have been updated.
	_, _, _, _, updateAttempts, updateSent := s.GetMetrics()
	if atomic.LoadInt64(&s.TotalUpdateAttempts) == 0 || atomic.LoadInt64(&s.TotalUpdateSent) == 0 {
		t.Errorf("Update metrics not updated correctly; attempts: %d, sent: %d", updateAttempts, updateSent)
	}
}

func TestQueueSizes(t *testing.T) {
	s := NewStore([]string{"target1"}, dummyBroadcastCallback)

	// Manually add some updates to the queue.
	s.postUpdateQueuesMu.Lock()
	s.postUpdateQueues["target1"] = NodeUpdatePatch{
		"a": {CurrentServerCount: 5, Delta: 5, Expires: time.Now().UnixMilli() + 5000},
		"b": {CurrentServerCount: 7, Delta: 7, Expires: time.Now().UnixMilli() + 5000},
	}
	s.postUpdateQueuesMu.Unlock()

	qs := s.GetQueueSizes()
	if qs["target1"] != 2 {
		t.Errorf("Expected queue size 2 for target1; got %d", qs["target1"])
	}
}
