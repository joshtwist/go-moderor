package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
)

// --------------------- Data Structures ---------------------

type KeyData struct {
	Value            int   `json:"value"`
	Expires          int64 `json:"expires"`
	UnpublishedDelta int   `json:"unpublishedDelta"`
}

type KeyPatch struct {
	Increment  int `json:"increment"`
	TTLSeconds int `json:"ttlSeconds"`
}

type NodeUpdateRecord struct {
	CurrentServerCount int   `json:"currentServerCount"`
	Delta              int   `json:"delta"`
	Expires            int64 `json:"expires"`
}

type NodeUpdatePatch map[string]NodeUpdateRecord

// --------------------- Instrumented Locks ---------------------

// Instrumented RWMutex to monitor acquisitions and waiting.
type InstrRWMutex struct {
	mu                sync.RWMutex
	totalAcquisitions int64
	waitCount         int64
}

func (m *InstrRWMutex) Lock() {
	start := time.Now()
	m.mu.Lock()
	atomic.AddInt64(&m.totalAcquisitions, 1)
	if elapsed := time.Since(start); elapsed > 10*time.Microsecond {
		atomic.AddInt64(&m.waitCount, 1)
	}
}

func (m *InstrRWMutex) Unlock() {
	m.mu.Unlock()
}

func (m *InstrRWMutex) RLock() {
	start := time.Now()
	m.mu.RLock()
	atomic.AddInt64(&m.totalAcquisitions, 1)
	if elapsed := time.Since(start); elapsed > 10*time.Microsecond {
		atomic.AddInt64(&m.waitCount, 1)
	}
}

func (m *InstrRWMutex) RUnlock() {
	m.mu.RUnlock()
}

// Instrumented Mutex to monitor acquisitions.
type InstrMutex struct {
	mu                sync.Mutex
	totalAcquisitions int64
	waitCount         int64
}

func (m *InstrMutex) Lock() {
	start := time.Now()
	m.mu.Lock()
	atomic.AddInt64(&m.totalAcquisitions, 1)
	if elapsed := time.Since(start); elapsed > 10*time.Microsecond {
		atomic.AddInt64(&m.waitCount, 1)
	}
}

func (m *InstrMutex) Unlock() {
	m.mu.Unlock()
}

// --------------------- Global Variables & Instrumented Locks ---------------------

var (
	// Data map and its instrumented mutex.
	data      = make(map[string]*KeyData)
	dataMutex = &InstrRWMutex{}

	// publishList is used as a set of keyIds that were updated.
	publishList      = make(map[string]struct{})
	publishListMutex = &InstrMutex{}

	// postUpdateQueues maps a port to a NodeUpdatePatch update queue.
	postUpdateQueues      = make(map[int]NodeUpdatePatch)
	postUpdateQueuesMutex = &InstrMutex{}

	// Define the complete port list.
	portList = []int{8000, 8001, 8002}

	// Will hold the list of ports excluding the current one.
	activePortList []int

	// The port on which this server is running.
	currentPort int

	// Global metrics counters.
	totalRequests           int64
	totalKeyGetRequests     int64
	totalKeyPatchRequests   int64
	totalNodeUpdateRequests int64
)

func main() {
	// Parse the command-line argument for port. Default to 8000 if not provided.
	if len(os.Args) < 2 {
		currentPort = 8000
	} else {
		p, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatalf("Invalid port argument: %v", err)
		}
		currentPort = p
	}

	// Build activePortList (all ports except currentPort).
	for _, p := range portList {
		if p != currentPort {
			activePortList = append(activePortList, p)
		}
	}

	// Start the background publishing loop.
	go publishUpdatesLoop()

	// Start heartbeat logging.
	go heartbeat()

	// Setup HTTP routes using gorilla/mux.
	r := mux.NewRouter()
	r.HandleFunc("/", handleRoot).Methods("GET")
	r.HandleFunc("/keys/{keyId}", handleGetKey).Methods("GET")
	r.HandleFunc("/keys/{keyId}", handlePatchKey).Methods("PATCH")
	r.HandleFunc("/node-update", handleNodeUpdate).Methods("POST")

	addr := ":" + strconv.Itoa(currentPort)
	log.Printf("Listening on port: %d", currentPort)
	log.Fatal(http.ListenAndServe(addr, r))
}

// --------------------- HTTP Handlers ---------------------

// GET "/" returns the full data as JSON.
func handleRoot(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&totalRequests, 1)
	dataMutex.RLock()
	defer dataMutex.RUnlock()

	// Copy data to avoid race conditions during JSON encoding.
	copyData := make(map[string]KeyData)
	for k, v := range data {
		copyData[k] = *v
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(copyData)
}

// GET "/keys/{keyId}" returns the specified key data or null if not found/expired.
func handleGetKey(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&totalRequests, 1)
	atomic.AddInt64(&totalKeyGetRequests, 1)
	vars := mux.Vars(r)
	keyId := vars["keyId"]
	now := time.Now().UnixMilli()

	dataMutex.RLock()
	key, exists := data[keyId]
	dataMutex.RUnlock()

	if !exists || key.Expires < now {
		// If expired, remove it.
		if exists {
			dataMutex.Lock()
			delete(data, keyId)
			dataMutex.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		// Return null (encoded as JSON null).
		json.NewEncoder(w).Encode(nil)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(key)
}

// PATCH "/keys/{keyId}" updates a key based on the provided increment and TTL.
func handlePatchKey(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&totalRequests, 1)
	atomic.AddInt64(&totalKeyPatchRequests, 1)
	vars := mux.Vars(r)
	keyId := vars["keyId"]

	var patch KeyPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	now := time.Now().UnixMilli()

	dataMutex.Lock()
	key, exists := data[keyId]
	if !exists || key.Expires < now {
		// Create a new key record if it does not exist or is expired.
		key = &KeyData{
			Value:            0,
			Expires:          now + int64(patch.TTLSeconds)*1000,
			UnpublishedDelta: 0,
		}
		data[keyId] = key
	}
	// Update the key with the increment.
	key.Value += patch.Increment
	key.UnpublishedDelta += patch.Increment
	dataMutex.Unlock()

	// Mark this key for publishing.
	publishListMutex.Lock()
	publishList[keyId] = struct{}{}
	publishListMutex.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(key)
}

// POST "/node-update" applies node update patches from other servers.
func handleNodeUpdate(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&totalRequests, 1)
	atomic.AddInt64(&totalNodeUpdateRequests, 1)
	var patch NodeUpdatePatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Removed verbose logging to reduce overhead.
	// log.Printf("Received node-update: %+v", patch)
	now := time.Now().UnixMilli()

	dataMutex.Lock()
	for keyId, update := range patch {
		if update.Expires < now {
			// Skipping expired update.
			continue
		}

		key, exists := data[keyId]
		if exists && key.Expires < now {
			// Key expired; treat as not found.
			exists = false
			key = nil
		}

		// Adjust expiry if necessary.
		if exists && key.Expires != update.Expires {
			if key.Expires > update.Expires {
				key.Expires = update.Expires
			}
		}

		if !exists {
			// Creating new record.
			key = &KeyData{
				Value:            update.CurrentServerCount,
				Expires:          update.Expires,
				UnpublishedDelta: 0,
			}
			data[keyId] = key
		} else {
			key.Value += update.Delta
			// If this server fell behind, take the upper value.
			if key.Value < update.CurrentServerCount {
				key.Value = update.CurrentServerCount
			}
			// Updating existing record.
		}
	}
	dataMutex.Unlock()

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("OK"))
}

// --------------------- Background Publishing Loop ---------------------

// publishUpdatesLoop runs every second to process and publish updates.
func publishUpdatesLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().UnixMilli()
		updates := make(NodeUpdatePatch)

		// Lock data and publishList to build updates.
		dataMutex.Lock()
		publishListMutex.Lock()
		for keyId := range publishList {
			key, exists := data[keyId]
			if !exists || key.Expires < now {
				continue
			}
			updates[keyId] = NodeUpdateRecord{
				CurrentServerCount: key.Value,
				Delta:              key.UnpublishedDelta,
				Expires:            key.Expires,
			}
			// Reset the unpublished delta after queuing.
			key.UnpublishedDelta = 0
		}
		// Clear the publish list.
		publishList = make(map[string]struct{})
		publishListMutex.Unlock()
		dataMutex.Unlock()

		// Merge these updates into the per-port update queues.
		postUpdateQueuesMutex.Lock()
		for _, p := range activePortList {
			existing := postUpdateQueues[p]
			merged := mergeNodeUpdates(existing, updates)
			postUpdateQueues[p] = merged
		}
		postUpdateQueuesMutex.Unlock()

		// Attempt to send the queued updates for each active port.
		for _, p := range activePortList {
			go func(port int) {
				postUpdateQueuesMutex.Lock()
				update, exists := postUpdateQueues[port]
				if !exists || len(update) == 0 {
					postUpdateQueuesMutex.Unlock()
					return
				}
				postUpdateQueuesMutex.Unlock()

				if err := postUpdates(port, update); err != nil {
					log.Printf("Error Publishing to Server '%d':\n%s", port, err.Error())
					// Do not clear the queue so that updates are retried.
					return
				}
				// On successful post, clear the update queue for that port.
				postUpdateQueuesMutex.Lock()
				postUpdateQueues[port] = make(NodeUpdatePatch)
				postUpdateQueuesMutex.Unlock()
			}(p)
		}
	}
}

// postUpdates sends a node-update patch to a specific port.
func postUpdates(port int, updates NodeUpdatePatch) error {
	// Removed verbose logging; errors are still logged.
	// log.Printf("Publishing node-update to '%d'", port)
	url := "http://localhost:" + strconv.Itoa(port) + "/node-update"

	bodyBytes, err := json.Marshal(updates)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("%d - %s\n%s", res.StatusCode, res.Status, string(b))
	}

	return nil
}

// mergeNodeUpdates merges old and new NodeUpdatePatch maps, summing deltas and taking the max/current as needed.
func mergeNodeUpdates(oldUpdate, newUpdate NodeUpdatePatch) NodeUpdatePatch {
	result := make(NodeUpdatePatch)
	now := time.Now().UnixMilli()

	// Add all keys from newUpdate that are not expired.
	for k, update := range newUpdate {
		if update.Expires > now {
			result[k] = update
		}
	}

	// Merge in non-expired keys from oldUpdate.
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

// Utility functions.
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

// heartbeat logs vital metrics and lock contention counters once per second.
func heartbeat() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Get a snapshot of the number of keys.
		dataMutex.RLock()
		dataCount := len(data)
		dataMutex.RUnlock()

		// Get the size of the publish list.
		publishListMutex.Lock()
		publishCount := len(publishList)
		publishListMutex.Unlock()

		// Summarize the update queues.
		postUpdateQueuesMutex.Lock()
		updateQueueInfo := ""
		for port, patch := range postUpdateQueues {
			updateQueueInfo += fmt.Sprintf(" %d:%d", port, len(patch))
		}
		postUpdateQueuesMutex.Unlock()

		// Read instrumented lock metrics.
		dataAcq := atomic.LoadInt64(&dataMutex.totalAcquisitions)
		dataWait := atomic.LoadInt64(&dataMutex.waitCount)
		plAcq := atomic.LoadInt64(&publishListMutex.totalAcquisitions)
		plWait := atomic.LoadInt64(&publishListMutex.waitCount)
		puqAcq := atomic.LoadInt64(&postUpdateQueuesMutex.totalAcquisitions)
		puqWait := atomic.LoadInt64(&postUpdateQueuesMutex.waitCount)

		totalReq := atomic.LoadInt64(&totalRequests)
		getReq := atomic.LoadInt64(&totalKeyGetRequests)
		patchReq := atomic.LoadInt64(&totalKeyPatchRequests)
		nodeUpdateReq := atomic.LoadInt64(&totalNodeUpdateRequests)

		log.Printf("HEARTBEAT: keys=%d, publishList=%d, updateQueues={%s}, totalReq=%d (get:%d, patch:%d, nodeUpdate:%d) | Locks: data(acq:%d, wait:%d), publish(acq:%d, wait:%d), updateQueues(acq:%d, wait:%d)",
			dataCount, publishCount, updateQueueInfo, totalReq, getReq, patchReq, nodeUpdateReq,
			dataAcq, dataWait, plAcq, plWait, puqAcq, puqWait)
	}
}
