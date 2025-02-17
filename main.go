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

// --------------------- Global Variables ---------------------

// KeyRecord wraps KeyData with a per-key mutex.
type KeyRecord struct {
	mu   sync.Mutex
	data *KeyData
}

var (
	// dataStore holds key records in a thread-safe manner.
	dataStore sync.Map // key: string -> *KeyRecord

	// publishSet holds keys pending publication.
	publishSet sync.Map // key: string -> bool

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

	// Build a copy of all keys by iterating dataStore.
	copyData := make(map[string]KeyData)
	dataStore.Range(func(key, value interface{}) bool {
		rec := value.(*KeyRecord)
		// Eventually consistent read: no locking performed.
		if rec.data != nil {
			copyData[key.(string)] = *rec.data
		}
		return true
	})

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

	recVal, exists := dataStore.Load(keyId)
	if !exists {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(nil)
		return
	}

	rec := recVal.(*KeyRecord)
	// Eventually consistent read: read without locking.
	if rec.data == nil || rec.data.Expires < now {
		dataStore.Delete(keyId)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(nil)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec.data)
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

	// Load or create the per-key record.
	recVal, exists := dataStore.Load(keyId)
	var rec *KeyRecord
	if !exists {
		newRec := &KeyRecord{
			data: &KeyData{
				Value:            0,
				Expires:          now + int64(patch.TTLSeconds)*1000,
				UnpublishedDelta: 0,
			},
		}
		actual, _ := dataStore.LoadOrStore(keyId, newRec)
		rec = actual.(*KeyRecord)
	} else {
		rec = recVal.(*KeyRecord)
	}

	// Lock the record while updating.
	rec.mu.Lock()
	if rec.data == nil || rec.data.Expires < now {
		rec.data = &KeyData{
			Value:            0,
			Expires:          now + int64(patch.TTLSeconds)*1000,
			UnpublishedDelta: 0,
		}
	}
	rec.data.Value += patch.Increment
	rec.data.UnpublishedDelta += patch.Increment
	rec.mu.Unlock()

	// Mark this key for publishing.
	publishSet.Store(keyId, true)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rec.data)
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

	now := time.Now().UnixMilli()

	for keyId, update := range patch {
		if update.Expires < now {
			continue
		}

		recVal, exists := dataStore.Load(keyId)
		var rec *KeyRecord
		if !exists {
			newRec := &KeyRecord{
				data: &KeyData{
					Value:            update.CurrentServerCount,
					Expires:          update.Expires,
					UnpublishedDelta: 0,
				},
			}
			actual, _ := dataStore.LoadOrStore(keyId, newRec)
			rec = actual.(*KeyRecord)
		} else {
			rec = recVal.(*KeyRecord)
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

		// Build updates from the publishSet.
		publishSet.Range(func(key, _ interface{}) bool {
			keyId := key.(string)
			recVal, exists := dataStore.Load(keyId)
			if exists {
				rec := recVal.(*KeyRecord)
				rec.mu.Lock()
				if rec.data != nil && rec.data.Expires >= now {
					updates[keyId] = NodeUpdateRecord{
						CurrentServerCount: rec.data.Value,
						Delta:              rec.data.UnpublishedDelta,
						Expires:            rec.data.Expires,
					}
					rec.data.UnpublishedDelta = 0
				}
				rec.mu.Unlock()
			}
			publishSet.Delete(key)
			return true
		})

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
		// Count keys in dataStore.
		dataCount := 0
		dataStore.Range(func(_, _ interface{}) bool {
			dataCount++
			return true
		})

		// Count keys in publishSet.
		publishCount := 0
		publishSet.Range(func(_, _ interface{}) bool {
			publishCount++
			return true
		})

		// Summarize the update queues.
		postUpdateQueuesMutex.Lock()
		updateQueueInfo := ""
		for port, patch := range postUpdateQueues {
			updateQueueInfo += fmt.Sprintf(" %d:%d", port, len(patch))
		}
		postUpdateQueuesMutex.Unlock()

		totalReq := atomic.LoadInt64(&totalRequests)
		getReq := atomic.LoadInt64(&totalKeyGetRequests)
		patchReq := atomic.LoadInt64(&totalKeyPatchRequests)
		nodeUpdateReq := atomic.LoadInt64(&totalNodeUpdateRequests)

		log.Printf("HEARTBEAT: keys=%d, publishSet=%d, updateQueues={%s}, totalReq=%d (get:%d, patch:%d, nodeUpdate:%d)",
			dataCount, publishCount, updateQueueInfo, totalReq, getReq, patchReq, nodeUpdateReq)
	}
}
