package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	// Add request counter
	var requestCount uint64

	// Parse the command-line argument for port. Default to 8000 if not provided.
	port := 8000
	if len(os.Args) > 1 {
		if p, err := strconv.Atoi(os.Args[1]); err == nil {
			port = p
		}
	}

	// Define handler that returns "HW" and counts requests
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&requestCount, 1)
		w.Write([]byte("HW"))
	})

	// Start heartbeat goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			count := atomic.LoadUint64(&requestCount)
			log.Printf("Total requests received: %d", count)
		}
	}()

	// Start server
	addr := ":" + strconv.Itoa(port)
	log.Printf("Listening on port: %d", port)
	log.Fatal(http.ListenAndServe(addr, nil))
}
