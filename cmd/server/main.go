package main

import (
	"encoding/json"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

type Metrics struct {
	Timestamp  time.Time `json:"timestamp"`
	CPUUsage  float64   `json:"cpu_usage"`
	RAMTotal  uint64    `json:"ram_total"`
	RAMUsed   uint64    `json:"ram_used"`
	RAMUsage  float64   `json:"ram_usage"`
	Goroutines int       `json:"goroutines"`
	HeapAlloc uint64    `json:"heap_alloc"`
}

func getMetrics() Metrics {
	virtualMemory, err := mem.VirtualMemory()
	if err != nil {
		log.Println("RAM metrics error:", err)
	}

	cpuUsage, err := cpu.Percent(0, false)
	if err != nil {
		log.Println("CPU metrics error:", err)
	}

	var cpuPercent float64
	if len(cpuUsage) > 0 {
		cpuPercent = cpuUsage[0]
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return Metrics{
		Timestamp:  time.Now(),
		CPUUsage:  cpuPercent,
		RAMTotal:  virtualMemory.Total,
		RAMUsed:   virtualMemory.Used,
		RAMUsage:  virtualMemory.UsedPercent,
		Goroutines: runtime.NumGoroutine(),
		HeapAlloc: memStats.HeapAlloc,
	}
}

// Звичайний HTTP endpoint для отримання метрик
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	metrics := getMetrics()

	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, "Failed to encode metrics", http.StatusInternalServerError)
	}
}

// SSE endpoint для передачі метрик у реальному часі
func metricsStreamHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("SSE client connected")

	for {
		select {
		case <-r.Context().Done():
			log.Println("SSE client disconnected")
			return

		case <-ticker.C:
			metrics := getMetrics()

			data, err := json.Marshal(metrics)
			if err != nil {
				log.Println("JSON encoding error:", err)
				continue
			}

			_, err = w.Write([]byte("data: " + string(data) + "\n\n"))
			if err != nil {
				log.Println("SSE write error:", err)
				return
			}

			flusher.Flush()
		}
	}
}

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Real-Time Operations Gateway is running!"))
	})

	http.HandleFunc("/api/v1/metrics", metricsHandler)

	http.HandleFunc("/api/v1/metrics/stream", metricsStreamHandler)

	log.Println("Server started on http://localhost:8080")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}