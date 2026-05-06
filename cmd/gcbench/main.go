package main

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

func main() {
	const (
		numSeries        = 1_000_000
		samplesPerSeries = 120
		batchSize        = 50_000
	)

	opts := tsdb.DefaultHeadOptions()
	opts.ChunkRange = 3600000
	h, err := tsdb.NewHead(nil, nil, nil, nil, opts, nil)
	if err != nil {
		panic(err)
	}
	defer h.Close()

	fmt.Printf("=== REALISTIC BENCHMARK (GC enabled, %dM series, %d samples) ===\n", numSeries/1_000_000, samplesPerSeries)

	start := time.Now()
	for sample := 0; sample < samplesPerSeries; sample++ {
		ts := int64(sample) * 15000
		app := h.Appender(context.Background())
		for i := 0; i < numSeries; i++ {
			lset := labels.FromStrings("__name__", "test", "i", fmt.Sprintf("%d", i))
			if _, err := app.Append(0, lset, ts, float64(sample)); err != nil {
				panic(err)
			}
			if (i+1)%batchSize == 0 {
				if err := app.Commit(); err != nil {
					panic(err)
				}
				app = h.Appender(context.Background())
			}
		}
		if err := app.Commit(); err != nil {
			panic(err)
		}
	}
	totalTime := time.Since(start)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Total workload time: %v\n", totalTime)
	fmt.Printf("GC cycles during:    %d\n", m.NumGC)
	fmt.Printf("Total GC pause:      %v\n", time.Duration(m.PauseTotalNs))
	fmt.Printf("Avg GC pause:        %v\n", time.Duration(m.PauseTotalNs/uint64(m.NumGC)))
	fmt.Printf("HeapAlloc:           %d MB\n", m.HeapAlloc/1024/1024)
	fmt.Printf("HeapObjects:         %d\n", m.HeapObjects)

	// Steady-state GC measurement
	debug.SetGCPercent(-1)
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	const iters = 5
	gcStart := time.Now()
	for i := 0; i < iters; i++ {
		runtime.GC()
	}
	gcElapsed := time.Since(gcStart)
	fmt.Printf("\nSteady-state GC:     %v/cycle\n", gcElapsed/time.Duration(iters))
	fmt.Printf("Final HeapAlloc:     %d MB\n", m.HeapAlloc/1024/1024)
	fmt.Printf("Final HeapObjects:   %d\n", m.HeapObjects)
	fmt.Printf("================================================================\n")
}
