package impl

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

func Validation() {
	policy := "LRU"

	var vRAM = 10.0 // GB
	var RAM = 16.0  // GB
	var Disk = 40.0 // GB

	numWorkers := 2

	BandwidthNet := 0.0125 // GB/s
	LatencyNetwork := 0.15 // seconds
	BandwidthVRAM := 128.0 // GB/s
	BandwidthDisk := 128.0 // GB/s

	sys := NewSystem(BandwidthNet, LatencyNetwork, BandwidthVRAM, BandwidthDisk)

	// 1. Setup Workers
	workers := make(map[int]*Worker)
	for i := 1; i <= numWorkers; i++ {
		mem := NewMemory(vRAM, RAM, Disk, policy, sys, i)
		w := &Worker{ID: i, Memory: mem, InputChan: make(chan WorkUnit, 100), OutputChan: make(chan bool), System: sys}
		w.Start()
		workers[i] = w
	}

	coord := NewCoordinator(workers, sys)
	go coord.Start()

	// 2. Setup Models from Specs
	model1 := Model{
		ID: uuid.New(),
		Layers: []Layer{
			{LayerID: uuid.New(), WeightSize: 4.0, ComputeTime: 100 * time.Millisecond},
		},
	}
	model2 := Model{
		ID: uuid.New(),
		Layers: []Layer{
			{LayerID: uuid.New(), WeightSize: 4.0, ComputeTime: 100 * time.Millisecond},
		},
	}

	client := Client{ID: 0, Coordinator: *coord, System: sys}

	client.SendRequest(Request{ClientID: 0, RequestID: 1, ModelID: model1.ID}, model1)

	client.SendRequest(Request{ClientID: 0, RequestID: 2, ModelID: model2.ID}, model2)

	//var wg sync.WaitGroup
	//
	//// 2. Launch Request 1 Concurrently
	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	client.SendRequest(Request{ClientID: 0, RequestID: 1, ModelID: model1.ID}, model1)
	//}()
	//
	//// 3. Launch Request 2 Concurrently
	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	client.SendRequest(Request{ClientID: 0, RequestID: 2, ModelID: model2.ID}, model2)
	//}()
	//
	//// 4. Wait for all concurrent requests to complete
	//wg.Wait()

	folderPath := "res/validation"
	err := os.MkdirAll(folderPath, 0755)
	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return
	}
	baseFileName := fmt.Sprintf("validation_%s.csv", policy)
	fullPath := filepath.Join(folderPath, baseFileName)

	sys.ExportCSV(fullPath)
	fmt.Printf("Completed. Results saved to: %s\n", fullPath)

}
