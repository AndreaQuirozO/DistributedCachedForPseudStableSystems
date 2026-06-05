package impl

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

type ScenarioConfig struct {
	Name           string
	Description    string
	NumWorkers     int
	WorkerVRAM     float64
	NumClients     int
	ModelSpecs     []ModelSpec // Helper to define size/layers
	RequestPattern []int       // Indices of models to request in order
}

type ModelSpec struct {
	Name       string
	NumLayers  int
	LayerSize  float64
	WeightSize float64
}

func ExecuteScenario(sc ScenarioConfig, policy string) {
	BandwidthNet := 0.0125 // GB/s
	LatencyNetwork := 0.15 // seconds
	BandwidthVRAM := 128.0 // GB/s
	BandwidthDisk := 128.0 // GB/s

	sys := NewSystem(BandwidthNet, LatencyNetwork, BandwidthVRAM, BandwidthDisk)
	fmt.Printf("\n>>> SCENARIO: %s | Policy: %s <<<\n", sc.Name, policy)
	fmt.Printf("Description: %s\n", sc.Description)

	// 1. Setup Workers
	workers := make(map[int]*Worker)
	for i := 1; i <= sc.NumWorkers; i++ {
		mem := NewMemory(sc.WorkerVRAM, 128.0, 1000.0, policy, sys, i)
		w := &Worker{ID: i, Memory: mem, InputChan: make(chan WorkUnit, 100), OutputChan: make(chan bool), System: sys}
		w.Start()
		workers[i] = w
	}

	coord := NewCoordinator(workers, sys)
	go coord.Start()

	// 2. Setup Models from Specs
	var modelZoo []Model
	for _, spec := range sc.ModelSpecs {
		layers := []Layer{}
		for j := 0; j < spec.NumLayers; j++ {
			layers = append(layers, Layer{
				LayerID: uuid.New(), WeightSize: spec.LayerSize, ComputeTime: 50 * time.Millisecond,
			})
		}
		modelZoo = append(modelZoo, Model{ID: uuid.New(), Layers: layers})
	}

	// 3. Execute Requests based on Pattern

	// Simple logic for multi-client: divide the pattern among them
	for cID := 1; cID <= sc.NumClients; cID++ {

		client := Client{ID: cID, Coordinator: *coord, System: sys}
		for i, modelIdx := range sc.RequestPattern {
			m := modelZoo[modelIdx]
			reqID := (cID * 100) + i
			client.SendRequest(Request{ClientID: cID, RequestID: reqID,
				ModelID: m.ID}, m)
		}
	}

	// Standardized Naming: res/handcrafted/handcrafted_{ScenarioName}_{Policy
	//}.csv
	folderPath := "res/handcrafted"
	err := os.MkdirAll(folderPath, 0755)
	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return
	}
	baseFileName := fmt.Sprintf("handcrafted_%s_%s.csv", sc.Name, policy)
	fullPath := filepath.Join(folderPath, baseFileName)

	sys.ExportCSV(fullPath)
	fmt.Printf("Completed %s. Results saved to: %s\n", sc.Name, fullPath)
}
