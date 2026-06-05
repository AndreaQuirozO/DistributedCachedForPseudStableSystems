package impl

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ExpConfig EXPERIMENT CONFIGURATION METRICS
var ExpConfig = struct {
	NumberOfRuns       int
	Policies           []string // Active cache eviction policies to iterate
	Distributions      []string // Request routing distributions to test
	NumClients         int      // Number of concurrent client routines
	NumWorkers         int      // Number of processing distributed nodes
	RequestsPerClient  int      // Base iterations per client context
	NumModelsAvailable int      // Size of the global Model Zoo pool

	// Architecture Structural baselines (Random variance preserved below)
	BaseLayersPerModel int     // Floor parameter for layer generation count
	BaseWeightSizeGB   float64 // Minimum allocation boundary for static layer weights
	BaseStateSizeGB    float64 // Minimum allocation boundary for dynamic KV Cache states

	// System Performance Metrics (Data Center Physical Constants)
	BandwidthNet   float64 // Network Ingress/Egress cap (GB/s)
	LatencyNetwork float64 // Base network interface round-trip delay penalty (s)
	BandwidthVRAM  float64 // GPU Memory Subsystem Interconnect Bus capacity (GB/s)
	BandwidthDisk  float64 // Local Storage Engine throughput speed (GB/s)

	// Resource Capacity Limitations (Per Worker Constraints)
	CapacityVRAM float64 // Upper boundary limit of dedicated GPU storage (GB)
	CapacityRAM  float64 // Upper boundary limit of local system host RAM (GB)
	CapacityDisk float64 // Upper boundary limit of node disk-backed array (GB)
}{
	NumberOfRuns: 10,
	Policies: []string{"Immediate", "RR", "LRU", "LFU", "FIFO",
		"LIFO", "MRU"},
	Distributions:      []string{"Uniform", "Normal", "PowerLaw", "Poisson", "Gamma"},
	NumClients:         10,
	NumWorkers:         4,
	RequestsPerClient:  20,
	NumModelsAvailable: 40, // 5600+GB, well above 2560 VRAM cap to force eviction dynamics

	BaseLayersPerModel: 32,
	BaseWeightSizeGB:   0.44,
	BaseStateSizeGB:    4.0,

	BandwidthNet:   0.0125, // 100 Mbps standard link
	LatencyNetwork: 0.15,   // 150 ms link latency
	BandwidthVRAM:  128.0,  // PCIe Gen5 x16 link line width
	BandwidthDisk:  128.0,

	CapacityVRAM: 640.0, // DGX Class memory array limits
	CapacityRAM:  2000.0,
	CapacityDisk: 34560.0,
}

func RunRealLifeScenarios() {
	workloadModes := []bool{false, true} // false = Single, true = Multi-model

	for _, isMulti := range workloadModes {
		for _, policy := range ExpConfig.Policies {
			for _, dist := range ExpConfig.Distributions {
				for i := 1; i <= ExpConfig.NumberOfRuns; i++ {
					RunBenchmarkWithID(policy, dist, ExpConfig.NumClients, ExpConfig.RequestsPerClient, i, isMulti)
				}
			}
		}
	}
}

func RunBenchmarkWithID(policy string, distType string, numClients int, requestsPerClient int, iteration int, isMulti bool) {
	sys := NewSystem(ExpConfig.BandwidthNet, ExpConfig.LatencyNetwork, ExpConfig.BandwidthVRAM, ExpConfig.BandwidthDisk)

	workers := make(map[int]*Worker)
	for i := 1; i <= ExpConfig.NumWorkers; i++ {
		mem := NewMemory(ExpConfig.CapacityVRAM, ExpConfig.CapacityRAM, ExpConfig.CapacityDisk, policy, sys, i)
		w := &Worker{
			ID:         i,
			Memory:     mem,
			InputChan:  make(chan WorkUnit, 100),
			OutputChan: make(chan bool),
			System:     sys,
		}
		w.Start()
		workers[i] = w
	}

	coord := NewCoordinator(workers, sys)
	go coord.Start()

	modelZoo := CreateModelZoo(ExpConfig.NumModelsAvailable)

	fmt.Printf("\n>>> BENCHMARK START: Policy=%s, Dist=%s, Clients=%d <<<\n", policy, distType, numClients)

	var wg sync.WaitGroup
	for cID := 1; cID <= numClients; cID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			client := Client{ID: id, Coordinator: *coord, System: sys}

			for r := 0; r < requestsPerClient; r++ {
				idx := GetModelIndex(distType, len(modelZoo), localRand, r, requestsPerClient)
				selectedModel := modelZoo[idx]
				modelsToInvoke := []Model{selectedModel}

				if isMulti {
					numSupports := localRand.Intn(3) + 1
					for s := 0; s < numSupports; s++ {
						sIdx := localRand.Intn(len(modelZoo))
						modelsToInvoke = append(modelsToInvoke, modelZoo[sIdx])
					}
				}

				for _, m := range modelsToInvoke {
					req := Request{
						ClientID:  id,
						RequestID: (id * 1000) + r,
						ModelID:   m.ID,
					}
					go client.SendRequest(req, m)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(cID)
	}

	wg.Wait()

	folderPath := "res/real_life"
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return
	}

	numMod := "Single"
	if isMulti {
		numMod = "Multi"
	}

	baseFileName := fmt.Sprintf("bench_%s_%s_%s_c%d_iter%d.csv", policy, distType, numMod, numClients, iteration)
	fullPath := filepath.Join(folderPath, baseFileName)
	sys.ExportCSV(fullPath)

	fmt.Printf(">>> BENCHMARK COMPLETE: VirtualTime=%.4fs, Log=%s <<<\n", sys.VirtualClock, baseFileName)
}

// CreateModelZoo builds runtime structural targets scaling off Global Configuration boundaries
func CreateModelZoo(size int) []Model {
	zoo := make([]Model, size)
	for i := 0; i < size; i++ {
		// Preserves +30 layer scale randomness over the defined Base configuration floor
		numLayers := ExpConfig.BaseLayersPerModel + rand.Intn(30)
		layers := []Layer{}
		for j := 0; j < numLayers; j++ {
			layers = append(layers, Layer{
				LayerID:     uuid.New(),
				WeightSize:  ExpConfig.BaseWeightSizeGB + rand.Float64()*1.0,
				StateSize:   ExpConfig.BaseStateSizeGB + rand.Float64()*10.0,
				ComputeTime: 50 * time.Millisecond,
			})
		}
		zoo[i] = Model{ID: uuid.New(), Layers: layers}
	}
	return zoo
}

// CreateFixedModelZoo Helper creating exact non-random allocations for targeted constraint verification
func CreateFixedModelZoo(size int, layers int, gbPerLayer float64) []Model {
	zoo := make([]Model, size)
	for i := 0; i < size; i++ {
		l := []Layer{}
		for j := 0; j < layers; j++ {
			l = append(l, Layer{LayerID: uuid.New(), WeightSize: gbPerLayer, ComputeTime: 50 * time.Millisecond})
		}
		zoo[i] = Model{ID: uuid.New(), Layers: l}
	}
	return zoo
}

func GetModelIndex(distType string, zooSize int, r *rand.Rand, currentReq int, totalReqs int) int {
	switch distType {
	case "Uniform":
		return r.Intn(zooSize)
	case "Normal":
		mean := float64(zooSize) / 2.0
		stdDev := float64(zooSize) / 6.0
		val := r.NormFloat64()*stdDev + mean
		idx := int(math.Floor(val))
		return int(math.Max(0, math.Min(float64(zooSize-1), float64(idx))))
	case "PowerLaw":
		z := rand.NewZipf(r, 1.1, 1.0, uint64(zooSize-1))
		return int(z.Uint64())
	case "Poisson":
		lambda := float64(zooSize) / 2.0
		L := math.Exp(-lambda)
		k := 0
		p := 1.0
		for p > L {
			k++
			p *= r.Float64()
		}
		idx := k - 1
		return int(math.Max(0, math.Min(float64(zooSize-1), float64(idx))))
	case "Gamma":
		const alpha = 2.0
		const beta = 2.0
		val := 0.0
		for i := 0; i < int(alpha); i++ {
			val += -math.Log(1.0 - r.Float64())
		}
		val = val * beta
		idx := int(math.Floor(val))
		return int(math.Max(0, math.Min(float64(zooSize-1), float64(idx))))
	default:
		return 0
	}
}
