package impl

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type Coordinator struct {
	ID                int
	mu                sync.RWMutex
	Workers           map[int]*Worker
	InputRequestChan  chan Request
	OutputRequestChan chan bool
	LayerRegistry     map[uuid.UUID]map[uuid.UUID]int
	ModelSequence     map[uuid.UUID][]uuid.UUID
	System            *System
	//LocalWorker       *Worker
}

func (c *Coordinator) Start() {
	fmt.Println("Coordinator: System online. Waiting for requests...")

	go func() {
		for req := range c.InputRequestChan {
			fmt.Printf("Coordinator: Receiving Request %d with Model %s from"+
				" Client %d\n",
				req.RequestID, req.ModelID.String()[:8], req.ClientID)

			go c.HandleRequest(req)
		}
	}()
}

func (c *Coordinator) HandleRequest(req Request) {
	hasModel := c.PollWorkersForModel(req.ModelID, req.RequestID)
	coordName := fmt.Sprintf("coordinator_%d", c.ID)

	if hasModel {
		fmt.Printf("Coordinator: Cache hit, "+
			"sending inference of Request %d with Model %s from Client %d\n",
			req.RequestID, req.ModelID.String()[:8], req.ClientID)
		c.System.RecordActivity(coordName, fmt.Sprintf("client_%d", req.ClientID),
			OpNetwork, 0.0001, "Signal: Cache Hit / Starting", req.RequestID, req.ModelID)
		req.ResponseChan <- true
		c.ProcessInference(req.ModelID, req.RequestID)
	} else {
		fmt.Printf("Coordinator: Cache miss, "+
			"requesting for Request %d with Model %s from Client %d\n", req.RequestID,
			req.ModelID.String()[:8], req.ClientID)
		c.System.RecordActivity(coordName,
			fmt.Sprintf("client_%d", req.ClientID),
			OpNetwork, 0.0001, "Signal: Cache Miss / Request Weights", req.RequestID, req.ModelID)
		req.ResponseChan <- false
		modelData := <-req.DataChan
		c.LoadModelToWorkers(modelData, req)
	}

	req.DoneChan <- true
}

func (c *Coordinator) PollWorkersForModel(modelID uuid.UUID, reqID int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	layers, exists := c.LayerRegistry[modelID]

	coordName := fmt.Sprintf("coordinator_%d", c.ID)

	if !exists {
		return false
	}

	for layerID, workerID := range layers {
		nodeName := fmt.Sprintf("worker_%d", workerID)

		c.System.RecordActivity(coordName, nodeName,
			OpNetwork, 0.0001, "Poll: Check Layer Cache", reqID, modelID)

		hasIt := c.Workers[workerID].Memory.HasModel(layerID)

		c.System.RecordActivity(nodeName, coordName,
			OpNetwork, 0.0001, "Poll: Cache Status Reply", reqID, modelID)

		if !hasIt {
			return false
		}
	}
	return true
}

// LoadModelToWorkers Model divided two layers per worker
func (c *Coordinator) LoadModelToWorkers(model Model, req Request) {
	fmt.Printf("Coordinator: Distributing Request %d with Model %s\n",
		req.RequestID, model.ID.String()[:8])

	c.mu.Lock()
	defer c.mu.Unlock()

	workerPlan := c.GetWorkerMapping(model.ID, len(model.Layers))

	if c.LayerRegistry[model.ID] == nil {
		c.LayerRegistry[model.ID] = make(map[uuid.UUID]int)
	}
	if c.ModelSequence[model.ID] == nil {
		c.ModelSequence[model.ID] = make([]uuid.UUID, len(model.Layers))
	}

	for i, layer := range model.Layers {
		workerID := workerPlan[i]

		c.LayerRegistry[model.ID][layer.LayerID] = workerID
		c.ModelSequence[model.ID][i] = layer.LayerID

		coordName := fmt.Sprintf("coordinator_%d", c.ID)
		nodeName := fmt.Sprintf("worker_%d", workerID)
		description := fmt.Sprintf("Distribute Layer ID: %s",
			layer.LayerID.String()[:8])
		c.System.RecordActivity(coordName, nodeName,
			OpNetwork, layer.WeightSize+layer.StateSize, description,
			req.RequestID, model.ID)
		c.Workers[workerID].InputChan <- WorkUnit{
			RequestID:     req.RequestID,
			ModelID:       model.ID,
			LayerID:       layer.LayerID,
			Layer:         layer,
			IsLast:        i == len(model.Layers)-1,
			SendingLayer:  true,
			CoordinatorID: c.ID,
		}

		<-c.Workers[workerID].OutputChan
	}
	fmt.Printf("Coordinator: Request %d with Model %s completed\n",
		req.RequestID, model.ID.String()[:8])
}

// ProcessInference handles a request where the model is already "Warm" (cached)
func (c *Coordinator) ProcessInference(modelID uuid.UUID, requestID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sequence, exists := c.ModelSequence[modelID]

	if !exists {
		fmt.Printf("Coordinator: I don't know the layer sequence for Request"+
			" %d with Model %s\n", requestID, modelID.String()[:8])
		return
	}

	for i, layerID := range sequence {
		workerID := c.LayerRegistry[modelID][layerID]

		worker, exists := c.Workers[workerID]
		if !exists || worker == nil {
			fmt.Printf("Coordinator Error: Worker %d not found in cluster for layer %s\n",
				workerID, layerID.String()[:8])
			return
		}
		coordName := fmt.Sprintf("coordinator_%d", c.ID)
		desc := fmt.Sprintf("Trigger Inference for Layer ID: %s", layerID.String()[:8])
		c.System.RecordActivity(coordName, fmt.Sprintf("worker_%d",
			workerID),
			OpNetwork, 0.0001, desc, requestID, modelID)
		worker.InputChan <- WorkUnit{
			RequestID:     requestID,
			ModelID:       modelID,
			LayerID:       layerID,
			IsLast:        i == len(sequence)-1,
			SendingLayer:  false,
			CoordinatorID: c.ID,
		}

		<-worker.OutputChan
	}
}

func (c *Coordinator) GetWorkerMapping(modelID uuid.UUID, numLayers int) []int {
	numWorkers := len(c.Workers)

	// Use the first byte of the UUID as the offset
	startWorkerOffset := int(modelID[0]) % numWorkers

	mapping := make([]int, numLayers)
	for i := 0; i < numLayers; i++ {
		// Use modulo to stay within [0, numWorkers-1], then add 1
		// to match your 1-indexed worker naming/storage
		mapping[i] = ((startWorkerOffset + i) % numWorkers) + 1
	}

	return mapping
}

func NewCoordinator(workers map[int]*Worker, sys *System) *Coordinator {
	return &Coordinator{
		Workers:          workers,
		InputRequestChan: make(chan Request),
		LayerRegistry:    make(map[uuid.UUID]map[uuid.UUID]int),
		ModelSequence:    make(map[uuid.UUID][]uuid.UUID),
		System:           sys,
	}
}
