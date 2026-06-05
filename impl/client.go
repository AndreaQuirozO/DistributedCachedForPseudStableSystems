package impl

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Client struct {
	ID          int
	Coordinator Coordinator
	System      *System
}

type Request struct {
	ClientID     int
	RequestID    int
	ModelID      uuid.UUID
	ResponseChan chan bool
	DataChan     chan Model // Client sends data here if requested
	DoneChan     chan bool
}

type Model struct {
	ID     uuid.UUID
	Layers []Layer
}

// Layer = W + S
type Layer struct {
	LayerID     uuid.UUID
	WeightSize  float64
	StateSize   float64
	ComputeTime time.Duration
}

func (m Model) TotalSize() float64 {
	var total float64
	for _, layer := range m.Layers {
		total += layer.WeightSize + layer.StateSize
	}
	return total
}

func (cl *Client) SendRequest(req Request, model Model) bool {
	fmt.Printf("Client %d: Sending Request %d for Model %s to Coordinator\n",
		cl.ID, req.RequestID, model.ID.String()[:8])
	replyChan := make(chan bool)
	dataChan := make(chan Model)
	req.ResponseChan = replyChan
	req.DataChan = dataChan
	doneChan := make(chan bool)
	req.DoneChan = doneChan

	cl.Coordinator.InputRequestChan <- req
	cl.System.RecordActivity(fmt.Sprintf("client_%d", cl.ID),
		fmt.Sprintf("coordinator_%d", cl.Coordinator.ID),
		OpNetwork, 0.0001,
		"Request Model ID", req.RequestID, req.ModelID)

	modelFound := <-req.ResponseChan

	if !modelFound {
		fmt.Printf("Client %d: For Request %d uploading Model %s to Coordinator\n",
			cl.ID,
			req.RequestID, req.ModelID.String()[:8])
		cl.System.RecordActivity(fmt.Sprintf("client_%d", cl.ID), fmt.Sprintf("coordinator_%d", cl.Coordinator.ID),
			OpNetwork, model.TotalSize(),
			"Upload Model Weights", req.RequestID, req.ModelID)
		req.DataChan <- model // Send the data
	}

	<-doneChan
	return true
}
