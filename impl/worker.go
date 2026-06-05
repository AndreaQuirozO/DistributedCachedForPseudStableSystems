package impl

import (
	"fmt"

	"github.com/google/uuid"
)

type Worker struct {
	ID         int
	InputChan  chan WorkUnit
	OutputChan chan bool
	Memory     *Memory
	System     *System
}

type WorkUnit struct {
	RequestID     int
	ModelID       uuid.UUID
	LayerID       uuid.UUID
	Layer         Layer
	IsLast        bool
	SendingLayer  bool
	CoordinatorID int
}

func (w *Worker) Start() {
	workerName := fmt.Sprintf("worker_%d", w.ID)

	go func() {
		for unit := range w.InputChan {
			coordName := fmt.Sprintf("coordinator_%d", unit.CoordinatorID)

			// Memory Request (Now includes Locking internally)
			if err := w.Memory.PrepareLayer(unit.LayerID, unit.Layer,
				unit.RequestID, unit.ModelID); err != nil {
				w.OutputChan <- false
				continue
			}

			// GPU Execution
			w.Memory.mu.Lock()
			storedComputeTime := w.Memory.ComputeTime[unit.LayerID]
			w.Memory.mu.Unlock()

			w.System.RecordActivity(workerName, workerName, OpInference,
				storedComputeTime.Seconds(), "GPU Compute",
				unit.RequestID, unit.ModelID)

			// THE CLEANUP (Abstracted)
			// Memory now decides if it unlocks, moves to RAM, or Wipes completely.
			w.Memory.FinishLayer(unit.LayerID, unit.RequestID, unit.ModelID)

			// Signal Completion
			w.System.RecordActivity(workerName, coordName, OpNetwork, 0.0001,
				"Signal Finished", unit.RequestID, unit.ModelID)
			w.OutputChan <- true
		}
	}()
}
