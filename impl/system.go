package impl

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/google/uuid"
)

type OpType string

const (
	OpInference   OpType = "GPU_COMPUTE"
	OpMemTransfer OpType = "MEM_MOVE"     // VRAM/RAM/Disk
	OpNetwork     OpType = "NET_TRANSFER" // Client/Coord/Worker
	OpDiskLoad    OpType = "DISK_LOAD"
)

type LogEntry struct {
	VirtualTime  float64 // The "Time" in the simulation when this finished
	OriginNodeID string  // Which Worker or Coordinator
	EndNodeID    string
	Operation    OpType
	SizeGB       float64
	Duration     float64 // How long it took
	Description  string
	RequestID    int
	ModelID      uuid.UUID
}

type System struct {
	VirtualClock float64
	Logs         []LogEntry
	mu           sync.Mutex

	// Hardware Constants
	BandwidthVRAM  float64 // RAM -> VRAM (PCIe)
	BandwidthDisk  float64 // Disk -> RAM (PCIe)
	BandwidthNet   float64 // Network speed  (GB/s)
	LatencyNetwork float64 // Fixed overhead per message (seconds)
}

func NewSystem(BandwidthNet, LatencyNetwork, BandwidthVRAM,
	BandwidthDisk float64) *System {
	sys := &System{
		VirtualClock: 0,
		Logs:         make([]LogEntry, 0),
	}

	sys.BandwidthNet = BandwidthNet
	sys.LatencyNetwork = LatencyNetwork
	sys.BandwidthVRAM = BandwidthVRAM
	sys.BandwidthDisk = BandwidthDisk

	return sys
}

func (s *System) RecordActivity(origin, end string, op OpType, size float64, desc string, reqID int, modID uuid.UUID) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var duration float64

	switch op {
	case OpInference:
		// Size here is the compute time in seconds
		duration = size

	case OpDiskLoad:
		// New: Specifically uses BandwidthDisk (0.5 - 1.5 GB/s)
		duration = size / s.BandwidthDisk

	case OpMemTransfer:
		// Now OpMemTransfer strictly uses BandwidthVRAM (64 - 128 GB/s)
		switch desc {
		case "NetworkToVRAM":
			// Accounting remains 0 because OpNetwork handled the clock jump
			duration = 0
		default:
			// Covers RAMToVRAM, VRAMToRAM, and any PCIe movements
			duration = size / s.BandwidthVRAM
		}

	case OpNetwork:
		duration = s.LatencyNetwork + (size / s.BandwidthNet)
	}

	s.VirtualClock += duration

	s.Logs = append(s.Logs, LogEntry{
		VirtualTime:  s.VirtualClock,
		OriginNodeID: origin,
		EndNodeID:    end,
		Operation:    op,
		SizeGB:       size,
		Duration:     duration,
		Description:  desc,
		RequestID:    reqID,
		ModelID:      modID,
	})

	return duration
}

func (s *System) ExportCSV(filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{
		"VirtualTime_Sec",
		"RequestID",
		"ModelID",
		"OriginNode",
		"EndNode",
		"Operation",
		"Size_GB",
		"Duration_Sec",
		"Description",
	}
	writer.Write(header)

	// 2. Write Data Rows
	for _, log := range s.Logs {
		row := []string{
			strconv.FormatFloat(log.VirtualTime, 'f', 6, 64),
			strconv.Itoa(log.RequestID), // Convert int to string
			log.ModelID.String(),        // UUID to string
			log.OriginNodeID,
			log.EndNodeID,
			string(log.Operation),
			strconv.FormatFloat(log.SizeGB, 'f', 6, 64),
			strconv.FormatFloat(log.Duration, 'f', 6, 64),
			log.Description,
		}
		writer.Write(row)
	}

	fmt.Printf("Successfully exported %d logs to %s for Analysis\n", len(s.Logs), filename)
	return nil
}
