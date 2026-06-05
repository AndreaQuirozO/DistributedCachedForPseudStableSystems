package impl

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type StorageTier int

const (
	None StorageTier = -1
	VRAM StorageTier = 0
	RAM  StorageTier = 1
	Disk StorageTier = 2
)

type Memory struct {
	mu                          sync.Mutex
	VRAMCap, RAMCap, DiskCap    float64
	VRAMUsed, RAMUsed, DiskUsed float64

	Storage       map[uuid.UUID]StorageTier
	Locked        map[uuid.UUID]bool
	Metadata      map[uuid.UUID]float64
	ComputeTime   map[uuid.UUID]time.Duration
	ModelLayerMap map[uuid.UUID]uuid.UUID // LayerID -> ModelID

	// Policy Tracking Data
	EvictionPolicy string
	AccessLog      map[uuid.UUID]time.Time // Last time used (LRU)
	Counts         map[uuid.UUID]int       // How many times used (LFU)
	InsertionLog   map[uuid.UUID]time.
			Time // When it was loaded into the system (FIFO/LIFO)

	OwnerID int
	System  *System
}

// HasModel checks if any layer of this model is in VRAM, RAM or disk
func (m *Memory) HasModel(modelID uuid.UUID) bool {
	for key := range m.Storage {
		if key == modelID {
			return true
		}
	}
	return false
}

func (m *Memory) PrepareLayer(layerID uuid.UUID, layer Layer, reqID int, modID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Initialize metadata if it's the first time we see this layer
	storedSize, exists := m.Metadata[layerID]
	if !exists {
		storedSize = layer.WeightSize + layer.StateSize
		m.Metadata[layerID] = storedSize
		m.ComputeTime[layerID] = layer.ComputeTime
		m.Storage[layerID] = -1
		m.ModelLayerMap[layerID] = modID
	}

	currentTier := m.Storage[layerID]

	// 2. Fast Path: Already in VRAM
	if currentTier == VRAM {
		m.Locked[layerID] = true
		m.Touch(layerID)
		return nil
	}

	// 3. Recursive Path Clearance
	// We clear VRAM, which clears RAM, which clears Disk.
	if err := m.EnsureVRAMSpace(storedSize, reqID, modID); err != nil {
		return err
	}

	// 4. Trigger the Move
	// The Transition function now handles all OpNetwork/OpDiskLoad/OpMemTransfer logs
	m.Transition(layerID, currentTier, VRAM, reqID, modID)

	m.Locked[layerID] = true
	m.Touch(layerID)

	return nil
}

// Transition moves a layer between tiers and updates capacity counters.
// This function assumes that the caller has already ensured space in the 'to' tier.
func (m *Memory) Transition(layerID uuid.UUID, from, to StorageTier, reqID int, modID uuid.UUID) {
	size := m.Metadata[layerID]
	evictedModelID := m.ModelLayerMap[layerID]
	node := fmt.Sprintf("worker_%d", m.OwnerID)
	modelInfo := fmt.Sprintf("[Model: %s, Layer: %s]",
		evictedModelID.String()[:8], layerID.String()[:8])

	// Update the Insertion Log only if it's moving to a physical tier (not wiping)
	if to != -1 && to != from {
		m.InsertionLog[layerID] = time.Now()
	}

	// --- TELEMETRY LOGGING ---
	// Handle specific log messages for the simulation export
	if to == VRAM {
		if from == -1 || from == Disk {
			desc := fmt.Sprintf("Disk -> RAM %s", modelInfo)
			m.System.RecordActivity(node, node, OpDiskLoad, size, desc,
				reqID, modID)
			desc = fmt.Sprintf("RAM -> VRAM %s", modelInfo)
			m.System.RecordActivity(node, node, OpMemTransfer, size, desc,
				reqID, modID)
		} else if from == RAM {
			desc := fmt.Sprintf("RAM -> VRAM %s", modelInfo)
			m.System.RecordActivity(node, node, OpMemTransfer, size, desc,
				reqID, modID)
		}
	} else if to == RAM && from == VRAM {
		desc := fmt.Sprintf("Evict VRAM -> RAM %s", modelInfo)
		m.System.RecordActivity(node, node, OpMemTransfer, size, desc, reqID, modID)
	} else if to == Disk && from == RAM {
		desc := fmt.Sprintf("Evict RAM -> Disk %s", modelInfo)
		m.System.RecordActivity(node, node, OpDiskLoad, size, desc, reqID, modID)
	}

	// --- CAPACITY ACCOUNTING (The "From" side) ---
	if from != -1 {
		switch from {
		case VRAM:
			m.VRAMUsed -= size
		case RAM:
			m.RAMUsed -= size
		case Disk:
			m.DiskUsed -= size
		}
	}

	// --- CAPACITY ACCOUNTING (The "To" side) ---
	if to != -1 {
		switch to {
		case VRAM:
			m.VRAMUsed += size
		case RAM:
			m.RAMUsed += size
		case Disk:
			m.DiskUsed += size
		}
		m.Storage[layerID] = to
	} else {
		// Handle the WIPE case (Transition to -1)
		desc := fmt.Sprintf(
			"Wipe/Deallocate Layer from Memory %s", modelInfo)
		m.System.RecordActivity(node, node, OpMemTransfer, 0, desc,
			reqID, modID)

		delete(m.Storage, layerID)
		delete(m.Metadata, layerID)
		delete(m.ModelLayerMap, layerID)
		delete(m.AccessLog, layerID)
		delete(m.Counts, layerID)
		delete(m.InsertionLog, layerID)
		delete(m.Locked, layerID)
		delete(m.ComputeTime, layerID)
	}
}

// Touch updates the metadata needed for policies (call this on every access)
func (m *Memory) Touch(layerID uuid.UUID) {
	// Standard LRU update
	m.AccessLog[layerID] = time.Now()
	// Standard LFU update
	m.Counts[layerID]++
}

// EnsureVRAMSpace evicts from vRAM depending on policy
func (m *Memory) EnsureVRAMSpace(needed float64, reqID int, modID uuid.UUID) error {
	for m.VRAMUsed+needed > m.VRAMCap {
		victim, err := m.selectVictim(VRAM)
		if err != nil {
			return err
		}

		// CRITICAL: Before moving VRAM -> RAM, ensure RAM has space!
		// This may trigger a RAM -> Disk eviction.
		err = m.EnsureRAMSpace(m.Metadata[victim], reqID, modID)
		if err != nil {
			return fmt.Errorf("cascading eviction failed at RAM: %v", err)
		}

		m.Transition(victim, VRAM, RAM, reqID, modID)
	}
	return nil
}

// EnsureRAMSpace evicts from RAM to Disk depending on policy
func (m *Memory) EnsureRAMSpace(needed float64, reqID int, modID uuid.UUID) error {
	for m.RAMUsed+needed > m.RAMCap {
		victim, err := m.selectVictim(RAM)
		if err != nil {
			return err
		}

		// CRITICAL: Before moving RAM -> Disk, ensure Disk has space!
		// This may trigger a Disk -> Wipe eviction.
		err = m.EnsureDiskSpace(m.Metadata[victim], reqID, modID)
		if err != nil {
			return fmt.Errorf("cascading eviction failed at Disk: %v", err)
		}

		m.Transition(victim, RAM, Disk, reqID, modID)
	}
	return nil
}

func (m *Memory) EnsureDiskSpace(needed float64, reqID int, modID uuid.UUID) error {
	for m.DiskUsed+needed > m.DiskCap {
		victim, err := m.selectVictim(Disk)
		if err != nil {
			return err
		}

		// Disk is the bottom of the hierarchy.
		// Moving to -1 deletes the metadata, so no further space check is needed.
		m.Transition(victim, Disk, -1, reqID, modID)
	}
	return nil
}

// Helper to keep the code clean
func (m *Memory) selectVictim(tier StorageTier) (uuid.UUID, error) {
	switch m.EvictionPolicy {
	case "LRU":
		return m.GetVictimLRUByTier(tier)
	case "LFU":
		return m.GetVictimLFUByTier(tier)
	case "FIFO":
		return m.GetVictimFIFOByTier(tier)
	case "LIFO":
		return m.GetVictimLIFOByTier(tier)
	case "MRU":
		return m.GetVictimMRUByTier(tier)
	default:
		return m.GetVictimRRByTier(tier)
	}
}

// FinishLayer releases the lock and handles policy-specific cleanup (like Immediate Wipe)
func (m *Memory) FinishLayer(layerID uuid.UUID, reqID int, modID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Release the lock
	m.Unlock(layerID)

	// 2. Handle the "Immediate" policy
	if m.EvictionPolicy == "Immediate" {
		m.internalWipe(layerID, reqID, modID)
	}
}

// internalWipe is a private helper that assumes m.mu is already held
func (m *Memory) internalWipe(layerID uuid.UUID, reqID int, modID uuid.UUID) {
	tier, exists := m.Storage[layerID]
	if !exists {
		return
	}

	m.Transition(layerID, tier, -1, reqID, modID)

	fmt.Printf("Memory Worker %d: Auto-Wiped Layer %s\n", m.OwnerID, layerID.String()[:8])
}

// Unlock releases the  layer so that the eviction policy
// can move it to a lower storage tier (RAM or Disk) if space is needed.
func (m *Memory) Unlock(layerID uuid.UUID) {
	if _, exists := m.Locked[layerID]; !exists {
		return
	}

	// 2. Set the locked status to false
	m.Locked[layerID] = false
}

func NewMemory(vram, ram, disk float64, evictionPolicy string, sys *System, ownerID int) *Memory {
	return &Memory{
		VRAMCap:        vram,
		RAMCap:         ram,
		DiskCap:        disk,
		Storage:        make(map[uuid.UUID]StorageTier),
		Locked:         make(map[uuid.UUID]bool),
		Metadata:       make(map[uuid.UUID]float64),
		ModelLayerMap:  make(map[uuid.UUID]uuid.UUID),
		ComputeTime:    make(map[uuid.UUID]time.Duration),
		EvictionPolicy: evictionPolicy,
		AccessLog:      make(map[uuid.UUID]time.Time),
		InsertionLog:   make(map[uuid.UUID]time.Time),
		Counts:         make(map[uuid.UUID]int),

		System:  sys,
		OwnerID: ownerID,
	}
}
