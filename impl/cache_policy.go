package impl

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

// GetVictimRRByTier finds any unlocked layer in the specified tier to evict.
func (m *Memory) GetVictimRRByTier(tier StorageTier) (uuid.UUID, error) {
	retries := 5
	for r := 0; r < retries; r++ {
		// 1. Collect all eligible candidates
		var candidates []uuid.UUID
		for id, currentTier := range m.Storage {
			if currentTier == tier && !m.Locked[id] {
				candidates = append(candidates, id)
			}
		}

		// 2. If we found candidates, pick one truly at random
		if len(candidates) > 0 {
			// Use your system's global rand or create a local one
			randomIndex := rand.Intn(len(candidates))
			return candidates[randomIndex], nil
		}

		// 3. If no candidates, wait for an unlock (Inference to finish)
		time.Sleep(2 * time.Millisecond)
	}

	return uuid.Nil, fmt.Errorf("OOM: No unlocked layers in %s after retries", tier)
}

// GetVictimLRUByTier finds the Least Recently Used unlocked layer in given
// tier.
func (m *Memory) GetVictimLRUByTier(tier StorageTier) (uuid.UUID, error) {
	retries := 5
	for r := 0; r < retries; r++ {
		var victim uuid.UUID
		var oldest time.Time
		found := false

		for id, currentTier := range m.Storage {
			if currentTier == tier && !m.Locked[id] {
				if !found || m.AccessLog[id].Before(oldest) {
					oldest = m.AccessLog[id]
					victim = id
					found = true
				}
			}
		}

		if found {
			return victim, nil
		}

		// If we didn't find one, wait a tiny bit for an inference to finish and unlock
		time.Sleep(2 * time.Millisecond)
	}

	// After all retries, if we still haven't found a victim, we truly are OOM
	return uuid.Nil, fmt.Errorf("OOM: No unlockable layers in tier %v after retries", tier)
}

// GetVictimLFUByTier finds the Least Frequently Used unlocked layer in given tier.
func (m *Memory) GetVictimLFUByTier(tier StorageTier) (uuid.UUID, error) {
	retries := 5
	for r := 0; r < retries; r++ {
		var victim uuid.UUID
		minCount := -1
		found := false

		for id, currentTier := range m.Storage {
			if currentTier == tier && !m.Locked[id] {
				count := m.Counts[id]

				// Logic:
				// 1. If this is the first one found, take it.
				// 2. OR if this has a strictly lower frequency than our current victim.
				// 3. OR if frequencies are equal, take the one used less recently (Tie-breaker).
				if !found || count < minCount || (count == minCount && m.AccessLog[id].Before(m.AccessLog[victim])) {
					minCount = count
					victim = id
					found = true
				}
			}
		}

		if found {
			return victim, nil
		}

		// Wait for inference to unlock potential victims
		time.Sleep(2 * time.Millisecond)
	}

	return uuid.Nil, fmt.Errorf("OOM: No unlockable layers in tier %v for LFU", tier)
}

// GetVictimFIFOByTier finds the layer that was loaded into the tier earliest.
func (m *Memory) GetVictimFIFOByTier(tier StorageTier) (uuid.UUID, error) {
	retries := 5
	for r := 0; r < retries; r++ {
		var victim uuid.UUID
		var earliest time.Time
		found := false

		for id, currentTier := range m.Storage {
			if currentTier == tier && !m.Locked[id] {
				// We look for the absolute oldest insertion time
				if !found || m.InsertionLog[id].Before(earliest) {
					earliest = m.InsertionLog[id]
					victim = id
					found = true
				}
			}
		}

		if found {
			return victim, nil
		}

		time.Sleep(2 * time.Millisecond)
	}

	return uuid.Nil, fmt.Errorf("OOM: No unlockable layers in tier %v for FIFO", tier)
}

// GetVictimLIFOByTier finds the layer that was loaded into the tier most recently.
func (m *Memory) GetVictimLIFOByTier(tier StorageTier) (uuid.UUID, error) {
	retries := 5
	for r := 0; r < retries; r++ {
		var victim uuid.UUID
		var latest time.Time
		found := false

		for id, currentTier := range m.Storage {
			if currentTier == tier && !m.Locked[id] {
				// Logic: Find the timestamp that is 'After' (more recent than) all others
				if !found || m.InsertionLog[id].After(latest) {
					latest = m.InsertionLog[id]
					victim = id
					found = true
				}
			}
		}
		if found {
			return victim, nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return uuid.Nil, fmt.Errorf("OOM: No unlockable layers in tier %v for LIFO", tier)
}

// GetVictimMRUByTier finds the layer that was used in the tier most recently.
func (m *Memory) GetVictimMRUByTier(tier StorageTier) (uuid.UUID, error) {
	retries := 5
	for r := 0; r < retries; r++ {
		var victim uuid.UUID
		var mostRecent time.Time
		found := false

		for id, currentTier := range m.Storage {
			if currentTier == tier && !m.Locked[id] {
				// Logic: Find the item with the highest AccessLog timestamp
				if !found || m.AccessLog[id].After(mostRecent) {
					mostRecent = m.AccessLog[id]
					victim = id
					found = true
				}
			}
		}
		if found {
			return victim, nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return uuid.Nil, fmt.Errorf("OOM: No unlockable layers in tier %v for MRU", tier)
}

//
//// GetVictimLFRUByTier implements a hybrid LFU/LRU eviction policy. It first looks for the lowest frequency,
//// and if there are ties, it evicts the least recently used among them.
//func (m *Memory) GetVictimLFRUByTier(tier StorageTier) (uuid.UUID, error) {
//	retries := 5
//	for r := 0; r < retries; r++ {
//		var victim uuid.UUID
//		minFreq := -1
//		var oldestAccess time.Time
//		found := false
//
//		for id, currentTier := range m.Storage {
//			if currentTier == tier && !m.Locked[id] {
//				freq := m.Counts[id]
//				lastUsed := m.AccessLog[id]
//
//				// LFRU Logic:
//				// 1. Evict the one with the lowest frequency (Unprivileged partition)
//				// 2. If frequencies are equal, evict the Least Recently Used (LRU)
//				if !found || freq < minFreq || (freq == minFreq && lastUsed.Before(oldestAccess)) {
//					minFreq = freq
//					oldestAccess = lastUsed
//					victim = id
//					found = true
//				}
//			}
//		}
//		if found {
//			return victim, nil
//		}
//		time.Sleep(2 * time.Millisecond)
//	}
//	return uuid.Nil, fmt.Errorf("OOM: No unlockable layers in tier %v for LFRU", tier)
//}
