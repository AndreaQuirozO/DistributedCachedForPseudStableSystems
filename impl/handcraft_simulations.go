package impl

func RunHandcraftedScenarios() {
	policies := []string{"Immediate", "RR", "LRU", "LFU", "FIFO", "LIFO",
		"MRU", "LFRU"}

	//policies := []string{"Immediate"}

	scenarios := []ScenarioConfig{
		{
			Name:        "1-evic-4w2m",
			Description: "Two clients, two models. Eviction expected",
			NumWorkers:  4, WorkerVRAM: 8.0, NumClients: 1, // 32GB total
			ModelSpecs: []ModelSpec{
				{"M1", 4, 5.0, 0.0},
				{"M2", 4, 5.0, 0.0},
			},
			RequestPattern: []int{0, 1, 0},
		},
		{
			Name:        "2-no-evic-4w1m",
			Description: "Baseline: Single client, single model. No eviction expected.",
			NumWorkers:  4, WorkerVRAM: 8.0, NumClients: 1,
			ModelSpecs:     []ModelSpec{{"M1", 4, 2.0, 0.0}},
			RequestPattern: []int{0, 0, 0},
		},

		{
			Name:        "3-lfu-frequency-4w4m",
			Description: "LFU should perform better",
			NumWorkers:  4, WorkerVRAM: 8.0, NumClients: 1, // 32GB total
			ModelSpecs: []ModelSpec{
				{"Small1", 4, 2.0, 0.0},
				{"Small2", 4, 2.0, 0.0},
				{"Small3", 4, 2.0, 0.0},
				{"Big1", 4, 4.0, 0.0},
			},
			RequestPattern: []int{0, 3, 3, 1, 0, 2, 3},
		},
		{
			Name:        "4-lru-locality-4w4m2c",
			Description: "Two clients, two models. Eviction expected",
			NumWorkers:  4, WorkerVRAM: 8.0, NumClients: 2,
			ModelSpecs: []ModelSpec{
				{"Small1", 4, 2.0, 0.0},
				{"Small2", 4, 2.0, 0.0},
				{"Small3", 4, 2.0, 0.0},
				{"Big1", 4, 4.0, 0.0},
			},
			RequestPattern: []int{0, 3, 3, 1, 0, 2, 3},
		},
		{
			Name:        "5-fifo-vs-lru-1w3m1c",
			Description: "Model A is loaded first, then used repeatedly. FIFO will kill it anyway.",
			NumWorkers:  1, WorkerVRAM: 10.0, NumClients: 1,
			ModelSpecs: []ModelSpec{
				{"ModelA", 1, 5.0, 0.0},
				{"ModelB", 1, 5.0, 0.0},
				{"ModelC", 1, 5.0, 0.0},
			},
			// Pattern:
			// 1. Load A (Oldest)
			// 2. Load B
			// 3. Touch A (LRU updates, FIFO stays same)
			// 4. Load C (Forces eviction)
			// 5. Load A (Should be a HIT for LRU, a MISS for FIFO)
			RequestPattern: []int{0, 1, 2, 0, 1},
		},
	}

	for _, policy := range policies {
		for _, sc := range scenarios {
			ExecuteScenario(sc, policy)
		}
	}
}
