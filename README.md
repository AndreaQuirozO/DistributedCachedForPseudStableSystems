# Distributed Cache Simulator for Pseudo-Stable Inference Systems

[![Go Version](https://img.shields.io/github/go-mod/go-version/AndreaQuirozO/DistributedCachedForPseudStableSystems)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An event-driven simulator written in Go designed to evaluate how classical and hybrid cache eviction policies impact inference latencies across multi-tiered memory topologies. Developed as a Semester Project at the [Distributed Computing Laboratory](https://dcl.epfl.ch/site/), EPFL.

---

## Project Overview

This project studies cache eviction policies in distributed inference systems for large-scale AI models. Modern LLM serving environments rely on hierarchical memory systems (disk, RAM, vRAM) and frequently face severe memory pressure due to large model sizes and multi-model execution pipelines. In this context, cache management decisions play a critical role in determining end-to-end inference latency.

We build an event-driven distributed inference simulator in Go that models clients, a central coordinator, worker nodes, and a hierarchical memory system. The simulator supports interchangeable cache eviction policies and allows controlled evaluation under diverse workload distributions, including Uniform, Normal, Poisson, Gamma, and Power Law request patterns. Both single-model and multi-model serving scenarios are supported to reflect modern AI workloads such as RAG and agentic systems.

We evaluate six classical cache replacement policies (LRU, LFU, FIFO, LIFO, MRU, and Random Replacement) under realistic memory constraints and measure their impact on median and tail latency. Our results show that no single policy dominates across all settings, but LFU and MRU consistently provide the best overall performance depending on workload characteristics. In particular, LFU excels in median latency under skewed multi-model workloads, while MRU often achieves better tail latency under high contention.

The project demonstrates how workload distribution and serving architecture strongly influence cache effectiveness in distributed inference systems, highlighting the need for workload-aware memory management strategies in modern AI infrastructure.


---

## Repository Structure

```text
.
├── analysis/
│   ├── aggregated_single_model_metrics.csv # Post-processed data matrix summaries
│   ├── analysis.ipynb                      # Primary metrics engineering and aggregation
│   └── report.ipynb                        # High-resolution plotting scripts (LaTeX vector exports)
├── impl/
│   ├── cache_policy.go                     # Eviction interfaces and policy implementations
│   ├── client.go                           # Workload distribution generators and request emission
│   ├── coordinator.go                      # Centralized state manager & global cluster clock
│   ├── handcraft_simulations.go            # Manually designed test cases for debugging and controlled experiments
│   ├── memory.go                           # Hierarchical memory system and eviction via pluggable cache policies
│   ├── real_life_simulations.go            # Implements realistic workload simulations based on statistical distributions
│   ├── scenario_engine.go                  # Configuration orchestrator for hand crafted scenario execution
│   ├── system.go                           # Shared system to track logical clock and log events costs
│   ├── validation.go                       # Verification tests for caching invariants
│   └── worker.go                           #  Represents compute nodes executing inference
├── go.mod                                  # Go module dependencies
├── go.sum                                  # Verification checksums
├── main.go                                 # Application entrypoint
└── res/                                    # Target directory for raw simulation log outputs (.csv)
    ├── handcrafted/
    ├── real_life/
    └── validation/

```

---

## Getting Started

### Prerequisites

* **Go Compiler:** Version `1.21` or higher installed locally.
* **Python Environment (for plotting):** Python `3.9+` with dependencies specified below.

### Installation

Clone this repository and verify your local Go module tracking tree:

```bash
git clone git@github.com:AndreaQuirozO/DistributedCachedForPseudStableSystems.git
cd DistributedCachedForPseudStableSystems
go mod download

```

---

## Running Simulations

The simulation suite executes permutations across all valid cache configurations and workload layers automatically via the entrypoint.

To trigger the complete test sweep (e.g., executing the 10-run randomized real-life evaluation arrays):

```bash
go run main.go

```

The execution runtime outputs granular log tracking records directly into the `./res/real_life/` directory structured by experiment configuration names:
`bench_{policy}_{distribution}_{topology}_c{clients}_iter{iteration}.csv`

---

## Data Post-Processing & Visualization

Raw request tracking dumps sum multi-tier layer delays into individual `RequestID` operational latencies. The analysis suite aggregates performance boundaries ($Median$, $P90$, $P99$) over independent seed runs.

### Python Environment Setup

Navigate to the `./analysis/` directory and spin up your virtual tracking dependencies:

```bash
cd analysis
pip install pandas matplotlib seaborn Jupyter

```

### Generating Artifacts

Launch the analysis notebook workspace to recreate publication-quality thesis figures or output automated LaTeX table code:

```bash
jupyter notebook report.ipynb

```

#### Sample Visualization Strategy

The analysis scripts map the multi-order-of-magnitude latencies using a logarithmic scale ($\log_{10}$) to clearly expose structural policy performance discrepancies (e.g., contrasting `LIFO` or `MRU` advantages under skewed looping loops) against hard hardware saturation plateaus.

---

## Performance Metrics Invariant

The systemic virtual latency $L(r)$ tracking profile for any discrete request inference step $r$ adheres to the following compositional structure modeled within `impl/memory.go`:

$$L(r) = L_{\text{network}}(r) + L_{\text{disk}}(r) + L_{\text{memory}}(r) + L_{\text{compute}}(r) + L_{\text{eviction}}(r)$$

---

## Academic Information

* **Author:** Laura Andrea Quiroz Ortega (`laura.quirozortega@epfl.ch`)
* **Institution:** École Polytechnique Fédérale de Lausanne (EPFL)
* **School:** School of Computer and Communication Sciences (IC)
* **Laboratory:** Distributed Computing Laboratory (DCL)
