# Distributed Transaction Processing System   
A high-performance, sharded distributed database system designed for ACID-compliant transactions across multiple clusters. This system utilizes Multi-Paxos for high availability within shards and Two-Phase Commit (2PC) to ensure atomicity for cross-shard operations. It also includes an advanced Hypergraph Partitioning module for dynamic data resharding based on transaction patterns.


## Key Features 
**Sharded Architecture:** Data is distributed across multiple clusters (shards), each maintained by a set of redundant nodes.

**Intra-shard Consensus (Multi-Paxos):** Ensures data consistency and high availability within a cluster, allowing the system to remain functional even if a minority of nodes fail.

**Cross-shard Atomicity (2PC):** Implements the Two-Phase Commit protocol to handle transactions involving items stored in different clusters.

**Dynamic Resharding:** Uses a Hypergraph Partitioner to analyze transaction history and re-route data to minimize cross-shard communication overhead.

**Fault Tolerance:** Includes mechanisms for leader election and node recovery with state synchronization from the cluster leader.

**Persistence:** A SQLite-based persistent storage layer ensures data durability across node restarts.

## Project Structure
**node.py:** Core logic for the distributed node, including Paxos and 2PC state machines. 

**client.py:** Client-side library for submitting transactions and managing retries.

**reshard.py:** Hypergraph partitioning logic for optimizing data distribution.

**persistent.py:** SQLite wrapper for node-local data persistence. 

**config.py:** Global system configurations, cluster mappings, and network addresses. 

**distributed.proto:** gRPC service definitions for inter-node and client-node communication.


## Getting Started
**Installation:** pip install grpcio grpcio-tools
**Initialize Clusters:** python main.py <test_file.csv>
Benchmark Mode:
Run a performance benchmark with various workloads (Smallbank, YCSB, or TPC-C style):

Bash
python main.py --benchmark -n 1000 --read-ratio 0.5 --cross-shard-ratio 0.3
Performance Metrics
The system tracks several key performance indicators:

Throughput: Transactions processed per second.

Latency: Average Latency

Success Rate: Percentage of successfully committed transactions.

Cross-shard vs Intra-shard: Latency breakdown by transaction type.

Protocols Implemented
Multi-Paxos
Each cluster maintains a leader. When a request is received:

Leader assigns a sequence number.

Accept Phase: Leader broadcasts the proposal to replicas.

Commit Phase: Once a majority responds, the transaction is applied locally and committed to the log.

Two-Phase Commit (2PC)
For transactions involving multiple clusters:

Prepare Phase: The coordinator node asks all involved clusters to lock the items and validate balances.

Commit/Abort Phase: If all clusters report "Prepared," the coordinator issues a global commit; otherwise, it issues a global abort.

Dynamic Resharding
The ReshardCoordinator logs transaction affinity. When triggered, it:

Builds a hypergraph where items are vertices and transactions are hyperedges.

Computes an optimized partition using a greedy approach followed by Kernighan-Lin refinement.

Generates a data movement plan to migrate items to their new optimal clusters.

Testing
The system supports interactive testing via CSV files. A test file should specify the test set number, the transaction (Transfer or Balance), and the list of currently "live" nodes to simulate failures.
