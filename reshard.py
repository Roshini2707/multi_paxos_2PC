from typing import List, Tuple, Dict, Set, Any
from collections import defaultdict
import random
import threading


class HypergraphPartitioner:
    def __init__(self, num_clusters = 3):
        self.num_clusters = num_clusters
        self.transaction_history = []
        
    def add_transaction(self, transaction):
        self.transaction_history.append(transaction)
    
    def build_hypergraph(self):
        vertices = set()
        hyperedges = []
        
        for txn in self.transaction_history:
            if len(txn) == 2:
                sender, receiver = txn
                vertices.add(sender)
                vertices.add(receiver)
                hyperedges.append({sender, receiver})
            elif len(txn) == 1:
                item = txn[0]
                vertices.add(item)
                hyperedges.append({item})
        
        return vertices, hyperedges
    
    def compute_affinity_matrix(self, vertices, hyperedges):
        affinity = defaultdict(lambda: defaultdict(int))
        
        for edge in hyperedges:
            edge_list = list(edge)
            for i, item1 in enumerate(edge_list):
                for item2 in edge_list[i+1:]:
                    affinity[item1][item2] += 1
                    affinity[item2][item1] += 1
                affinity[item1][item1] += 1
        
        return affinity
    
    def compute_edge_cut_cost(self, partition, hyperedges):
        cut_cost = 0
        for edge in hyperedges:
            partitions = {partition.get(v, -1) for v in edge}
            partitions.discard(-1)
            
            if len(partitions) > 1:
                cut_cost += (len(partitions) - 1)
        
        return cut_cost
    
    def compute_partition_balance(self, partition):
        sizes = [0, 0, 0]
        for cluster_id in partition.values():
            if 0 <= cluster_id < 3:
                sizes[cluster_id] += 1
        return tuple(sizes)
    
    def is_balanced(self, partition, tolerance = 0.35):
        sizes = self.compute_partition_balance(partition)
        total = sum(sizes)
        if total == 0:
            return True
        
        avg = total / self.num_clusters
        for size in sizes:
            if abs(size - avg) > avg * tolerance:
                return False
        return True
    
    def greedy_initial_partition(self, vertices, affinity):
        partition = {}
        cluster_sizes = [0, 0, 0]
        
        vertex_list = list(vertices)
        vertex_scores = {}
        for v in vertex_list:
            vertex_scores[v] = sum(affinity[v].values())
        
        sorted_vertices = sorted(vertex_list, key=lambda v: vertex_scores[v], reverse=True)
        
        for vertex in sorted_vertices:
            cluster_scores = [0.0, 0.0, 0.0]
            
            for cluster_id in range(3):
                affinity_score = 0
                for other_vertex, assigned_cluster in partition.items():
                    if assigned_cluster == cluster_id:
                        affinity_score += affinity[vertex].get(other_vertex, 0)
                
                avg_size = len(partition) / 3 if len(partition) > 0 else 0
                balance_penalty = abs(cluster_sizes[cluster_id] - avg_size) * 5
                
                cluster_scores[cluster_id] = affinity_score - balance_penalty
            
            best_cluster = max(range(3), key=lambda i: cluster_scores[i])
            partition[vertex] = best_cluster
            cluster_sizes[best_cluster] += 1
        
        return partition
    
    def kernighan_lin_refinement(self, partition, 
                                  hyperedges, 
                                  max_iterations = 100):
        improved = True
        iteration = 0
        best_cost = self.compute_edge_cut_cost(partition, hyperedges)
        
        while improved and iteration < max_iterations:
            improved = False
            iteration += 1
            
            vertices = list(partition.keys())
            random.shuffle(vertices)
            
            for vertex in vertices[:min(100, len(vertices))]:
                current_partition = partition[vertex]
                current_cost = self.compute_edge_cut_cost(partition, hyperedges)
                
                for new_partition in range(self.num_clusters):
                    if new_partition == current_partition:
                        continue
                    
                    partition[vertex] = new_partition
                    
                    if not self.is_balanced(partition, tolerance=0.4):
                        partition[vertex] = current_partition
                        continue
                    
                    new_cost = self.compute_edge_cut_cost(partition, hyperedges)
                    
                    if new_cost < current_cost:
                        current_cost = new_cost
                        best_cost = new_cost
                        improved = True
                        break
                    else:
                        partition[vertex] = current_partition
            
            if iteration % 10 == 0:
                print(f"  Refinement iteration {iteration}, cost: {best_cost}")
        
        return partition
    
    def partition_data(self, initial_partition = None):
        vertices, hyperedges = self.build_hypergraph()
        
        if not vertices:
            return {}
        
        if not hyperedges:
            return initial_partition if initial_partition else {}
        
        affinity = self.compute_affinity_matrix(vertices, hyperedges)
        
        if initial_partition is None:
            partition = {}
            for v in vertices:
                if v <= 3000:
                    partition[v] = 0
                elif v <= 6000:
                    partition[v] = 1
                else:
                    partition[v] = 2
        else:
            partition = {k: v - 1 if v > 0 else 0 for k, v in initial_partition.items()}
            for v in vertices:
                if v not in partition:
                    if v <= 3000:
                        partition[v] = 0
                    elif v <= 6000:
                        partition[v] = 1
                    else:
                        partition[v] = 2
        
        original_partition = partition.copy()
        
        initial_cost = self.compute_edge_cut_cost(partition, hyperedges)
        print(f"  Initial cross-shard cost: {initial_cost}")
        
        greedy_partition = self.greedy_initial_partition(vertices, affinity)
        greedy_cost = self.compute_edge_cut_cost(greedy_partition, hyperedges)
        print(f"  Greedy partition cost: {greedy_cost}")
        
        movement_cost = sum(1 for v in vertices if partition.get(v) != greedy_partition.get(v))
        
        MOVEMENT_PENALTY = 0.5
        AMORTIZATION_FACTOR = max(len(self.transaction_history), 50)
        amortized_movement_cost = (movement_cost * MOVEMENT_PENALTY) / AMORTIZATION_FACTOR
        
        greedy_cost_with_penalty = greedy_cost + amortized_movement_cost
        
        print(f"  Movement cost analysis:")
        print(f"    - Current cost: {initial_cost}")
        
        if greedy_cost_with_penalty < initial_cost:
            benefit = initial_cost - greedy_cost_with_penalty
            partition = greedy_partition
        else:
            print("")
        
        optimized_partition = self.kernighan_lin_refinement(partition, hyperedges, max_iterations=50)
        
        final_cost = self.compute_edge_cut_cost(optimized_partition, hyperedges)
        
        total_moved = sum(1 for v in vertices if original_partition.get(v) != optimized_partition.get(v))
        
        print(f"  Improvement: {initial_cost - final_cost} fewer cross-shard accesses")
        
        return {k: v + 1 for k, v in optimized_partition.items()}
    
    def get_moved_items(self, old_partition, new_partition):
        moved = []
        for item_id in new_partition:
            if item_id in old_partition:
                old_cluster = old_partition[item_id]
                new_cluster = new_partition[item_id]
                
                if old_cluster != new_cluster:
                    moved.append((item_id, old_cluster, new_cluster))
        
        return sorted(moved)
    
    def _get_default_cluster(self, item_id):
        if item_id <= 3000:
            return 1
        elif item_id <= 6000:
            return 2
        else:
            return 3


class ReshardCoordinator:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self.partitioner = HypergraphPartitioner(num_clusters=3)
        self.transaction_log: List[Tuple] = []
        self.log_lock = threading.Lock()
        
    def log_transaction(self, transaction):
        with self.log_lock:
            if transaction.get('type') == 'TRANSFER':
                sender = transaction.get('sender')
                receiver = transaction.get('receiver')
                if sender and receiver:
                    self.transaction_log.append((sender, receiver))
                    self.partitioner.add_transaction((sender, receiver))
            elif transaction.get('type') == 'BALANCE':
                item_id = transaction.get('item_id')
                if item_id:
                    self.transaction_log.append((item_id,))
                    self.partitioner.add_transaction((item_id,))
    
    def compute_resharding(self, max_transactions = None):
        with self.log_lock:
            
            if len(self.transaction_log) == 0:
                print("  No transactions logged yet")
                return {}, []

            partitioner = self.partitioner
            print(f"  Using all {len(self.transaction_log)} transactions for analysis")
        
        vertices, _ = partitioner.build_hypergraph()
        current_partition = {}
        for v in vertices:
            if v <= 3000:
                current_partition[v] = 1
            elif v <= 6000:
                current_partition[v] = 2
            else:
                current_partition[v] = 3
        
        new_partition = partitioner.partition_data(current_partition)
        
        moved_items = partitioner.get_moved_items(current_partition, new_partition)
        
        print(f"\nResharding complete: {len(moved_items)} items to move")
        
        return new_partition, moved_items
    
    def get_statistics(self):
        with self.log_lock:
            return {
                'total_transactions': len(self.transaction_log),
                'transfer_transactions': sum(1 for txn in self.transaction_log if len(txn) == 2),
                'balance_queries': sum(1 for txn in self.transaction_log if len(txn) == 1),
            }
    
    def clear_log(self):
        with self.log_lock:
            self.transaction_log.clear()
            self.partitioner = HypergraphPartitioner(num_clusters=3)
            print("ReshardCoordinator: Transaction log cleared")


_global_coordinator = None

def get_reshard_coordinator() -> ReshardCoordinator:
    global _global_coordinator
    if _global_coordinator is None:
        _global_coordinator = ReshardCoordinator()
    return _global_coordinator
