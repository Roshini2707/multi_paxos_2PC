import csv
import time
import sys
import os
import shutil
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import config
from node import Node
from client import Client, parse_transaction
from benchmark_generator import BenchmarkGenerator


class PerformanceMetrics:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.transactions = []
        self.successful = 0
        self.failed = 0
        self.balance_queries = 0
        self.transfers = 0
        self.cross_shard = 0
        self.intra_shard = 0
        self.latencies = []
        
    def start(self):
        self.start_time = time.time()
        
    def end(self):
        self.end_time = time.time()
        
    def record_transaction(self, txn_type, success, latency, is_cross_shard=False):
        self.transactions.append({
            'type': txn_type,
            'success': success,
            'latency': latency,
            'cross_shard': is_cross_shard
        })
        
        if success:
            self.successful += 1
        else:
            self.failed += 1
            
        if txn_type == 'BALANCE':
            self.balance_queries += 1
        elif txn_type == 'TRANSFER':
            self.transfers += 1
            if is_cross_shard:
                self.cross_shard += 1
            else:
                self.intra_shard += 1
                
        self.latencies.append(latency)
    
    def get_summary(self):
        if not self.start_time or not self.end_time:
            return "No timing data available"
        
        duration = self.end_time - self.start_time
        total_txns = len(self.transactions)
        
        if total_txns == 0:
            return "No transactions processed"
        
        throughput = total_txns / duration if duration > 0 else 0
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0
        success_rate = (self.successful / total_txns * 100) if total_txns > 0 else 0
        
        sorted_latencies = sorted(self.latencies)
        p50 = sorted_latencies[len(sorted_latencies) // 2] if sorted_latencies else 0
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)] if sorted_latencies else 0
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)] if sorted_latencies else 0
        
        cross_shard_latencies = [t['latency'] for t in self.transactions if t['cross_shard']]
        intra_shard_latencies = [t['latency'] for t in self.transactions if not t['cross_shard'] and t['type'] == 'TRANSFER']
        
        avg_cross_shard = sum(cross_shard_latencies) / len(cross_shard_latencies) if cross_shard_latencies else 0
        avg_intra_shard = sum(intra_shard_latencies) / len(intra_shard_latencies) if intra_shard_latencies else 0
        
        return {
            'duration': duration,
            'total_transactions': total_txns,
            'successful': self.successful,
            'failed': self.failed,
            'throughput': throughput,
            'avg_latency': avg_latency,
            'p50_latency': p50,
            'p95_latency': p95,
            'p99_latency': p99,
            'success_rate': success_rate,
            'balance_queries': self.balance_queries,
            'transfers': self.transfers,
            'cross_shard': self.cross_shard,
            'intra_shard': self.intra_shard,
            'avg_cross_shard_latency': avg_cross_shard,
            'avg_intra_shard_latency': avg_intra_shard
        }
    
    def print_summary(self):
        summary = self.get_summary()
        
        if isinstance(summary, str):
            print(summary)
            return
        
        print("BENCHMARK PERFORMANCE RESULTS")
        
        print(f"  Total Duration:        {summary['duration']:.2f} seconds")
        print(f"  Total Transactions:    {summary['total_transactions']}")
        print(f"  Balance Queries:       {summary['balance_queries']}")
        print(f"  Transfer Transactions: {summary['transfers']}")
        print(f"    - Intra-shard:       {summary['intra_shard']} (avg: {summary['avg_intra_shard_latency']*1000:.2f} ms)")
        print(f"    - Cross-shard:       {summary['cross_shard']} (avg: {summary['avg_cross_shard_latency']*1000:.2f} ms)")
        print(f"  Throughput:            {summary['throughput']:.2f} transactions/sec")
        print(f"  Average Latency:       {summary['avg_latency']*1000:.2f} ms")


class DistributedSystem:
    def __init__(self):
        self.nodes: Dict[int, Node] = {}
        self.client = Client()
        self.start_time = None
        self.end_time = None
        self.transaction_count = 0
        self.metrics = PerformanceMetrics()
        
    def initialize_nodes(self):
        print("Initializing nodes...")
        for node_id in range(1, config.NUM_NODES + 1):
            self.nodes[node_id] = Node(node_id)
            time.sleep(0.1)
        time.sleep(1)
        print(f"All {config.NUM_NODES} nodes initialized\n")
    
    def shutdown_nodes(self):
        print("Shutting down nodes...")
        for node in self.nodes.values():
            node.shutdown()
    
    def flush_system(self, live_nodes=None):
        print("Flushing system state...")
        for node in self.nodes.values():
            node.flush()
        
        from reshard import get_reshard_coordinator
        coordinator = get_reshard_coordinator()
        coordinator.clear_log()
        
        time.sleep(0.5)

        if live_nodes is not None:
            print(f"Setting node states based on live_nodes: {live_nodes}")
            for node_id, node in self.nodes.items():
                if node_id not in live_nodes:
                    node.servicer.is_failed = True
                    print(f"  Node {node_id}: Set to FAILED (not in live_nodes)")
                else:
                    node.servicer.is_failed = False
                    print(f"  Node {node_id}: Set to ACTIVE")
        
        print("System flushed\n")
    
    def print_balance(self, item_id):
        cluster_id = config.get_cluster_for_item(item_id)
        cluster_nodes = config.CLUSTER_MAPPING[cluster_id]
        
        balances = []
        for node_id in cluster_nodes:
            node = self.nodes[node_id]
            balance = node.get_balance(item_id)
            balances.append(f"n{node_id} : {balance}")
        
        print(f"PrintBalance({item_id}); Output: {', '.join(balances)}")
    

    def print_db(self):
        print("\n--- PrintDB: Modified Data Balances ---")
        
        def fetch_node_balances(node_id):
            node = self.nodes[node_id]
            try:
                balances = node.get_all_balances()
                is_failed = node.servicer.is_failed
                return (node_id, balances, None, is_failed)
            except Exception as e:
                return (node_id, None, str(e), False)
        
        results = {}
        with ThreadPoolExecutor(max_workers=config.NUM_NODES) as executor:
            future_to_node = {executor.submit(fetch_node_balances, node_id): node_id 
                            for node_id in self.nodes.keys()}
            
            for future in as_completed(future_to_node):
                node_id, balances, error, is_failed = future.result()
                results[node_id] = (balances, error, is_failed)
        
        for node_id in sorted(results.keys()):
            balances, error, is_failed = results[node_id]
            cluster_id = config.get_cluster_for_node(node_id)
            
            if error:
                print(f"Node n{node_id} (C{cluster_id}): {error}")
            elif balances:
                balance_str = ", ".join([f"{k}: {v}" for k, v in sorted(balances.items())])
                status = " (FAILED)" if is_failed else ""
                print(f"Node n{node_id} (C{cluster_id}): {balance_str}{status}")
            else:
                status = " (FAILED)" if is_failed else ""
                print(f"Node n{node_id} (C{cluster_id}): No modified items.{status}")
        
        print("--- End PrintDB ---\n")

    def print_reshard(self):
        print("\n--- PrintReshard: Triggering Resharding Protocol ---")
        live_nodes = [node_id for node_id, node in self.nodes.items() if not node.servicer.is_failed]
        
        moved_records = self.client.send_reshard(live_nodes)
        
        print("\nResharding Protocol Output (Moved Records):")
        if not moved_records:
            print("No records were moved, or resharding failed/was not implemented.")
        else:
            for item_id, src_cid, dest_cid in moved_records:
                print(f"({item_id}, C{src_cid}, C{dest_cid})")
            
            self._execute_data_movement(moved_records, live_nodes)
            
            import config
            config.apply_resharding(moved_records)
            
            print(f"Resharding complete!!!")
            
            if moved_records:
                test_item = moved_records[0][0]
                test_cluster = config.get_cluster_for_item(test_item)
                print(f"Verification: Item {test_item} now routes to Cluster {test_cluster}")
        
        print("--------------------------------------------------")
    
    def _execute_data_movement(self, moved_records, live_nodes):
        movements = defaultdict(list)
        
        for item_id, src_cluster, dest_cluster in moved_records:
            movements[(src_cluster, dest_cluster)].append(item_id)
        
        for (src_cluster, dest_cluster), item_ids in movements.items():
            print(f"\nMoving {len(item_ids)} items: C{src_cluster} → C{dest_cluster}")
            
            src_nodes = [n for n in config.CLUSTER_MAPPING[src_cluster] if n in live_nodes]
            dest_nodes = [n for n in config.CLUSTER_MAPPING[dest_cluster] if n in live_nodes]
            
            if not src_nodes or not dest_nodes:
                print(f"  WARNING: Cannot move - missing live nodes")
                continue
            
            for item_id in item_ids:
                success = self._transfer_item(item_id, src_nodes, dest_nodes)
                if success:
                    print(f" Moved item {item_id}")
                else:
                    print(f" Failed to move item {item_id}")
        
        print("\n------- Data Movement Complete -----")
    
    def _transfer_item(self, item_id, src_nodes, dest_nodes):
        try:
            src_node = src_nodes[0]
            balance = self.nodes[src_node].get_balance(item_id)
            
            if balance == config.INITIAL_BALANCE:
                return True
            
            for dest_node_id in dest_nodes:
                dest_node = self.nodes[dest_node_id]
                dest_node.set_balance(item_id, balance)
            
            for src_node_id in src_nodes:
                src_node = self.nodes[src_node_id]
                src_node.delete_balance(item_id)
            
            return True
            
        except Exception as e:
            print(f"    Error transferring item {item_id}: {e}")
            return False
    
    def print_view(self):
        print("\n--- PrintView: Leader Status ---")
        for cluster_id in range(1, config.NUM_CLUSTERS + 1):
            cluster_nodes = config.CLUSTER_MAPPING[cluster_id]
            live_cluster_nodes = [n for n in cluster_nodes if not self.nodes[n].servicer.is_failed]
            
            leaders = []
            for node_id in live_cluster_nodes:
                if self.nodes[node_id].servicer.is_leader:
                    leaders.append(node_id)
            
            if leaders:
                if len(leaders) == 1:
                    print(f"  Cluster C{cluster_id}: n{leaders[0]} (Leader)")
                else:
                    print(f"  Cluster C{cluster_id}: SPLIT BRAIN - Multiple leaders: {['n'+str(n) for n in leaders]}")
            else:
                if live_cluster_nodes:
                    print(f"  Cluster C{cluster_id}: NO LEADER (live nodes: {['n'+str(n) for n in live_cluster_nodes]})")
                else:
                    print(f"  Cluster C{cluster_id}: ALL NODES FAILED")
        
        all_view_changes = []
        
        for node_id, node in self.nodes.items():
            if not node.servicer.is_failed:
                view_changes = node.get_view_changes()
                all_view_changes.extend(view_changes)
        
        print("\nLeader Election History:")
        if not all_view_changes:
            print("  No leader elections have occurred")
        else:
            unique_views = {}
            for vc in all_view_changes:
                key = (vc['view'], vc['cluster'])
                if key not in unique_views:
                    unique_views[key] = vc
            
            sorted_views = sorted(unique_views.values(), key=lambda x: x['view'])
            
            print(f"  Total leader elections: {len(sorted_views)}")
            for vc in sorted_views:
                print(f"  View {vc['view']}: Cluster C{vc['cluster']} - "
                      f"Leader changed from n{vc['old_leader']} to n{vc['new_leader']}")
        
        print("--- End PrintView ---\n")
    
    def print_performance(self):
        self.metrics.print_summary()
    
    def process_test_set(self, set_number, transactions, live_nodes):
        print(f"\n{'-'*60}")
        print(f"Processing Test Set {set_number}")
        print(f"Live Nodes: {[f'n{n}' for n in live_nodes]}")
        print(f"Number of Transactions: {len(transactions)}")
        print(f"{'-'*60}\n")
        

        self.metrics = PerformanceMetrics()
        self.metrics.start()
        self.transaction_count = 0
        
        parsed_txns = []
        for txn_str in transactions:
            txn = parse_transaction(txn_str)
            if txn:
                parsed_txns.append(txn)
        
        phases = self._split_into_phases(parsed_txns)
        
        for phase_num, phase in enumerate(phases, 1):
            control_cmds = phase['control']
            data_txns = phase['transactions']
            
            for cmd in control_cmds:
                if cmd['type'] == 'FAIL':
                    node_id = cmd['node_id']
                    print(f"Phase {phase_num}: Failing node n{node_id}...")
                    self.client.send_fail_command(node_id)
                    if node_id in live_nodes:
                        live_nodes.remove(node_id)
                    
                elif cmd['type'] == 'RECOVER':
                    node_id = cmd['node_id']
                    print(f"Phase {phase_num}: Recovering node n{node_id}...")
                    self.client.send_recover_command(node_id)
                    if node_id not in live_nodes:
                        live_nodes.append(node_id)

            if data_txns:
                print(f"Phase {phase_num}: Processing {len(data_txns)} transactions in parallel...")
                completed = self._process_transactions_parallel(data_txns, live_nodes.copy())
                self.transaction_count += completed
                print(f"Phase {phase_num}: Completed {completed} transactions\n")
        
        self.metrics.end()
        
        print(f"\nTest Set {set_number} completed")
        print(f"Processed {self.transaction_count} transactions in parallel\n")
    
    def _split_into_phases(self, transactions):
        phases = []
        current_phase = {
            'control': [],
            'transactions': []
        }
        
        for txn in transactions:
            if txn['type'] in ['FAIL', 'RECOVER']:
                if current_phase['transactions'] or current_phase['control']:
                    phases.append(current_phase)
                    current_phase = {
                        'control': [],
                        'transactions': []
                    }
                current_phase['control'].append(txn)
            else:
                current_phase['transactions'].append(txn)
        
        if current_phase['transactions'] or current_phase['control']:
            phases.append(current_phase)
        
        return phases
    
    def _process_transactions_parallel(self, transactions, live_nodes):
        if not transactions:
            return 0
        
        completed_count = 0
        max_workers = min(20, len(transactions))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_txn = {
                executor.submit(self._execute_single_transaction, txn, live_nodes): txn 
                for txn in transactions
            }
            
            for future in as_completed(future_to_txn):
                txn = future_to_txn[future]
                try:
                    result = future.result()
                    if result:
                        completed_count += 1
                except Exception as e:
                    print(f"Transaction failed: {e}")
        
        return completed_count
    
    def _execute_single_transaction(self, txn, live_nodes):
        txn_start = time.time()
        
        try:
            if txn.get('type') == 'BALANCE':
                txn_type = 'BALANCE'
                is_cross_shard = False
            else:
                txn_type = 'TRANSFER'
                sender_cluster = config.get_cluster_for_item(txn['sender'])
                receiver_cluster = config.get_cluster_for_item(txn['receiver'])
                is_cross_shard = (sender_cluster != receiver_cluster)
            
            result = self.client.send_transaction(txn, live_nodes)
            txn_latency = time.time() - txn_start
            
            success = result is not None and result.get('status') == 'SUCCESS'
            self.metrics.record_transaction(txn_type, success, txn_latency, is_cross_shard)
            
            return success
            
        except Exception as e:
            txn_latency = time.time() - txn_start
            txn_type = 'BALANCE' if txn.get('type') == 'BALANCE' else 'TRANSFER'
            self.metrics.record_transaction(txn_type, False, txn_latency, False)
            return False
    
    def load_test_file(self, filename):
        test_sets = []
        current_set = None
        current_transactions = []
        current_live_nodes = []
        
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            for row in reader:
                if not row or len(row) < 3:
                    continue
                
                set_num_str = row[0].strip()
                transaction_str = row[1].strip()
                live_nodes_str = row[2].strip()
                
                if set_num_str:
                    if current_set is not None:
                        test_sets.append((current_set, current_transactions, current_live_nodes))
                    
                    current_set = int(set_num_str)
                    current_transactions = []
                    
                    live_nodes_str = live_nodes_str.strip('[]')
                    node_strs = [s.strip() for s in live_nodes_str.split(',')]
                    current_live_nodes = []
                    for ns in node_strs:
                        if ns.startswith('n'):
                            current_live_nodes.append(int(ns[1:]))
                        else:
                            current_live_nodes.append(int(ns))
                
                if transaction_str:
                    current_transactions.append(transaction_str)
            
            if current_set is not None:
                test_sets.append((current_set, current_transactions, current_live_nodes))
        
        return test_sets
    
    def benchmark_mode(self, num_transactions, read_ratio, 
                      cross_shard_ratio, skewness):
        print("\n" + "-"*70)
        print("BENCHMARK MODE - WORKLOAD GENERATION")
        print("-"*70)
        print(f"Transactions:       {num_transactions}")
        print(f"Read Ratio:         {read_ratio*100:.0f}%")
        print(f"Cross-Shard Ratio:  {cross_shard_ratio*100:.0f}%")
        print(f"Skewness:           {skewness:.2f}")
        print("="*70 + "\n")
        
        print("Generating workload...")
        generator = BenchmarkGenerator(
            num_items=config.TOTAL_ITEMS,
            num_transactions=num_transactions
        )
        
        transactions = generator.generate_smallbank(
            read_write_ratio=read_ratio,
            cross_shard_ratio=cross_shard_ratio,
            skewness=skewness
        )
        

        if transactions and transactions[-1] == 'F':
            transactions = transactions[:-1]
        
        print(f"Generated {len(transactions)} transactions\n")
        
        live_nodes = list(range(1, config.NUM_NODES + 1))
        
        self.metrics = PerformanceMetrics()
        self.metrics.start()
        
        print(f"\n{'-'*60}")
        print(f"PROCESSING BENCHMARK TRANSACTIONS")
        print(f"{'-'*60}\n")
        
        success_count = 0
        failed_count = 0
        
        for i, txn_str in enumerate(transactions, 1):
            parsed = parse_transaction(txn_str)
            
            if not parsed:
                failed_count += 1
                continue
            
            txn_type = parsed.get('type')
            
            txn_start = time.time()
            
            if txn_type == 'BALANCE':
                transaction = {
                    'item_id': parsed['item_id']
                }
                is_cross_shard = False
            else:
                transaction = {
                    'sender': parsed['sender'],
                    'receiver': parsed['receiver'],
                    'amount': parsed['amount']
                }
                sender_cluster = config.get_cluster_for_item(parsed['sender'])
                receiver_cluster = config.get_cluster_for_item(parsed['receiver'])
                is_cross_shard = (sender_cluster != receiver_cluster)
            
            response = self.client.send_transaction(transaction, live_nodes)
            txn_latency = time.time() - txn_start
            
            if response and response.get('status') == 'SUCCESS':
                success_count += 1
                self.metrics.record_transaction(txn_type, True, txn_latency, is_cross_shard)
            else:
                failed_count += 1
                self.metrics.record_transaction(txn_type, False, txn_latency, is_cross_shard)
        
        self.metrics.end()
        
        print("\n" + "-"*70)
        print("BENCHMARK COMPLETE")
        print("-"*70)
        print(f"Successful: {success_count}")
        print(f"Failed: {failed_count}")
        print("-"*70)
        self.print_performance()
    
    def interactive_mode(self, test_sets):
        for i, (set_num_from_file, transactions, live_nodes) in enumerate(test_sets):
            set_num = i + 1
            
            if i > 0:
                self.flush_system(live_nodes)
                self.client.reset_counter()
            else:
                print("Setting initial node states...")
                for node_id, node in self.nodes.items():
                    if node_id not in live_nodes:
                        node.servicer.is_failed = True
                        print(f"  Node {node_id}: Set to FAILED (not in live_nodes)")
                    else:
                        node.servicer.is_failed = False
                        print(f"  Node {node_id}: Set to ACTIVE")
            
            print(f"\nTest Set {set_num} ready")
            print(f"Transactions: {len(transactions)}")
            print(f"Live Nodes: {live_nodes}")
            
            while True:
                print("\nOptions:")
                print("  1. Process all transactions in this set")
                print("  2. Process next transaction (step-by-step)")
                print("  3. PrintBalance(item_id)")
                print("  4. PrintDB")
                print("  5. PrintView")
                print("  6. PrintPerformance")
                print("  7. PrintReshard (trigger and report resharding)")
                print("  8. Next Test Set (Flush & Continue)") 
                print("  9. Exit System")
                
                choice = input("\nEnter your choice: ").strip()
                
                if choice == '1':
                    self.process_test_set(set_num, transactions, live_nodes.copy())
                    
                elif choice == '2':
                    print("Step-by-step not fully implemented in this snippet, running all...")
                    self.process_test_set(set_num, transactions, live_nodes.copy())
                    
                elif choice == '3':
                    try:
                        val = input("Enter item_id: ")
                        if val:
                            item_id = int(val)
                            self.client.print_balance(item_id, [node for node in self.nodes if not self.nodes[node].servicer.is_failed])
                    except ValueError:
                        print("Invalid item_id")
                        
                elif choice == '4':
                    self.print_db()
                    
                elif choice == '5':
                    self.print_view()
                    
                elif choice == '6':
                    self.print_performance()
                    
                elif choice == '7':
                    self.print_reshard()
                    
                elif choice == '8':
                    print("Moving to next test set...")
                    break
                    
                elif choice == '9':
                    print("Exiting system...")
                    return
                    
                else:
                    print("Invalid choice")
                    
        print("\n" + "-"*60)
        print("All test sets completed!")
        print("-"*60)
    
    def run(self, test_file = None, benchmark = False, 
            num_transactions = 1000, read_ratio = 0.5,
            cross_shard_ratio = 0.3, skewness = 0.0):
        try:
            import stat
            
            def on_rm_error(func, path, exc_info):
                os.chmod(path, stat.S_IWRITE)
                func(path)

            if os.path.exists('data'):
                try:
                    shutil.rmtree('data', onerror=on_rm_error)
                except Exception as e:
                    print("Attempting to continue...")
            
            os.makedirs('data', exist_ok=True)
            self.initialize_nodes()
            
            if benchmark:
                self.benchmark_mode(num_transactions, read_ratio, cross_shard_ratio, skewness)
            else:
                test_sets = self.load_test_file(test_file)
                print(f"Loaded {len(test_sets)} test sets\n")
                
                self.interactive_mode(test_sets)
            
        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.shutdown_nodes()
            self.client.close()
            print("\nSystem shutdown complete")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Distributed Transaction Processing System',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--benchmark', '-b', action='store_true',
                       help='Run in benchmark mode')
    parser.add_argument('--transactions', '-n', type=int, default=1000,
                       help='Number of transactions (benchmark mode, default: 1000)')
    parser.add_argument('--read-ratio', '-r', type=float, default=0.5,
                       help='Read transaction ratio 0-1 (benchmark mode, default: 0.5)')
    parser.add_argument('--cross-shard-ratio', '-c', type=float, default=0.3,
                       help='Cross-shard transaction ratio 0-1 (benchmark mode, default: 0.3)')
    parser.add_argument('--skewness', '-s', type=float, default=0.0,
                       help='Data access skewness 0-1 (benchmark mode, default: 0.0)')
    parser.add_argument('test_file', nargs='?', default=None,
                       help='CSV test file for interactive mode')
    
    args = parser.parse_args()
    
    if not args.benchmark and not args.test_file:
        parser.error("Either provide a test file or use --benchmark mode")
    
    if args.benchmark:
        if not (0 <= args.read_ratio <= 1):
            parser.error("Read ratio must be between 0 and 1")
        if not (0 <= args.cross_shard_ratio <= 1):
            parser.error("Cross-shard ratio must be between 0 and 1")
        if not (0 <= args.skewness <= 1):
            parser.error("Skewness must be between 0 and 1")
        if args.transactions <= 0:
            parser.error("Number of transactions must be positive")
    
    system = DistributedSystem()
    system.run(
        test_file=args.test_file,
        benchmark=args.benchmark,
        num_transactions=args.transactions,
        read_ratio=args.read_ratio,
        cross_shard_ratio=args.cross_shard_ratio,
        skewness=args.skewness
    )


if __name__ == "__main__":
    main()
