import grpc
from concurrent import futures
import threading
import time
from typing import Dict, List, Optional, Set
from collections import defaultdict

import distributed_pb2 as pb2
import distributed_pb2_grpc as pb2_grpc
import config
from reshard import get_reshard_coordinator
from persistent import PersistentDatabase


class NodeServicer(pb2_grpc.NodeServiceServicer):
    
    def __init__(self, node_id):
        self.node_id = node_id
        self.cluster_id = config.get_cluster_for_node(node_id)
        self.is_leader = (node_id == config.get_leader_node(self.cluster_id))
        self.is_failed = False
        
        self.current_view = 0
        self.view_lock = threading.Lock()
        self.election_timeout = config.ELECTION_TIMEOUT
        self.last_heartbeat = time.time() + 10.0
        self.heartbeat_thread = None
        self.election_in_progress = False
        
        self.promised_ballot = 0
        self.election_lock = threading.Lock()
        
        self.db = PersistentDatabase(node_id=node_id, db_dir="./data")
        
        self.locks = {}
        self.lock_table_lock = threading.Lock()
        
        if self.is_leader:
            self.ballot_number = self.node_id
        else:
            self.ballot_number = 0 
        self.accepted_ballot = {}
        self.accepted_value = {}
        self.committed_seq = set()
        self.sequence_number = 0
        self.paxos_lock = threading.Lock()
        
        self.wal = []
        self.wal_lock = threading.Lock()

        self.reshard_records = set() 
        self.reshard_lock = threading.Lock()
        
        self.accept_count = defaultdict(int)
        self.accepted_nodes = defaultdict(set)
        self.consensus_lock = threading.Lock()
        
        self.prepared_count = defaultdict(int)
        self.prepared_nodes = defaultdict(set)
        self.ack_count = defaultdict(int)
        self.twopc_lock = threading.Lock()
        
        self.pending_2pc = {}
        
        self.view_changes = []
        self.view_change_lock = threading.Lock()
        
        self.stubs: Dict[int, pb2_grpc.NodeServiceStub] = {}
        self.stubs_lock = threading.Lock()
        
        self.modified_items = set()
        
        self.propagation_retry_queue = []
        self.retry_queue_lock = threading.Lock()

        shard_items = config.get_items_for_cluster(self.cluster_id)

        self.db.initialize_balances(shard_items, config.INITIAL_BALANCE)

        print(f"Node {self.node_id} initialized with {len(shard_items)} items "
            f"at balance {config.INITIAL_BALANCE}.")
        
        if self.is_leader:
            self.retry_worker_thread = threading.Thread(
                target=self._propagation_retry_worker, 
                daemon=True
            )
            self.retry_worker_thread.start()
        self.monitoring_started = False
        
    def _ensure_monitoring_started(self):
        if not config.ENABLE_LEADER_ELECTION and not getattr(self, '_election_forced', False):
            return
            
        if not self.monitoring_started and not self.is_leader:
            self.monitoring_started = True
            self.last_heartbeat = time.time()
            self._start_heartbeat_monitor()
    
    def _start_heartbeat_monitor(self):
        def monitor():
            time.sleep(3.0)
            
            inactive_time = 0
            while not self.is_failed:
                time.sleep(2.0)
                
                if self.is_leader:
                    break
                
                elapsed = time.time() - self.last_heartbeat
                
                if elapsed > 10.0:
                    inactive_time += 2
                    if inactive_time > 30:
                        print(f"Node {self.node_id}: Pausing heartbeat monitoring due to inactivity")
                        time.sleep(10)
                        inactive_time = 0
                        self.last_heartbeat = time.time()
                        continue
                else:
                    inactive_time = 0
                    
                if elapsed > (self.election_timeout * 2) and elapsed < 10.0:
                    print(f"Node {self.node_id}: Leader heartbeat timeout ({elapsed:.2f}s > {self.election_timeout * 2}s)")
                    self._trigger_leader_election()
                    self.last_heartbeat = time.time()
        
        self.heartbeat_thread = threading.Thread(target=monitor, daemon=True)
        self.heartbeat_thread.start()
    
    def _trigger_leader_election(self):
        with self.election_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
        
        print(f"\n{'='*60}")
        print(f"Node {self.node_id}: PAXOS LEADER ELECTION for Cluster {self.cluster_id}")
        print(f"{'='*60}\n")
        
        with self.view_lock:
            self.current_view += 1
            new_view = self.current_view
        
        new_ballot = (new_view * 100) + self.node_id
        
        cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
        old_leader = config.get_leader_node(self.cluster_id)
        
        prepare_msg = pb2.PrepareElectionMessage(
            sender=self.node_id,
            ballot=new_ballot,
            cluster=self.cluster_id
        )
        
        promises = []
        promise_count = 1
        
        with self.election_lock:
            if new_ballot > self.promised_ballot:
                self.promised_ballot = new_ballot
                uncommitted = self._get_uncommitted_values()
                promises.append({
                    'sender': self.node_id,
                    'ballot': new_ballot,
                    'uncommitted': uncommitted
                })
        
        for node_id in cluster_nodes:
            if node_id == self.node_id:
                continue
            
            try:
                stub = self._get_stub(node_id)
                response = stub.SendPrepareElection(prepare_msg, timeout=1.0)
                
                if response.promised and response.ballot == new_ballot:
                    promise_count += 1
                    promises.append({
                        'sender': response.sender,
                        'ballot': response.ballot,
                        'uncommitted': list(response.uncommitted_values)
                    })
                    print(f"Node {self.node_id}: Got PROMISE from n{response.sender}")
                else:
                    print(f"Node {self.node_id}: n{node_id} rejected (ballot {response.ballot})")
            except Exception as e:
                print(f"Node {self.node_id}: Failed to contact n{node_id}")
        
        if promise_count >= 2:
            print(f"Node {self.node_id}: Got MAJORITY ({promise_count}/3)! I am the new leader!")
            
            self.is_leader = True
            self.ballot_number = new_ballot
            
            with self.view_change_lock:
                self.view_changes.append({
                    'view': new_view,
                    'cluster': self.cluster_id,
                    'old_leader': old_leader,
                    'new_leader': self.node_id,
                    'timestamp': time.time()
                })
            
            self._recover_uncommitted_values(promises)
            
            new_view_msg = pb2.NewViewMessage(
                sender=self.node_id,
                view=new_view,
                cluster=self.cluster_id
            )
            
            for node_id in cluster_nodes:
                if node_id != self.node_id:
                    try:
                        stub = self._get_stub(node_id)
                        stub.SendNewView(new_view_msg, timeout=1.0)
                    except:
                        pass
            
            time.sleep(0.2)
            
            with self.view_lock:
                if self.current_view > new_view:
                    self.is_leader = False
                    self.ballot_number = 0
                    with self.election_lock:
                        self.election_in_progress = False
                    return
            
            if not self.is_leader:
                with self.election_lock:
                    self.election_in_progress = False
                return
            
            print(f"Node {self.node_id}: Election completed (ballot {new_ballot})\n")
        else:
            print(f"Node {self.node_id}: Failed to get majority ({promise_count}/3)")
            self.is_leader = False
        
        self._election_forced = False
        
        with self.election_lock:
            self.election_in_progress = False
    
    
    def SendPrepareElection(self, request, context):
        if self.is_failed:
            return pb2.PromiseMessage(sender=self.node_id, ballot=0, promised=False, uncommitted_values=[])
        
        ballot = request.ballot
        sender = request.sender
        
        print(f"Node {self.node_id}: Received PREPARE(ballot={ballot}) from n{sender}")
        
        with self.election_lock:
            if ballot > self.promised_ballot:
                self.promised_ballot = ballot
                uncommitted = self._get_uncommitted_values()
                
                uncommitted_pb = []
                for uv in uncommitted:
                    uncommitted_pb.append(pb2.UncommittedValue(
                        seq=uv['seq'],
                        ballot=uv['ballot'],
                        value=uv['value']
                    ))
                
                print(f"Node {self.node_id}: PROMISE ballot {ballot}")
                return pb2.PromiseMessage(
                    sender=self.node_id,
                    ballot=ballot,
                    promised=True,
                    uncommitted_values=uncommitted_pb
                )
            else:
                print(f"Node {self.node_id}: REJECT ballot {ballot} (promised {self.promised_ballot})")
                return pb2.PromiseMessage(
                    sender=self.node_id,
                    ballot=self.promised_ballot,
                    promised=False,
                    uncommitted_values=[]
                )
    
    def _get_uncommitted_values(self):
        uncommitted = []
        with self.paxos_lock:
            for seq, ballot in self.accepted_ballot.items():
                # Only include if NOT committed
                if seq not in self.committed_seq and seq in self.accepted_value:
                    uncommitted.append({
                        'seq': seq,
                        'ballot': ballot,
                        'value': self.accepted_value[seq]
                    })
        return uncommitted
    
    def _recover_uncommitted_values(self, promises):
        seq_to_value = {}
        max_seq = self.sequence_number
        
        for promise in promises:
            for uv in promise.get('uncommitted', []):
                seq = uv['seq'] if isinstance(uv, dict) else uv.seq
                ballot = uv['ballot'] if isinstance(uv, dict) else uv.ballot
                value = uv['value'] if isinstance(uv, dict) else uv.value
                
                if seq > max_seq:
                    max_seq = seq
                
                if seq not in seq_to_value or ballot > seq_to_value[seq]['ballot']:
                    seq_to_value[seq] = {'seq': seq, 'ballot': ballot, 'value': value}
        
        with self.paxos_lock:
            if self.accepted_ballot:
                local_max = max(self.accepted_ballot.keys())
                if local_max > max_seq:
                    max_seq = local_max
        
        with self.paxos_lock:
            self.sequence_number = max_seq
            print(f"Node {self.node_id}: Synchronized sequence_number to {self.sequence_number}")
        
        if seq_to_value:
            print(f"Node {self.node_id}: Recovering {len(seq_to_value)} uncommitted values")
            cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
            for seq, uv in seq_to_value.items():
                accept_msg = pb2.AcceptMessage(
                    sender=self.node_id,
                    ballot=self.ballot_number,
                    seq=seq,
                    value=uv['value']
                )
                for node_id in cluster_nodes:
                    if node_id != self.node_id:
                        try:
                            self._get_stub(node_id).SendAccept(accept_msg, timeout=1.0)
                        except:
                            pass
                
                commit_msg = pb2.CommitMessage(sender=self.node_id, seq=seq, value=uv['value'])
                for node_id in cluster_nodes:
                    if node_id != self.node_id:
                        try:
                            self._get_stub(node_id).SendCommit(commit_msg, timeout=1.0)
                        except:
                            pass
    
    def SendNewView(self, request, context):
        if self.is_failed:
            return pb2.Empty()
        
        sender = request.sender
        view = request.view
        cluster = request.cluster
        
        sender_ballot = (view * 100) + sender
        
        print(f"Node {self.node_id}: Received NEW-VIEW (view={view}, ballot={sender_ballot}) from n{sender}")
        
        with self.view_lock:
            if view > self.current_view:
                old_view = self.current_view
                self.current_view = view
                
                if sender != self.node_id:
                    my_ballot = (old_view * 100) + self.node_id if self.is_leader else 0
                    
                    if sender_ballot >= my_ballot:
                        if self.is_leader:
                            print(f"Node {self.node_id}: n{sender} has higher ballot ({sender_ballot} >= {my_ballot})")
                        self.is_leader = False
                        self.ballot_number = 0
                        print(f"Node {self.node_id}: Acknowledging n{sender} as new leader (view {view})")
            
            elif view == self.current_view:
                my_ballot = (view * 100) + self.node_id if self.is_leader else 0
                
                if sender != self.node_id and sender_ballot > my_ballot:
                    if self.is_leader:
                        print(f"Node {self.node_id}: n{sender} has higher ballot in same view ({sender_ballot} > {my_ballot})")
                    self.is_leader = False
                    self.ballot_number = 0 
                    print(f"Node {self.node_id}: Acknowledging n{sender} as leader (same view, higher ballot)")
        
        self.last_heartbeat = time.time()
        return pb2.Empty()
        
    def _get_stub(self, node_id) -> pb2_grpc.NodeServiceStub:
        with self.stubs_lock:
            if node_id not in self.stubs:
                options = [
                    ('grpc.max_receive_message_length', 50 * 1024 * 1024),
                    ('grpc.max_send_message_length', 50 * 1024 * 1024),
                    ('grpc.enable_retries', 0),
                ]
                channel = grpc.insecure_channel(config.get_node_address(node_id), options=options)
                self.stubs[node_id] = pb2_grpc.NodeServiceStub(channel)
            return self.stubs[node_id]
    
    def _get_balance(self, item_id):
        return self.db.get_balance(item_id, config.INITIAL_BALANCE)
    
    def _set_balance(self, item_id, balance):
        self.db.set_balance(item_id, balance)
    
    def _acquire_lock(self, item_id, txn_id):
        with self.lock_table_lock:
            if item_id not in self.locks or self.locks[item_id] is None:
                self.locks[item_id] = txn_id
                return True
            return False
    
    def _acquire_lock_with_retry(self, item_id, txn_id, max_attempts = 20, base_delay = 0.005):
        for attempt in range(max_attempts):
            with self.lock_table_lock:
                if item_id not in self.locks or self.locks[item_id] is None:
                    self.locks[item_id] = txn_id
                    if attempt > 0:
                        print(f"Node {self.node_id}: [LOCK] Acquired lock on item {item_id} for {txn_id} after {attempt} retries")
                    return True
                else:
                    holding_txn = self.locks.get(item_id, "unknown")
                    if attempt == 0:
                        print(f"Node {self.node_id}: [LOCK] Item {item_id} locked by {holding_txn}, {txn_id} will retry")

            if attempt < max_attempts - 1:
                delay = base_delay * (attempt + 1)
                time.sleep(delay)
        
        with self.lock_table_lock:
            holding_txn = self.locks.get(item_id, "unknown")
        print(f"Node {self.node_id}: [LOCK] FAILED to acquire lock on item {item_id} for {txn_id} after {max_attempts} attempts (locked by {holding_txn})")
        return False
    
    def _wait_for_lock_release(self, item_id, timeout = 0.5):
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.lock_table_lock:
                if item_id not in self.locks or self.locks[item_id] is None:
                    return True
            time.sleep(0.002)
        return False
    
    def _release_lock(self, item_id):
        with self.lock_table_lock:
            if item_id in self.locks:
                self.locks[item_id] = None
    
    def _append_wal(self, entry):
        with self.wal_lock:
            self.wal.append(entry)
    
    def SendRequest(self, request, context):
        if self.is_failed:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Node is failed.")

        if not self.is_leader:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Not the leader.")

        self._ensure_monitoring_started()

        self.last_heartbeat = time.time()

        txn = request.transaction
        txn_id = request.transaction_id
        
        if txn.sender == 0:
            txn_type = "BALANCE"
        else:
            txn_type = "TRANSFER"
        
        try:
            coordinator = get_reshard_coordinator()
            if txn_type == "BALANCE":
                coordinator.log_transaction({
                    'type': 'BALANCE',
                    'item_id': txn.item_id
                })
                print(f"Node {self.node_id}: [RESHARD] Logged BALANCE query for item {txn.item_id}")
            else:
                coordinator.log_transaction({
                    'type': 'TRANSFER',
                    'sender': txn.sender,
                    'receiver': txn.receiver,
                    'amount': txn.amount
                })
                print(f"Node {self.node_id}: [RESHARD] Logged TRANSFER {txn.sender}->{txn.receiver}")
        except Exception as e:
            print(f"Node {self.node_id}: Warning - failed to log transaction: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"Node {self.node_id}: Received Request {txn_id} ({txn_type})")

        if txn_type == "BALANCE":
            self._wait_for_lock_release(txn.item_id, timeout=0.5)
            
            bal = self.db.get_balance(txn.item_id, config.INITIAL_BALANCE)
            print(f"Node {self.node_id}: Transaction {txn_id} SUCCESS - Balance query for item {txn.item_id} returned {bal}")
            return pb2.ReplyMessage(transaction_id=txn_id, status="SUCCESS", balance=bal)

        elif txn_type == "TRANSFER":
            sender_cluster = config.get_cluster_for_item(txn.sender)
            receiver_cluster = config.get_cluster_for_item(txn.receiver)
            
            if sender_cluster == receiver_cluster:
                success = self.run_paxos(txn, txn_id)
            else:
                if sender_cluster == self.cluster_id:
                    sender_balance = self._get_balance(txn.sender)
                    if sender_balance < txn.amount:
                        print(f"Node {self.node_id}: Transaction {txn_id} ABORTED - insufficient balance (has {sender_balance}, needs {txn.amount})")
                        return pb2.ReplyMessage(transaction_id=txn_id, status="INSUFFICIENT_FUNDS", balance=-1)
                    
                    print(f"Node {self.node_id}: Coordinator checks passed for {txn_id} - sufficient balance ({sender_balance} >= {txn.amount})")
                
                involved_clusters = [sender_cluster, receiver_cluster]
                success = self.run_2pc(txn, txn_id, involved_clusters)

            if success:
                print(f"Node {self.node_id}: Transaction {txn_id} COMMITTED - Transfer from {txn.sender} to {txn.receiver} amount {txn.amount}")
                return pb2.ReplyMessage(transaction_id=txn_id, status="SUCCESS", balance=-1)
            else:
                print(f"Node {self.node_id}: Transaction {txn_id} FAILED - Transfer from {txn.sender} to {txn.receiver} could not be completed")
                return pb2.ReplyMessage(transaction_id=txn_id, status="FAILURE", balance=-1)

        return pb2.ReplyMessage(transaction_id=txn_id, status="FAILURE", message="Unknown Type")
    
    def run_paxos(self, transaction, txn_id):
        if not self._acquire_lock_with_retry(transaction.sender, txn_id):
            return False
        
        if not self._acquire_lock_with_retry(transaction.receiver, txn_id):
            self._release_lock(transaction.sender)
            print(f"Node {self.node_id}: Transaction {txn_id} SKIPPED - Could not acquire lock on item {transaction.receiver} after retries")
            return False
        
        print(f"Node {self.node_id}: Successfully acquired locks on items {transaction.sender} and {transaction.receiver}")
        
        sender_balance = self._get_balance(transaction.sender)
        if sender_balance < transaction.amount:
            print(f"Node {self.node_id}: Paxos ABORTED - insufficient balance (has {sender_balance}, needs {transaction.amount})")
            self._release_lock(transaction.sender)
            self._release_lock(transaction.receiver)
            return False
        
        print(f"Node {self.node_id}: Paxos checks passed - sufficient balance ({sender_balance} >= {transaction.amount})")

        with self.paxos_lock:
            self.sequence_number += 1
            seq = self.sequence_number
            ballot = self.ballot_number
            
            paxos_value = pb2.PaxosValue(transaction=transaction)
            
            self.accepted_ballot[seq] = ballot
            self.accepted_value[seq] = paxos_value

        accept_msg = pb2.AcceptMessage(
            sender=self.node_id,
            ballot=ballot,
            seq=seq,
            value=paxos_value
        )
        
        accept_count = 1
        cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
        
        print(f"Node {self.node_id}: Sending Accept to cluster {cluster_nodes}, seq={seq}")
        
        for nid in cluster_nodes:
            if nid == self.node_id: continue
            
            try:
                stub = self._get_stub(nid)
                resp = stub.SendAccept(accept_msg, timeout=config.PAXOS_TIMEOUT)
                print(f"Node {self.node_id}: Got Accept response from n{nid}, seq={resp.seq}")
                if resp.seq == seq:
                    accept_count += 1
                    print(f"Node {self.node_id}: Accept count now {accept_count}")
            except Exception as e:
                print(f"Node {self.node_id}: Failed to get Accept from n{nid}: {e}")
                pass

        if accept_count >= 2:
            print(f"Node {self.node_id}: Paxos Consensus Reached (Seq {seq})")
            
            with self.paxos_lock:
                self.committed_seq.add(seq)
            
            commit_msg = pb2.CommitMessage(
                sender=self.node_id,
                seq=seq,
                value=paxos_value
            )
            
            self.apply_transaction(transaction)
                
            for nid in cluster_nodes:
                if nid == self.node_id: continue
                try:
                    stub = self._get_stub(nid)
                    stub.SendCommit(commit_msg, timeout=0.5)
                except: pass

            self._release_lock(transaction.sender)
            self._release_lock(transaction.receiver)
            print(f"Node {self.node_id}: Released locks for {txn_id}")
                
            return True
        else:
            print(f"Node {self.node_id}: Paxos Failed due to no majority")
            self._release_lock(transaction.sender)
            self._release_lock(transaction.receiver)
            print(f"Node {self.node_id}: Released locks (consensus failed) for {txn_id}")
            return False
    
    def _handle_intra_shard(self, request):
        txn = request.transaction
        txn_id = request.transaction_id
        
        if not self._acquire_lock_with_retry(txn.sender, txn_id):
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="LOCKED", balance=-1)
        
        if not self._acquire_lock_with_retry(txn.receiver, txn_id):
            self._release_lock(txn.sender)
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="LOCKED", balance=-1)
        
        sender_balance = self._get_balance(txn.sender)
        if sender_balance < txn.amount:
            self._release_lock(txn.sender)
            self._release_lock(txn.receiver)
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="INSUFFICIENT_FUNDS", balance=-1)
        
        success = self._run_paxos_consensus(txn, txn_id)
        
        self._release_lock(txn.sender)
        self._release_lock(txn.receiver)
        
        if success:
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="SUCCESS", balance=-1)
        else:
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="FAILED", balance=-1)
    
    def _handle_cross_shard(self, request):
        txn = request.transaction
        txn_id = request.transaction_id
        
        sender_cluster = config.get_cluster_for_item(txn.sender)
        receiver_cluster = config.get_cluster_for_item(txn.receiver)
        
        if sender_cluster != self.cluster_id:
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="WRONG_COORDINATOR", balance=-1)
        
        if not self._acquire_lock_with_retry(txn.sender, txn_id):
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="LOCKED", balance=-1)
        
        sender_balance = self._get_balance(txn.sender)
        if sender_balance < txn.amount:
            self._release_lock(txn.sender)
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="INSUFFICIENT_FUNDS", balance=-1)
        
        with self.paxos_lock:
            self.sequence_number += 1
            seq_prepare = self.sequence_number
        
        print(f"Node {self.node_id}: [2PC] Assigned sequence number {seq_prepare} for PREPARE phase of {txn_id}")
        
        paxos_prepare = pb2.PaxosValue(
            transaction_id=txn_id,
            transaction=txn,
            type="2pc-prepare",
            phase="P",
            coordinator=self.node_id
        )
        
        accept_msg_prepare = pb2.AcceptMessage(
            sender=self.node_id,
            ballot=self.ballot_number,
            seq=seq_prepare,
            value=paxos_prepare,
            phase="P"
        )
        
        accept_count = 1
        cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
        
        print(f"Node {self.node_id}: [2PC-PREPARE] Sending Accept to cluster {cluster_nodes}, seq={seq_prepare}, ballot={self.ballot_number}")
        
        for node_id in cluster_nodes:
            if node_id != self.node_id:
                try:
                    stub = self._get_stub(node_id)
                    response = stub.SendAccept(accept_msg_prepare, timeout=config.PAXOS_TIMEOUT)
                    if response.ballot == self.ballot_number and response.seq == seq_prepare:
                        accept_count += 1
                except:
                    pass
        
        if accept_count < 2:
            self._release_lock(txn.sender)
            return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                   status="FAILED", balance=-1)
        
        commit_prepare = pb2.CommitMessage(sender=self.node_id, seq=seq_prepare, value=paxos_prepare)
        for node_id in cluster_nodes:
            if node_id != self.node_id:
                try:
                    stub = self._get_stub(node_id)
                    stub.SendCommit(commit_prepare, timeout=config.PAXOS_TIMEOUT)
                except:
                    pass
        
        prepare_msg = pb2.PrepareMessage(
            sender=self.node_id,
            transaction_id=txn_id,
            transaction=txn
        )
        
        participant_leader = config.get_leader_node(receiver_cluster)
        
        try:
            stub = self._get_stub(participant_leader)
            prepared_response = stub.SendPrepare(prepare_msg, timeout=config.TWO_PC_TIMEOUT)
            
            if prepared_response.status == "PREPARED":
                with self.paxos_lock:
                    self.sequence_number += 1
                    seq_commit = self.sequence_number
                
                print(f"Node {self.node_id}: [2PC] Assigned sequence number {seq_commit} for COMMIT phase of {txn_id}")
                
                paxos_commit = pb2.PaxosValue(
                    transaction_id=txn_id,
                    transaction=txn,
                    type="2pc-commit",
                    phase="C",
                    coordinator=self.node_id
                )
                
                accept_msg_commit = pb2.AcceptMessage(
                    sender=self.node_id,
                    ballot=self.ballot_number,
                    seq=seq_commit,
                    value=paxos_commit,
                    phase="C"
                )
                
                print(f"Node {self.node_id}: [2PC-COMMIT] Sending Accept to cluster {cluster_nodes}, seq={seq_commit}, ballot={self.ballot_number}")
                
                accept_count = 1
                for node_id in cluster_nodes:
                    if node_id != self.node_id:
                        try:
                            stub = self._get_stub(node_id)
                            response = stub.SendAccept(accept_msg_commit, timeout=config.PAXOS_TIMEOUT)
                            if response.ballot == self.ballot_number and response.seq == seq_commit:
                                accept_count += 1
                        except:
                            pass
                
                if accept_count >= 2:
                    commit_msg = pb2.CommitMessage(sender=self.node_id, seq=seq_commit, value=paxos_commit)
                    for node_id in cluster_nodes:
                        if node_id != self.node_id:
                            try:
                                stub = self._get_stub(node_id)
                                stub.SendCommit(commit_msg, timeout=config.PAXOS_TIMEOUT)
                            except:
                                pass
                    
                    with self.paxos_lock:
                        self.committed_seq.add(seq_commit)
                    
                    self.apply_transaction(txn)
                    
                    commit_2pc_msg = pb2.Commit2PCMessage(sender=self.node_id, transaction_id=txn_id)
                    try:
                        stub = self._get_stub(participant_leader)
                        stub.SendCommit2PC(commit_2pc_msg, timeout=config.TWO_PC_TIMEOUT)
                    except:
                        pass
                    
                    self._release_lock(txn.sender)
                    return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                                           status="SUCCESS", balance=-1)
            
            abort_msg = pb2.Abort2PCMessage(sender=self.node_id, transaction_id=txn_id)
            try:
                stub = self._get_stub(participant_leader)
                stub.SendAbort2PC(abort_msg, timeout=config.TWO_PC_TIMEOUT)
            except:
                pass
            
        except:
            pass
        
        self._release_lock(txn.sender)
        return pb2.ReplyMessage(sender=self.node_id, transaction_id=txn_id,
                               status="FAILED", balance=-1)
    

    def SendReshard(self, request, context):
        if self.cluster_id != 1:
            print(f"Node {self.node_id}: Not in Cluster 1")
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f'Node {self.node_id} is in cluster {self.cluster_id}, not cluster 1')
            return pb2.ReshardResponse()
        
        if not self.is_leader:
            print(f"Node {self.node_id}: Not the leader of Cluster 1")
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f'Node {self.node_id} is not the leader')
            return pb2.ReshardResponse()
        
        coordinator = get_reshard_coordinator()
        stats = coordinator.get_statistics()
        print(f"  Transaction history: {stats['total_transactions']} total")
        print(f"    - Transfers: {stats['transfer_transactions']}")
        print(f"    - Balance queries: {stats['balance_queries']}")
        
        if stats['total_transactions'] == 0:
            return pb2.ReshardResponse()
        
        try:
            new_partition, moved_items = coordinator.compute_resharding()
            
            with self.reshard_lock:
                self.reshard_records = set(moved_items)
            
            response = pb2.ReshardResponse()
            for item_id, src_cid, dest_cid in moved_items:
                response.moved_records.append(pb2.ReshardRecord(
                    item_id=item_id, 
                    source_cluster=src_cid, 
                    destination_cluster=dest_cid
                ))
            
            return response
            
        except Exception as e:
            print(f"  Error during resharding: {e}")
            import traceback
            traceback.print_exc()
            return pb2.ReshardResponse()
    def _execute_transaction(self, txn):
        sender_balance = self._get_balance(txn.sender)
        receiver_balance = self._get_balance(txn.receiver)
        
        self._set_balance(txn.sender, sender_balance - txn.amount)
        self._set_balance(txn.receiver, receiver_balance + txn.amount)


    def apply_transaction(self, transaction):
        if transaction.sender == 0:
            return
        
        sender = transaction.sender
        receiver = transaction.receiver
        amount = transaction.amount
        
        if config.get_cluster_for_item(sender) == self.cluster_id:
            current = self.db.get_balance(sender, config.INITIAL_BALANCE)
            if current < amount:
                print(f"Node {self.node_id}: WARNING - Transaction would cause negative balance! Item {sender}: {current} - {amount} = {current - amount}")
            self.db.set_balance(sender, current - amount)
            print(f"Node {self.node_id}: Applied txn - Item {sender}: {current} -> {current - amount}")
            self.modified_items.add(sender)
            
        if config.get_cluster_for_item(receiver) == self.cluster_id:
            current = self.db.get_balance(receiver, config.INITIAL_BALANCE)
            self.db.set_balance(receiver, current + amount)
            print(f"Node {self.node_id}: Applied txn - Item {receiver}: {current} -> {current + amount}")
            self.modified_items.add(receiver)

    def _run_paxos_consensus(self, txn, txn_id):
        with self.paxos_lock:
            self.sequence_number += 1
            seq = self.sequence_number
            ballot = self.ballot_number
        
        cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
        
        print(f"Node {self.node_id}: Starting Paxos PREPARE phase for seq={seq}, ballot={ballot}")
        
        prepare_msg = pb2.PrepareMessage(
            sender=self.node_id,
            ballot=ballot,
            seq=seq
        )
        
        promise_count = 1
        max_accepted_ballot = 0
        max_accepted_value = None
        
        for node_id in cluster_nodes:
            if node_id == self.node_id:
                continue
            
            try:
                stub = self._get_stub(node_id)
                response = stub.SendPaxosPrepare(prepare_msg, timeout=config.PAXOS_TIMEOUT)
                
                if response.promised and response.ballot == ballot:
                    promise_count += 1
                    print(f"Node {self.node_id}: Got PROMISE from n{response.sender}")
                    
                    if response.accepted_ballot > max_accepted_ballot:
                        max_accepted_ballot = response.accepted_ballot
                        max_accepted_value = response.accepted_value
                else:
                    print(f"Node {self.node_id}: n{node_id} rejected prepare or wrong ballot")
            except Exception as e:
                print(f"Node {self.node_id}: Failed to get promise from n{node_id}: {type(e).__name__}")
        
        if promise_count < 2:
            print(f"Node {self.node_id}: Paxos PREPARE phase FAILED - no majority ({promise_count}/3)")
            return False
        
        print(f"Node {self.node_id}: Paxos PREPARE phase SUCCESS - got majority ({promise_count}/3)")
        
        if max_accepted_value:
            paxos_value = max_accepted_value
            print(f"Node {self.node_id}: Using previously accepted value from ballot {max_accepted_ballot}")
        else:
            paxos_value = pb2.PaxosValue(
                transaction_id=txn_id,
                transaction=txn,
                type="intra-shard",
                phase="C"
            )
            print(f"Node {self.node_id}: Proposing new value for txn {txn_id}")
        
        print(f"Node {self.node_id}: Starting Paxos ACCEPT phase for seq={seq}, ballot={ballot}")
        
        accept_msg = pb2.AcceptMessage(
            sender=self.node_id,
            ballot=ballot,
            seq=seq,
            value=paxos_value,
            phase="C"
        )
        
        accept_count = 1
        with self.paxos_lock:
            self.accepted_ballot[seq] = ballot
            self.accepted_value[seq] = paxos_value
        
        for node_id in cluster_nodes:
            if node_id == self.node_id:
                continue
            
            try:
                stub = self._get_stub(node_id)
                response = stub.SendAccept(accept_msg, timeout=config.PAXOS_TIMEOUT)
                
                if response.ballot == ballot and response.seq == seq:
                    accept_count += 1
                    print(f"Node {self.node_id}: Got ACCEPT from n{response.sender}")
                else:
                    print(f"Node {self.node_id}: n{node_id} rejected accept")
            except Exception as e:
                print(f"Node {self.node_id}: Failed to get accept from n{node_id}: {type(e).__name__}")
        
        if accept_count < 2:
            print(f"Node {self.node_id}: Paxos ACCEPT phase FAILED - no majority ({accept_count}/3)")
            return False
        
        print(f"Node {self.node_id}: Paxos ACCEPT phase SUCCESS - got majority ({accept_count}/3)")
        
        print(f"Node {self.node_id}: Starting Paxos COMMIT phase for seq={seq}")
        
        with self.paxos_lock:
            self.committed_seq.add(seq)
        
        self.apply_transaction(paxos_value.transaction)
        
        commit_msg = pb2.CommitMessage(
            sender=self.node_id,
            seq=seq,
            value=paxos_value
        )
        
        for node_id in cluster_nodes:
            if node_id == self.node_id:
                continue
            
            try:
                stub = self._get_stub(node_id)
                stub.SendCommit(commit_msg, timeout=config.PAXOS_TIMEOUT)
                print(f"Node {self.node_id}: Sent COMMIT to n{node_id}")
            except Exception as e:
                print(f"Node {self.node_id}: Failed to send commit to n{node_id}: {type(e).__name__}")
        
        print(f"Node {self.node_id}: Paxos consensus COMPLETED successfully for seq={seq}")
        return True
    

    def SendPaxosPrepare(self, request, context):
        print(f"Node {self.node_id}: Received Paxos PREPARE from n{request.sender}, seq={request.seq}, ballot={request.ballot}")
        
        self._ensure_monitoring_started()
        self.last_heartbeat = time.time()
        
        if self.is_failed:
            return pb2.PrepareResponseMessage(
                sender=self.node_id, 
                promised=False,
                ballot=0, 
                seq=0,
                accepted_ballot=0,
                accepted_value=None
            )
        
        with self.paxos_lock:
            seq = request.seq
            ballot = request.ballot
            
            # Check if we can promise this ballot
            if ballot > self.ballot_number:
                # Update our promised ballot number
                old_ballot = self.ballot_number
                self.ballot_number = ballot
                
                # Get any previously accepted value for this sequence
                accepted_ballot = self.accepted_ballot.get(seq, 0)
                accepted_value = self.accepted_value.get(seq, None)
                
                print(f"Node {self.node_id}: PROMISED ballot={ballot} (was {old_ballot}), seq={seq}")
                if accepted_value:
                    print(f"Node {self.node_id}: Returning previously accepted value at ballot={accepted_ballot}")
                
                return pb2.PrepareResponseMessage(
                    sender=self.node_id,
                    promised=True,
                    ballot=ballot,
                    seq=seq,
                    accepted_ballot=accepted_ballot,
                    accepted_value=accepted_value
                )
            else:
                print(f"Node {self.node_id}: REJECTED Paxos PREPARE - ballot too low ({ballot} <= {self.ballot_number})")
                return pb2.PrepareResponseMessage(
                    sender=self.node_id,
                    promised=False,
                    ballot=self.ballot_number,
                    seq=seq,
                    accepted_ballot=0,
                    accepted_value=None
                )
    
    def SendAccept(self, request, context):
        print(f"Node {self.node_id}: Received Accept from n{request.sender}, seq={request.seq}, ballot={request.ballot}, failed={self.is_failed}")
        
        self._ensure_monitoring_started()
    
        self.last_heartbeat = time.time()
        
        if self.is_failed:
            return pb2.AcceptedMessage(sender=self.node_id, ballot=0, seq=0)

        with self.paxos_lock:
            if request.ballot >= self.ballot_number:
                self.ballot_number = request.ballot
                
                self.accepted_ballot[request.seq] = request.ballot
                self.accepted_value[request.seq] = request.value
                
                print(f"Node {self.node_id}: Accepted seq={request.seq}, ballot={request.ballot}")
                
                return pb2.AcceptedMessage(sender=self.node_id, 
                                         ballot=request.ballot,
                                         seq=request.seq)
            else:
                print(f"Node {self.node_id}: Rejecting Accept - ballot too low ({request.ballot} < {self.ballot_number})")
                return pb2.AcceptedMessage(sender=self.node_id, ballot=0, seq=0)
    
    def SendCommit(self, request, context):
        if self.is_failed:
            return pb2.Empty()
        
        self._ensure_monitoring_started()
        
        self.last_heartbeat = time.time()
        
        seq = request.seq
        sender = request.sender
    
        with self.paxos_lock:
            self.committed_seq.add(seq)
            committed_val = self.accepted_value.get(seq)
            
            if committed_val and committed_val.transaction:
                if sender != self.node_id:
                    self.apply_transaction(committed_val.transaction)
        
        return pb2.Empty()
    
    def SendPrepare(self, request, context):
        if self.is_failed: 
            return pb2.PreparedMessage(sender=self.node_id, transaction_id=request.transaction_id, status="FAILED")
        
        txn_id = request.transaction_id
        transaction = request.transaction
        
        if self.is_leader:
            max_wait_time = 3.0 
            wait_interval = 0.1
            waited = 0.0
            
            while waited < max_wait_time:
                with self.lock_table_lock:
                    sender_locked = False
                    receiver_locked = False
                    
                    if config.get_cluster_for_item(transaction.sender) == self.cluster_id:
                        if transaction.sender in self.locks and self.locks[transaction.sender] is not None:
                            sender_locked = True
                    
                    if config.get_cluster_for_item(transaction.receiver) == self.cluster_id:
                        if transaction.receiver in self.locks and self.locks[transaction.receiver] is not None:
                            receiver_locked = True
                    
                    if not sender_locked and not receiver_locked:
                        if config.get_cluster_for_item(transaction.sender) == self.cluster_id:
                            self.locks[transaction.sender] = txn_id
                            print(f"Node {self.node_id}: Pre-locked sender item {transaction.sender} for {txn_id}")
                        if config.get_cluster_for_item(transaction.receiver) == self.cluster_id:
                            self.locks[transaction.receiver] = txn_id
                            print(f"Node {self.node_id}: Pre-locked receiver item {transaction.receiver} for {txn_id}")
                        break
                
                if sender_locked or receiver_locked:
                    time.sleep(wait_interval)
                    waited += wait_interval
                else:
                    break

            if waited >= max_wait_time:
                print(f"Node {self.node_id}: PREPARE ABORT - timeout waiting for locks on items {transaction.sender}/{transaction.receiver}")
                return pb2.PreparedMessage(sender=self.node_id, transaction_id=txn_id, status="ABORT")
            
            if config.get_cluster_for_item(transaction.sender) == self.cluster_id:
                sender_balance = self._get_balance(transaction.sender)
                if sender_balance < transaction.amount:
                    print(f"Node {self.node_id}: PREPARE ABORT - insufficient balance (has {sender_balance}, needs {transaction.amount})")
                    with self.lock_table_lock:
                        if config.get_cluster_for_item(transaction.sender) == self.cluster_id:
                            self.locks.pop(transaction.sender, None)
                        if config.get_cluster_for_item(transaction.receiver) == self.cluster_id:
                            self.locks.pop(transaction.receiver, None)
                    return pb2.PreparedMessage(sender=self.node_id, transaction_id=txn_id, status="ABORT")
            
            print(f"Node {self.node_id}: PREPARE validation passed for {txn_id}")
        
        with self.wal_lock:
            existing_entry = None
            for entry in reversed(self.wal):
                if entry.get("txn_id") == txn_id:
                    existing_entry = entry
                    break
            
            if existing_entry:
                status = existing_entry.get("status", "PREPARED")
                print(f"Node {self.node_id}: PREPARE idempotent - {txn_id} already in WAL with status {status}")

                if status == "COMMITTED":
                    return pb2.PreparedMessage(sender=self.node_id, transaction_id=txn_id, status="PREPARED")
                return pb2.PreparedMessage(sender=self.node_id, transaction_id=txn_id, status=status.upper())
            
            self.wal.append({
                "txn_id": txn_id,
                "status": "PREPARED",
                "transaction": transaction
            })
            
        if not self.is_leader:
            with self.lock_table_lock:
                if config.get_cluster_for_item(transaction.sender) == self.cluster_id:
                    self.locks[transaction.sender] = txn_id
                    print(f"Node {self.node_id}: Locked sender item {transaction.sender} for {txn_id}")
                if config.get_cluster_for_item(transaction.receiver) == self.cluster_id:
                    self.locks[transaction.receiver] = txn_id
                    print(f"Node {self.node_id}: Locked receiver item {transaction.receiver} for {txn_id}")
        
        if self.is_leader:
            cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
            failed_prepares = []
            for node_id in cluster_nodes:
                if node_id != self.node_id:
                    try:
                        stub = self._get_stub(node_id)
                        stub.SendPrepare(request, timeout=3.0)
                        print(f"Node {self.node_id}: Propagated PREPARE to replica n{node_id}")
                    except Exception as e:
                        print(f"Node {self.node_id}: Failed to propagate PREPARE to n{node_id}: {type(e).__name__}")
                        failed_prepares.append(node_id)
            
            if failed_prepares:
                print(f"Node {self.node_id}: WARNING - PREPARE failed for replicas {failed_prepares}")
                
        return pb2.PreparedMessage(
            sender=self.node_id,
            transaction_id=txn_id,
            status="PREPARED"
        )
    
    def run_2pc(self, transaction, txn_id, clusters):
        print(f"Node {self.node_id}: Coordinator starting 2PC for {txn_id}")
        
        prepare_msg = pb2.PrepareMessage(
            sender=self.node_id,
            transaction_id=txn_id,
            transaction=transaction
        )
        
        prepared_clusters = set()
        for cid in clusters:
            cluster_nodes = config.CLUSTER_MAPPING[cid]
            
            prepared = False
            
            if cid == self.cluster_id:
                try:
                    resp = self.SendPrepare(prepare_msg, None)
                    if resp.status == "PREPARED":
                        prepared = True
                        prepared_clusters.add(cid)
                        print(f"Node {self.node_id}: Cluster {cid} PREPARED (via coordinator self)")
                except Exception as e:
                    print(f"Node {self.node_id}: Coordinator self-PREPARE failed: {e}")
            else:
                prepare_count = 0
                prepared_nodes = []
                
                for node_id in cluster_nodes:
                    try:
                        stub = self._get_stub(node_id)
                        resp = stub.SendPrepare(prepare_msg, timeout=config.TWO_PC_TIMEOUT)
                        if resp.status == "PREPARED":
                            prepare_count += 1
                            prepared_nodes.append(node_id)
                            print(f"Node {self.node_id}: n{node_id} PREPARED for cluster {cid}")
                    except Exception as e:
                        print(f"Node {self.node_id}: n{node_id} PREPARE failed: {type(e).__name__}")
                        continue
                
                if prepare_count >= 2:
                    prepared = True
                    prepared_clusters.add(cid)
                    print(f"Node {self.node_id}: Cluster {cid} PREPARED ({prepare_count}/3 nodes: {prepared_nodes})")
                else:
                    print(f"Node {self.node_id}: Cluster {cid} PREPARE FAILED - only {prepare_count}/3 nodes prepared")
            
            if not prepared:
                print(f"Node {self.node_id}: Cluster {cid} failed to PREPARE (no nodes responded)")
                
        if len(prepared_clusters) == len(clusters):
            print(f"Node {self.node_id}: 2PC GLOBAL COMMIT")
            
            commit_msg = pb2.Commit2PCMessage(sender=self.node_id, transaction_id=txn_id)
            
            for cid in clusters:
                cluster_nodes = config.CLUSTER_MAPPING[cid]
                failed_propagations = []
                
                if cid == self.cluster_id:
                    try:
                        self.SendCommit2PC(commit_msg, None)
                    except Exception as e:
                        print(f"Node {self.node_id}: Coordinator self-COMMIT failed: {e}")
                
                for node_id in cluster_nodes:
                    if node_id == self.node_id:
                        continue
                    try:
                        stub = self._get_stub(node_id)
                        
                        try:
                            prep_resp = stub.SendPrepare(prepare_msg, timeout=2.0)
                            if prep_resp.status == "PREPARED":
                                print(f"Node {self.node_id}: Pre-COMMIT PREPARE sent to n{node_id}")
                        except:
                            pass
                        
                        stub.SendCommit2PC(commit_msg, timeout=3.0)
                        print(f"Node {self.node_id}: Propagated COMMIT to replica n{node_id}")
                    except Exception as e:
                        print(f"Node {self.node_id}: Failed to propagate COMMIT to n{node_id}: {type(e).__name__}")
                        failed_propagations.append(node_id)
                
                if cid == self.cluster_id and failed_propagations:
                    self._add_to_retry_queue(txn_id, failed_propagations)
            
            return True
        else:
            print(f"Node {self.node_id}: 2PC GLOBAL ABORT (only {len(prepared_clusters)}/{len(clusters)} clusters prepared)")
            
            abort_msg = pb2.Abort2PCMessage(sender=self.node_id, transaction_id=txn_id)
            
            for cid in clusters:
                cluster_nodes = config.CLUSTER_MAPPING[cid]
                
                if cid == self.cluster_id:
                    try:
                        self.SendAbort2PC(abort_msg, None)
                    except Exception as e:
                        print(f"Node {self.node_id}: Coordinator self-ABORT failed: {e}")
                
                for node_id in cluster_nodes:
                    if node_id == self.node_id:
                        continue
                    try:
                        stub = self._get_stub(node_id)
                        stub.SendAbort2PC(abort_msg, timeout=1.0)
                        print(f"Node {self.node_id}: Propagated ABORT to replica n{node_id}")
                    except:
                        pass
            
            return False
    def SendCommit2PC(self, request, context):
        if self.is_failed:
            if context:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(f"Node {self.node_id} is failed")
            return pb2.Empty()
        
        txn_id = request.transaction_id
        sender_node = request.sender
        
        with self.wal_lock:
            txn_entry = None
            for entry in reversed(self.wal):
                if entry.get("txn_id") == txn_id:
                    txn_entry = entry
                    break
            
            if not txn_entry:
                return pb2.Empty()
            
            current_status = txn_entry.get("status")
            
            if current_status == "COMMITTED":
                print(f"Node {self.node_id}: Transaction {txn_id} already committed (idempotent)")
                return pb2.Empty()
            
            if current_status != "PREPARED":
                print(f"Node {self.node_id}: ERROR - Cannot commit {txn_id} with status {current_status}")
                if context:
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                return pb2.Empty()
            
            txn = txn_entry.get("transaction")
            
            if not txn:
                print(f"Node {self.node_id}: ERROR - Transaction {txn_id} has no data")
                return pb2.Empty()
            
            try:
                self.apply_transaction(txn)
            except Exception as e:
                print(f"Node {self.node_id}: CRITICAL ERROR applying {txn_id}: {e}")
                if context:
                    context.set_code(grpc.StatusCode.INTERNAL)
                return pb2.Empty()
            
            txn_entry["status"] = "COMMITTED"
            txn_entry["commit_time"] = time.time()
            
            with self.lock_table_lock:
                if config.get_cluster_for_item(txn.sender) == self.cluster_id:
                    self.locks.pop(txn.sender, None)
                if config.get_cluster_for_item(txn.receiver) == self.cluster_id:
                    self.locks.pop(txn.receiver, None)
            
            print(f"Node {self.node_id}: Successfully committed {txn_id}")
        
        if self.is_leader and sender_node != self.node_id:
            cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
            failed_propagations = []
            
            for node_id in cluster_nodes:
                if node_id == self.node_id:
                    continue
                
                try:
                    stub = self._get_stub(node_id)
                    propagation_request = pb2.Commit2PCMessage(
                        sender=self.node_id,
                        transaction_id=txn_id
                    )
                    stub.SendCommit2PC(propagation_request, timeout=3.0)
                    print(f"Node {self.node_id}: Propagated COMMIT to n{node_id}")
                    
                except Exception as e:
                    print(f"Node {self.node_id}: Failed to propagate to n{node_id}: {type(e).__name__}")
                    failed_propagations.append(node_id)
            
            if failed_propagations:
                self._add_to_retry_queue(txn_id, failed_propagations)
        
        return pb2.Empty()
    
    def SendAbort2PC(self, request, context):
        if self.is_failed:
            return pb2.Empty()
        
        txn_id = request.transaction_id
        sender_node = request.sender
        
        with self.wal_lock:
            txn_entry = None
            for entry in reversed(self.wal):
                if entry.get("txn_id") == txn_id:
                    txn_entry = entry
                    break
            
            if not txn_entry:
                return pb2.Empty()
            
            current_status = txn_entry.get("status")
            
            if current_status == "ABORTED":
                print(f"Node {self.node_id}: Transaction {txn_id} already aborted")
                return pb2.Empty()
            
            if current_status != "PREPARED":
                return pb2.Empty()
            
            txn = txn_entry.get("transaction")
            
            if txn:
                with self.lock_table_lock:
                    if config.get_cluster_for_item(txn.sender) == self.cluster_id:
                        self.locks.pop(txn.sender, None)
                    if config.get_cluster_for_item(txn.receiver) == self.cluster_id:
                        self.locks.pop(txn.receiver, None)
            
            txn_entry["status"] = "ABORTED"
            txn_entry["abort_time"] = time.time()
        
        if self.is_leader and sender_node != self.node_id:
            cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
            for node_id in cluster_nodes:
                if node_id != self.node_id:
                    try:
                        stub = self._get_stub(node_id)
                        propagation_request = pb2.Abort2PCMessage(
                            sender=self.node_id,
                            transaction_id=txn_id
                        )
                        stub.SendAbort2PC(propagation_request, timeout=3.0)
                        print(f"Node {self.node_id}: Propagated ABORT to n{node_id}")
                    except:
                        pass
        
        return pb2.Empty()
    
    def SendFail(self, request, context):
        if request.node_id == self.node_id:
            self.is_failed = True
            
            if self.is_leader:
                print(f"Node {self.node_id}: I was the leader - triggering election in cluster")
                self.is_leader = False
                
                cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
                for node_id in cluster_nodes:
                    if node_id != self.node_id:
                        for attempt in range(3):
                            try:
                                stub = self._get_stub(node_id)
                                stub.SendLeaderFailed(
                                    pb2.FailMessage(sender=self.node_id, node_id=self.node_id), 
                                    timeout=2.0
                                )
                                print(f"Node {self.node_id}: Notified n{node_id} of leader failure")
                                break
                            except Exception as e:
                                if attempt < 2:
                                    time.sleep(0.1)
                                else:
                                    print(f"Node {self.node_id}: Failed to notify n{node_id} after 3 attempts")
        return pb2.Empty()
    
    def SendLeaderFailed(self, request, context):
        if self.is_failed:
            return pb2.Empty()
        
        failed_leader = request.node_id
        print(f"Node {self.node_id}: Notified that leader n{failed_leader} has failed - activating election")
        
        self._election_forced = True
        
        def trigger_election():
            self._trigger_leader_election()
        
        threading.Thread(target=trigger_election, daemon=True).start()
        
        return pb2.Empty()
    
    def SendRecover(self, request, context):
        if request.node_id == self.node_id:
            self.is_failed = False
            self.last_heartbeat = time.time()
            
            if not self.is_leader:
                print(f"Node {self.node_id}: Requesting state synchronization from leader...")
                self._sync_state_from_leader()
            
            self.monitoring_started = False
        return pb2.Empty()
    
    def _sync_state_from_leader(self):
        cluster_nodes = config.CLUSTER_MAPPING[self.cluster_id]
        
        for potential_leader in cluster_nodes:
            if potential_leader == self.node_id:
                continue
            
            try:
                stub = self._get_stub(potential_leader)
                response = stub.GetAllBalances(pb2.Empty(), timeout=5.0)
                
                synced_count = 0
                for entry in response.balances:
                    item_id = entry.item_id
                    leader_balance = entry.balance
                    my_balance = self.db.get_balance(item_id, config.INITIAL_BALANCE)
                    
                    if my_balance != leader_balance:
                        print(f"Node {self.node_id}: Syncing item {item_id}: {my_balance} -> {leader_balance}")
                        self.db.set_balance(item_id, leader_balance)
                        self.modified_items.add(item_id)
                        synced_count += 1
                
                print(f"Node {self.node_id}: Successfully synced {synced_count} items from leader n{potential_leader}")
                return
                
            except Exception as e:
                print(f"Node {self.node_id}: Failed to sync from n{potential_leader}: {type(e).__name__}")
                continue
        
        print(f"Node {self.node_id}: WARNING - Could not sync state from any leader!")

    def _add_to_retry_queue(self, txn_id, failed_nodes):
        with self.retry_queue_lock:
            retry_item = {
                'txn_id': txn_id,
                'node_ids': failed_nodes,
                'timestamp': time.time(),
                'attempts': 0
            }
            self.propagation_retry_queue.append(retry_item)
            print(f"Node {self.node_id}: Added {len(failed_nodes)} nodes to retry queue for {txn_id}")
    
    def _propagation_retry_worker(self):
        print(f"Node {self.node_id}: Starting propagation retry worker")
        
        while not self.is_failed:
            time.sleep(5.0)
            
            if not self.is_leader:
                continue
            
            with self.retry_queue_lock:
                if not self.propagation_retry_queue:
                    continue
                retry_item = self.propagation_retry_queue.pop(0)
            
            txn_id = retry_item['txn_id']
            node_ids = retry_item['node_ids']
            attempts = retry_item['attempts']
            
            with self.wal_lock:
                txn_entry = None
                for entry in reversed(self.wal):
                    if entry.get("txn_id") == txn_id:
                        txn_entry = entry
                        break
                
                if not txn_entry or txn_entry.get("status") != "COMMITTED":
                    continue
                
                txn_data = txn_entry.get("transaction")
            
            still_failed = []
            for node_id in node_ids:
                try:
                    stub = self._get_stub(node_id)
                    
                    prepare_request = pb2.PrepareMessage(
                        sender=self.node_id,
                        transaction_id=txn_id,
                        transaction=txn_data
                    )
                    try:
                        stub.SendPrepare(prepare_request, timeout=2.0)
                        print(f"Node {self.node_id}: Retry PREPARE sent to n{node_id}")
                    except:
                        pass
                    
                    commit_request = pb2.Commit2PCMessage(
                        sender=self.node_id,
                        transaction_id=txn_id
                    )
                    stub.SendCommit2PC(commit_request, timeout=3.0)
                    print(f"Node {self.node_id}: Retry COMMIT successful for n{node_id}")
                except Exception as e:
                    print(f"Node {self.node_id}: Retry failed for n{node_id}: {type(e).__name__}")
                    still_failed.append(node_id)
            
            if still_failed:
                retry_item['node_ids'] = still_failed
                retry_item['attempts'] = attempts + 1
                
                if retry_item['attempts'] < 5:
                    with self.retry_queue_lock:
                        self.propagation_retry_queue.append(retry_item)
                else:
                    print(f"Node {self.node_id}: GAVE UP retrying {txn_id} after 5 attempts")
    
    

    def GetBalance(self, request: pb2.BalanceRequest, context):
        if self.is_failed:
            return pb2.BalanceResponse(balance=0)

        item_id = request.item_id

        if config.get_cluster_for_item(item_id) != self.cluster_id:
            print(f"Node n{self.node_id}: Item {item_id} does not belong to my cluster C{self.cluster_id}")
            return pb2.BalanceResponse(balance=0)
        
        balance = self.db.get_balance(item_id, config.INITIAL_BALANCE)
            
        return pb2.BalanceResponse(balance=balance)
    
    def set_balance_internal(self, item_id, balance):
        self.db.set_balance(item_id, balance)
    
    def delete_balance_internal(self, item_id):
        self.db.delete_balance(item_id)
    
    def GetAllBalances(self, request, context):
        modified_balances = self.db.get_modified_balances()
        balances = [pb2.BalanceEntry(item_id=item_id, balance=balance) 
                    for item_id, balance in modified_balances.items()]
        return pb2.AllBalancesResponse(balances=balances)
    
    def Flush(self, request, context):
        self.db.flush_all()
        
        shard_items = config.get_items_for_cluster(self.cluster_id)
        self.db.initialize_balances(shard_items, config.INITIAL_BALANCE)
        
        with self.lock_table_lock:
            self.locks.clear()
        with self.wal_lock:
            self.wal.clear()
        with self.paxos_lock:
            self.accepted_ballot.clear()
            self.accepted_value.clear()
            self.committed_seq.clear()
            self.sequence_number = 0
        
        with self.election_lock:
            self.promised_ballot = 0
            self.election_in_progress = False
        
        with self.view_lock:
            self.current_view = 0
        
        with self.consensus_lock:
            self.accept_count.clear()
            self.accepted_nodes.clear()
        
        with self.twopc_lock:
            self.prepared_count.clear()
            self.prepared_nodes.clear()
            self.ack_count.clear()
        
        self.pending_2pc.clear()
        
        self.modified_items.clear()
        
        with self.view_change_lock:
            self.view_changes.clear()
        
        with self.reshard_lock:
            self.reshard_records.clear()
        
        with self.retry_queue_lock:
            self.propagation_retry_queue.clear()
        
        self.is_failed = False
        self.is_leader = (self.node_id == config.get_leader_node(self.cluster_id))
        
        with self.paxos_lock:
            if self.is_leader:
                self.ballot_number = self.node_id
            else:
                self.ballot_number = 0
        
        return pb2.Empty()


class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.servicer = NodeServicer(node_id)
        
        options = [
            ('grpc.max_concurrent_streams', 100),
            ('grpc.so_reuseport', 1),              
            ('grpc.max_receive_message_length', 50 * 1024 * 1024),  
            ('grpc.max_send_message_length', 50 * 1024 * 1024),     
        ]
        
        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=50),
            options=options
        )
        pb2_grpc.add_NodeServiceServicer_to_server(self.servicer, self.server)
        self.server.add_insecure_port(config.get_node_address(node_id))
        self.server.start()
    
    def shutdown(self):
        self.server.stop(grace=1)
    
    def flush(self):
        self.servicer.Flush(pb2.Empty(), None)
    
    def get_balance(self, item_id):
        response = self.servicer.GetBalance(pb2.BalanceRequest(item_id=item_id), None)
        return response.balance
    
    def set_balance(self, item_id, balance):
        self.servicer.set_balance_internal(item_id, balance)
    
    def delete_balance(self, item_id):
        self.servicer.delete_balance_internal(item_id)
    
    def get_all_balances(self):
        response = self.servicer.GetAllBalances(pb2.Empty(), None)
        return {entry.item_id: entry.balance for entry in response.balances}
    
    def get_view_changes(self):
        with self.servicer.view_change_lock:
            return list(self.servicer.view_changes)
