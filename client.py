import grpc
import time
from typing import Dict

import distributed_pb2 as pb2
import distributed_pb2_grpc as pb2_grpc
import config


class Client:
    def __init__(self, max_retries = 3, retry_delay = 1.0):
        self.transaction_counter = 0
        self.stubs: Dict[int, pb2_grpc.NodeServiceStub] = {}
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
    def _get_stub(self, node_id) -> pb2_grpc.NodeServiceStub:
        if node_id not in self.stubs:
            channel = grpc.insecure_channel(config.get_node_address(node_id))
            self.stubs[node_id] = pb2_grpc.NodeServiceStub(channel)
        return self.stubs[node_id]


    def print_balance(self, item_id, live_nodes):
        cluster_id = config.get_cluster_for_item(item_id)
        
        if cluster_id is None:
            print(f"Error: Item {item_id} is not mapped to any cluster.")
            return

        target_nodes = config.CLUSTER_MAPPING.get(cluster_id, [])
        
        results = []
        
        for node_id in target_nodes:
            if node_id in live_nodes:
                try:
                    stub = self._get_stub(node_id)
                    response = stub.GetBalance(pb2.BalanceRequest(item_id=item_id), timeout=2.0)
                    results.append(f"n{node_id}: {response.balance}")
                except grpc.RpcError:
                    results.append(f"n{node_id}: FAILED")
                except Exception:
                    results.append(f"n{node_id}: ERROR")
            else:
                pass

        output_str = ", ".join(results)
        print(f"PrintBalance({item_id}); Output: {output_str}")

    def send_reshard(self, live_nodes):
        if not live_nodes:
            print("Error: No live nodes available to coordinate resharding.")
            return []

        cluster_1_nodes = config.CLUSTER_MAPPING[1]
        available_c1_nodes = [n for n in cluster_1_nodes if n in live_nodes]
        
        if not available_c1_nodes:
            print("Error: No Cluster 1 nodes are live. Cannot initiate resharding.")
            return []
        
        print(f"Client: Trying Cluster 1 nodes to find leader: {available_c1_nodes}")
        
        for node_id in available_c1_nodes:
            try:
                print(f"Client: Contacting Node {node_id}...")
                stub = self._get_stub(node_id)
                request = pb2.ReshardMessage(sender=0)
                
                response = stub.SendReshard(request, timeout=15.0)

                moved_records = []
                for record in response.moved_records:
                    moved_records.append((
                        record.item_id,
                        record.source_cluster,
                        record.destination_cluster
                    ))
                
                print(f"Client: Successfully completed resharding via Node {node_id}")
                print(f"Client: {len(moved_records)} records to be moved")
                return moved_records
                    
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    print(f"Client: Node {node_id} is not the leader, trying next...")
                    continue
                elif e.code() == grpc.StatusCode.UNAVAILABLE:
                    print(f"Client: Node {node_id} is unavailable, trying next...")
                    continue
                elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    print(f"Client: Node {node_id} timed out, trying next...")
                    continue
                else:
                    print(f"Client: Node {node_id} error ({e.code()}): {e.details()}")
                    continue
            except Exception as e:
                print(f"Client: Node {node_id} exception: {type(e).__name__}")
                continue
        
        print("Client: Could not find the leader in Cluster")
        return []
    
    def send_transaction(self, transaction, live_nodes):
        self.transaction_counter += 1
        if 'sender' in transaction and transaction['sender'] is not None:
            transaction_id = f"txn_{self.transaction_counter}_({transaction['sender']}->{transaction['receiver']},{transaction['amount']})"
        else:
            transaction_id = f"txn_{self.transaction_counter}_balance({transaction['item_id']})"
        
        if 'sender' in transaction and transaction['sender'] is not None:
            item_id = transaction['sender']
        else:
            item_id = transaction['item_id']
        
        cluster_id = config.get_cluster_for_item(item_id)
        
        for attempt in range(self.max_retries):
            cluster_nodes = config.CLUSTER_MAPPING[cluster_id]
            available_nodes = [n for n in cluster_nodes if n in live_nodes]
            
            if not available_nodes:
                if attempt < self.max_retries - 1:
                    print(f"Client: No available nodes in cluster {cluster_id}, retrying in {self.retry_delay}s... (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(self.retry_delay)
                    continue
                return None
            
            for leader_node in available_nodes:
                if 'sender' in transaction and transaction['sender'] is not None:
                    txn = pb2.Transaction(
                        sender=transaction['sender'],
                        receiver=transaction['receiver'],
                        amount=transaction['amount'],
                        item_id=0
                    )
                else:
                    txn = pb2.Transaction(
                        sender=0,
                        receiver=0,
                        amount=0,
                        item_id=transaction['item_id']
                    )
                
                request = pb2.RequestMessage(
                    sender=0,
                    transaction_id=transaction_id,
                    transaction=txn
                )
                
                try:
                    stub = self._get_stub(leader_node)
                    response = stub.SendRequest(request, timeout=5.0)
                    
                    txn_type = "BALANCE" if 'item_id' in transaction else "TRANSFER"
                    if txn_type == "BALANCE":
                        print(f"Client: Received reply for {transaction_id} - Status: {response.status}, Balance: {response.balance}")
                    else:
                        print(f"Client: Received reply for {transaction_id} - Status: {response.status}")
                    
                    return {
                        'status': response.status,
                        'transaction_id': transaction_id,
                        'balance': response.balance
                    }
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                        continue
                    elif e.code() == grpc.StatusCode.UNAVAILABLE:
                        continue
                    else:
                        continue
                except Exception as e:
                    continue
            
            if attempt < self.max_retries - 1:
                print(f"Client: Transaction {transaction_id} failed on all cluster nodes (attempt {attempt + 1}/{self.max_retries}), retrying in {self.retry_delay}s...")
                time.sleep(self.retry_delay)
            else:
                print(f"Client: Transaction {transaction_id} failed after {self.max_retries} attempts")
        
        return None
    
    def send_fail_command(self, node_id):
        try:
            stub = self._get_stub(node_id)
            fail_msg = pb2.FailMessage(sender=0, node_id=node_id)
            stub.SendFail(fail_msg, timeout=1.0)
            time.sleep(0.1)
        except:
            pass
    
    def send_recover_command(self, node_id):
        try:
            stub = self._get_stub(node_id)
            recover_msg = pb2.RecoverMessage(sender=0, node_id=node_id)
            stub.SendRecover(recover_msg, timeout=1.0)
            time.sleep(0.1)
        except:
            pass
    
    def reset_counter(self):
        self.transaction_counter = 0
        print("Client: Transaction counter reset to 0")
    
    def close(self):
        pass


def parse_transaction(cmd_str):
    cmd_str = cmd_str.strip()
    
    if cmd_str.startswith('F'):
        node_str = cmd_str[1:].strip('() nN')
        try:
            node_id = int(node_str)
            return {
                'type': 'FAIL',
                'node_id': node_id
            }
        except ValueError:
            return None
    
    if cmd_str.startswith('R'):
        node_str = cmd_str[1:].strip('() nN')
        try:
            node_id = int(node_str)
            return {
                'type': 'RECOVER',
                'node_id': node_id
            }
        except ValueError:
            return None
    
    cmd_str = cmd_str.replace(',', ' ')
    parts = cmd_str.split()
    
    if not parts:
        return None
    
    if len(parts) == 1 and parts[0].startswith('-'):
        try:
            item_id = int(parts[0][1:]) 
            print(f"Client: Parsed balance query for item {item_id}")
            return {
                'type': 'BALANCE',
                'item_id': item_id
            }
        except ValueError:
            return None
    
    elif len(parts) == 2 and parts[0] == 's':
        try:
            item_id = int(parts[1])
            print(f"Client: Parsed balance query for item {item_id}")
            return {
                'type': 'BALANCE',
                'item_id': item_id
            }
        except ValueError:
            return None
    
    elif len(parts) >= 3:
        try:
            sender = int(parts[0].strip('() ,'))
            receiver = int(parts[1].strip('() ,'))
            amount = int(parts[2].strip('() ,'))
            return {
                'type': 'TRANSFER',
                'sender': sender,
                'receiver': receiver,
                'amount': amount
            }
        except ValueError:
            return None
    
    elif len(parts) == 1:
        try:
            item_id = int(parts[0].strip('()'))
            print(f"Client: Read-Only query for item {item_id}")
            return {
                'type': 'BALANCE',
                'item_id': item_id
            }
        except ValueError:
            return None
    
    return None
