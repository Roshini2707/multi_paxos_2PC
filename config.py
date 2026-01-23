NUM_CLUSTERS = 3
NODES_PER_CLUSTER = 3
NUM_NODES = NUM_CLUSTERS * NODES_PER_CLUSTER
INITIAL_BALANCE = 10

TOTAL_ITEMS = 9000

CLUSTER_MAPPING = {}

SHARD_MAPPING = {}

def configure_clusters(num_clusters=NUM_CLUSTERS, nodes_per_cluster=NODES_PER_CLUSTER, total_items=TOTAL_ITEMS):
    global NUM_CLUSTERS, NODES_PER_CLUSTER, NUM_NODES, TOTAL_ITEMS
    global CLUSTER_MAPPING, SHARD_MAPPING, RESHARDING_MOVES
    
    NUM_CLUSTERS = num_clusters
    NODES_PER_CLUSTER = nodes_per_cluster
    NUM_NODES = num_clusters * nodes_per_cluster
    TOTAL_ITEMS = total_items
    
    RESHARDING_MOVES = {}
    
    CLUSTER_MAPPING = {}
    node_id = 1
    for cluster_id in range(1, num_clusters + 1):
        nodes = []
        for _ in range(nodes_per_cluster):
            nodes.append(node_id)
            node_id += 1
        CLUSTER_MAPPING[cluster_id] = nodes
    
    SHARD_MAPPING = {}
    items_per_cluster = total_items // num_clusters
    
    for cluster_id in range(1, num_clusters + 1):
        start_item = (cluster_id - 1) * items_per_cluster + 1
        if cluster_id == num_clusters:
            end_item = total_items + 1
        else:
            end_item = cluster_id * items_per_cluster + 1
        
        SHARD_MAPPING[cluster_id] = list(range(start_item, end_item))
    
    print(f"CLUSTER CONFIGURATION")
    print(f"Clusters: {num_clusters}")
    print(f"Nodes per cluster: {nodes_per_cluster}")
    print(f"Total nodes: {NUM_NODES}")
    print(f"Total items: {total_items}")
    print(f"\nCluster Mapping:")
    for cluster_id, nodes in CLUSTER_MAPPING.items():
        print(f"  Cluster {cluster_id}: Nodes {nodes}")
    print(f"\nInitial Shard Distribution:")
    for cluster_id, items in SHARD_MAPPING.items():
        print(f"  Cluster {cluster_id}: Items {items[0]}-{items[-1]} ({len(items)} items)")

def get_default_configuration():
    return {
        'num_clusters': NUM_CLUSTERS,
        'nodes_per_cluster': NODES_PER_CLUSTER,
        'num_nodes': NUM_NODES,
        'total_items': TOTAL_ITEMS
    }


configure_clusters()

RESHARDING_MOVES = {}

def apply_resharding(moved_records):
    global RESHARDING_MOVES
    
    print("\n--------- Applying Resharding to Config ---------")
    for item_id, src_cluster, dest_cluster in moved_records:
        RESHARDING_MOVES[item_id] = dest_cluster
        print(f"  Item {item_id}: C{src_cluster} to  C{dest_cluster}")
    
    print(f"\n Total items resharded: {len(RESHARDING_MOVES)}")
    print(f" RESHARDING_MOVES dictionary now contains {len(RESHARDING_MOVES)} entries")
    print_cluster_sizes()

def get_current_cluster(item_id):
    global RESHARDING_MOVES
    
    if item_id in RESHARDING_MOVES:
        return RESHARDING_MOVES[item_id]
    
    return get_original_cluster(item_id)

def get_cluster_for_item(item_id):
    global RESHARDING_MOVES
    
    cluster = get_current_cluster(item_id)
    return cluster

def get_items_for_cluster(cluster_id):
    global RESHARDING_MOVES

    if cluster_id not in SHARD_MAPPING:
        return []
    
    items = SHARD_MAPPING[cluster_id].copy()
    
    items_to_add = []
    items_to_remove = []
    
    for item_id, new_cluster in RESHARDING_MOVES.items():
        if new_cluster == cluster_id:
            if item_id not in items:
                items_to_add.append(item_id)
        else:
            original_cluster = get_original_cluster(item_id)
            if original_cluster == cluster_id:
                if item_id in items:
                    items_to_remove.append(item_id)
    
    for item in items_to_add:
        items.append(item)
    for item in items_to_remove:
        items.remove(item)
    
    return items

def get_original_cluster(item_id):
    for cluster_id, items in SHARD_MAPPING.items():
        if item_id in items:
            return cluster_id
    
    items_per_cluster = TOTAL_ITEMS // NUM_CLUSTERS
    cluster_id = ((item_id - 1) // items_per_cluster) + 1
    return min(cluster_id, NUM_CLUSTERS)

def reset_resharding():
    global RESHARDING_MOVES
    RESHARDING_MOVES = {}

def print_cluster_sizes():
    print("\nCurrent Cluster Sizes:")
    for cluster_id in range(1, NUM_CLUSTERS + 1):
        items = get_items_for_cluster(cluster_id)
        original_items = SHARD_MAPPING.get(cluster_id, [])
        
        resharded_in = [i for i in items if i not in original_items]
        resharded_out = [i for i in original_items if i not in items]
        
        print(f"  Cluster {cluster_id}: {len(items)} items", end="")
        
        if resharded_in:
            print(f" (gained {len(resharded_in)})", end="")
        if resharded_out:
            print(f" (lost {len(resharded_out)})", end="")
        print()

def print_resharding_status():
    global RESHARDING_MOVES
    
    if not RESHARDING_MOVES:
        print("No resharding applied - using original shard mapping")
    else:
        print(f"\nResharding Status: {len(RESHARDING_MOVES)} items moved")
        for item_id, cluster_id in sorted(RESHARDING_MOVES.items())[:10]:
            original = get_original_cluster(item_id)
            print(f"  Item {item_id}: C{original} to C{cluster_id}")
        if len(RESHARDING_MOVES) > 10:
            print(f"  ... and {len(RESHARDING_MOVES) - 10} more")


PAXOS_TIMEOUT = 2.0
TWO_PC_TIMEOUT = 5.0
ELECTION_TIMEOUT = 1.5

ENABLE_LEADER_ELECTION = False

BASE_PORT = 5000

def get_node_port(node_id):
    return BASE_PORT + node_id

def get_node_address(node_id):
    return f'localhost:{get_node_port(node_id)}'

def get_cluster_for_node(node_id):
    for cluster_id, nodes in CLUSTER_MAPPING.items():
        if node_id in nodes:
            return cluster_id
    return None

def get_leader_node(cluster_id, live_nodes=None):
    nodes = CLUSTER_MAPPING.get(cluster_id, [])
    if not nodes:
        return None
    
    if live_nodes:
        available = [n for n in nodes if n in live_nodes]
        if available:
            return available[0]
        return None
    return nodes[0]
