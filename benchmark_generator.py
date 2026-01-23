import random
import argparse


class BenchmarkGenerator:
    def __init__(self, num_items=9000, num_transactions=1000):
        self.num_items = num_items
        self.num_transactions = num_transactions
    
    def generate_smallbank(self, read_write_ratio=0.5, cross_shard_ratio=0.3, skewness=0.0):
        transactions = []
        
        if skewness > 0:
            hotspot_count = max(1, int(self.num_items * 0.1))
            hotspot_items = random.sample(range(1, self.num_items + 1), hotspot_count)
            
            def select_item():
                if random.random() < skewness:
                    return random.choice(hotspot_items)
                return random.randint(1, self.num_items)
        else:
            def select_item():
                return random.randint(1, self.num_items)
        
        for _ in range(self.num_transactions):
            if random.random() < read_write_ratio:
                client_id = select_item()
                transactions.append(f"s {client_id}")
            else:
                if random.random() < cross_shard_ratio:
                    sender_shard = random.randint(0, 2)
                    receiver_shard = random.choice([i for i in range(3) if i != sender_shard])
                    
                    sender = random.randint(sender_shard * 3000 + 1, (sender_shard + 1) * 3000)
                    receiver = random.randint(receiver_shard * 3000 + 1, (receiver_shard + 1) * 3000)
                else:
                    shard = random.randint(0, 2)
                    sender = random.randint(shard * 3000 + 1, (shard + 1) * 3000)
                    receiver = random.randint(shard * 3000 + 1, (shard + 1) * 3000)
                    while receiver == sender:
                        receiver = random.randint(shard * 3000 + 1, (shard + 1) * 3000)
                
                amount = random.randint(1, 10)
                transactions.append(f"{sender} {receiver} {amount}")
        
        transactions.append("F")
        return transactions
    
    def generate_ycsb(self, workload_type='A', cross_shard_ratio=0.3):
        workload_config = {
            'A': 0.5,
            'B': 0.95,
            'C': 1.0,
        }
        
        read_ratio = workload_config.get(workload_type, 0.5)
        return self.generate_smallbank(read_ratio, cross_shard_ratio, skewness=0.0)
    
    def generate_tpcc(self):        
        transactions = []
        
        for _ in range(self.num_transactions):
            tx_type = random.random()
            
            if tx_type < 0.45:
                sender = random.randint(1, self.num_items)
                receiver = random.randint(1, self.num_items)
                while receiver == sender:
                    receiver = random.randint(1, self.num_items)
                amount = random.randint(1, 20)
                transactions.append(f"{sender} {receiver} {amount}")
            
            elif tx_type < 0.88:
                sender = random.randint(1, self.num_items)
                receiver = random.randint(1, self.num_items)
                while receiver == sender:
                    receiver = random.randint(1, self.num_items)
                amount = random.randint(1, 10)
                transactions.append(f"{sender} {receiver} {amount}")
            
            else:
                client_id = random.randint(1, self.num_items)
                transactions.append(f"s {client_id}")
        
        transactions.append("F")
        return transactions
    
    def save_to_file(self, transactions, filename):
        """Save transactions to file"""
        with open(filename, 'w') as f:
            for tx in transactions:
                f.write(tx + '\n')


def main():
    parser = argparse.ArgumentParser(description='Generate benchmark workloads')
    parser.add_argument('--type', choices=['smallbank', 'ycsb', 'tpcc'], default='smallbank',
                       help='Benchmark type')
    parser.add_argument('--output', '-o', default='workload.txt',
                       help='Output file')
    parser.add_argument('--transactions', '-n', type=int, default=1000,
                       help='Number of transactions')
    parser.add_argument('--read-ratio', '-r', type=float, default=0.5,
                       help='Read-only transaction ratio (0-1)')
    parser.add_argument('--cross-shard-ratio', '-c', type=float, default=0.3,
                       help='Cross-shard transaction ratio (0-1)')
    parser.add_argument('--skewness', '-s', type=float, default=0.0,
                       help='Data access skewness (0=uniform, 1=highly skewed)')
    parser.add_argument('--ycsb-workload', choices=['A', 'B', 'C'], default='A',
                       help='YCSB workload type')
    
    args = parser.parse_args()
    
    generator = BenchmarkGenerator(num_items=9000, num_transactions=args.transactions)
    
    print(f"Generating {args.type} workload...")
    print(f"Transactions: {args.transactions}")
    
    if args.type == 'smallbank':
        print(f"Read ratio: {args.read_ratio}")
        print(f"Cross-shard ratio: {args.cross_shard_ratio}")
        print(f"Skewness: {args.skewness}")
        transactions = generator.generate_smallbank(
            args.read_ratio, args.cross_shard_ratio, args.skewness
        )
    elif args.type == 'ycsb':
        print(f"YCSB Workload: {args.ycsb_workload}")
        print(f"Cross-shard ratio: {args.cross_shard_ratio}")
        transactions = generator.generate_ycsb(args.ycsb_workload, args.cross_shard_ratio)
    else:
        print("TPC-C style workload")
        transactions = generator.generate_tpcc()
    
    generator.save_to_file(transactions, args.output)
    print(f"\nWorkload saved to {args.output}")
    print(f"Total lines: {len(transactions)}")


if __name__ == "__main__":
    main()
