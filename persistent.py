import sqlite3
import threading
import os
from typing import Dict, List, Optional, Set, Tuple, Any
from contextlib import contextmanager
import json

class PersistentDatabase:
    def __init__(self, node_id, db_dir = "./data"):
        self.node_id = node_id
        self.db_dir = db_dir
        
        os.makedirs(db_dir, exist_ok=True)
        
        self.db_path = os.path.join(db_dir, f"node_{node_id}.db")
        
        self._local = threading.local()
        
        self._initialize_schema()
        
        print(f"Node {node_id}: Persistent database initialized at {self.db_path}")
    
    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, 'conn'):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn
    
    @contextmanager
    def transaction(self, isolation_level = "DEFERRED"):
        conn = self._get_connection()
        conn.isolation_level = isolation_level
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.isolation_level = None
    
    def _initialize_schema(self):
        with self.transaction() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS balances (
                    item_id INTEGER PRIMARY KEY,
                    balance INTEGER NOT NULL,
                    modified INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_modified 
                ON balances(modified) WHERE modified = 1
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS locks (
                    item_id INTEGER PRIMARY KEY,
                    transaction_id TEXT,
                    acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS wal_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    txn_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    transaction_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_wal_txn_id 
                ON wal_entries(txn_id)
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS paxos_state (
                    seq INTEGER PRIMARY KEY,
                    accepted_ballot INTEGER,
                    accepted_value TEXT,
                    committed INTEGER DEFAULT 0
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS node_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS reshard_records (
                    item_id INTEGER PRIMARY KEY,
                    source_cluster INTEGER NOT NULL,
                    destination_cluster INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
    
    
    def initialize_balances(self, items, initial_balance):
        with self.transaction() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO balances (item_id, balance, modified)
                VALUES (?, ?, 0)
                """,
                [(item_id, initial_balance) for item_id in items]
            )
        print(f"Node {self.node_id}: Initialized {len(items)} items with balance {initial_balance}")
    
    def get_balance(self, item_id, default = 10):
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT balance FROM balances WHERE item_id = ?",
            (item_id,)
        )
        row = cursor.fetchone()
        return row['balance'] if row else default
    
    def set_balance(self, item_id, balance, mark_modified = True):
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO balances (item_id, balance, modified, last_updated)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (item_id, balance, 1 if mark_modified else 0)
            )
    
    def delete_balance(self, item_id):
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM balances WHERE item_id = ?",
                (item_id,)
            )
    
    def update_balance(self, item_id, delta):
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE balances 
                SET balance = balance + ?, 
                    modified = 1,
                    last_updated = CURRENT_TIMESTAMP
                WHERE item_id = ?
                """,
                (delta, item_id)
            )
    
    def get_all_balances(self):
        conn = self._get_connection()
        cursor = conn.execute("SELECT item_id, balance FROM balances")
        return {row['item_id']: row['balance'] for row in cursor}
    
    def get_modified_balances(self):
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT item_id, balance FROM balances WHERE modified = 1 ORDER BY item_id"
        )
        return {row['item_id']: row['balance'] for row in cursor}
    
    def get_modified_items(self):
        conn = self._get_connection()
        cursor = conn.execute("SELECT item_id FROM balances WHERE modified = 1")
        return {row['item_id'] for row in cursor}
    
    def clear_balances(self):
        with self.transaction() as conn:
            conn.execute("DELETE FROM balances")
    
    def acquire_lock(self, item_id, transaction_id):
        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO locks (item_id, transaction_id)
                    VALUES (?, ?)
                    """,
                    (item_id, transaction_id)
                )
            return True
        except sqlite3.IntegrityError:
            return False
    
    def release_lock(self, item_id):
        with self.transaction() as conn:
            conn.execute("DELETE FROM locks WHERE item_id = ?", (item_id,))
    
    def get_lock(self, item_id):
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT transaction_id FROM locks WHERE item_id = ?",
            (item_id,)
        )
        row = cursor.fetchone()
        return row['transaction_id'] if row else None
    
    def clear_locks(self):
        with self.transaction() as conn:
            conn.execute("DELETE FROM locks")
    
    
    def append_wal(self, txn_id, status, transaction_data = None):
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO wal_entries (txn_id, status, transaction_data)
                VALUES (?, ?, ?)
                """,
                (txn_id, status, json.dumps(transaction_data) if transaction_data else None)
            )
    
    def get_wal_entry(self, txn_id):
        conn = self._get_connection()
        cursor = conn.execute(
            """
            SELECT status, transaction_data, created_at 
            FROM wal_entries 
            WHERE txn_id = ? 
            ORDER BY id DESC 
            LIMIT 1
            """,
            (txn_id,)
        )
        row = cursor.fetchone()
        if not row:
            return None
        
        return {
            'txn_id': txn_id,
            'status': row['status'],
            'transaction_data': json.loads(row['transaction_data']) if row['transaction_data'] else None,
            'created_at': row['created_at']
        }
    
    def get_all_wal_entries(self):
        conn = self._get_connection()
        cursor = conn.execute(
            """
            SELECT txn_id, status, transaction_data, created_at 
            FROM wal_entries 
            ORDER BY id
            """
        )
        entries = []
        for row in cursor:
            entries.append({
                'txn_id': row['txn_id'],
                'status': row['status'],
                'transaction_data': json.loads(row['transaction_data']) if row['transaction_data'] else None,
                'created_at': row['created_at']
            })
        return entries
    
    def update_wal_status(self, txn_id, new_status):
        with self.transaction() as conn:
            cursor = conn.execute(
                "SELECT id FROM wal_entries WHERE txn_id = ? ORDER BY id DESC LIMIT 1",
                (txn_id,)
            )
            row = cursor.fetchone()
            if row:
                conn.execute(
                    "UPDATE wal_entries SET status = ? WHERE id = ?",
                    (new_status, row['id'])
                )
    
    def clear_wal(self):
        with self.transaction() as conn:
            conn.execute("DELETE FROM wal_entries")

    
    def set_paxos_state(self, seq, ballot, value = None, committed = False):
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO paxos_state (seq, accepted_ballot, accepted_value, committed)
                VALUES (?, ?, ?, ?)
                """,
                (seq, ballot, value, 1 if committed else 0)
            )
    
    def get_paxos_state(self, seq):
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT accepted_ballot, accepted_value, committed FROM paxos_state WHERE seq = ?",
            (seq,)
        )
        row = cursor.fetchone()
        if not row:
            return None
        
        return {
            'seq': seq,
            'accepted_ballot': row['accepted_ballot'],
            'accepted_value': json.loads(row['accepted_value']) if row['accepted_value'] else None,
            'committed': bool(row['committed'])
        }
    
    def get_all_paxos_state(self):
        conn = self._get_connection()
        cursor = conn.execute("SELECT seq, accepted_ballot, accepted_value, committed FROM paxos_state")
        result = {}
        for row in cursor:
            result[row['seq']] = {
                'accepted_ballot': row['accepted_ballot'],
                'accepted_value': json.loads(row['accepted_value']) if row['accepted_value'] else None,
                'committed': bool(row['committed'])
            }
        return result
    
    def clear_paxos_state(self):
        with self.transaction() as conn:
            conn.execute("DELETE FROM paxos_state")
    
    
    def set_metadata(self, key, value):
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO node_metadata (key, value)
                VALUES (?, ?)
                """,
                (key, json.dumps(value))
            )
    
    def get_metadata(self, key, default = None):
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT value FROM node_metadata WHERE key = ?",
            (key,)
        )
        row = cursor.fetchone()
        if not row:
            return default
        return json.loads(row['value'])
    
    def clear_metadata(self):
        with self.transaction() as conn:
            conn.execute("DELETE FROM node_metadata")
    
    
    def add_reshard_record(self, item_id, source_cluster, destination_cluster):
        try:
            with self.transaction() as conn:
                conn.execute(
                    """
                    INSERT INTO reshard_records (item_id, source_cluster, destination_cluster)
                    VALUES (?, ?, ?)
                    """,
                    (item_id, source_cluster, destination_cluster)
                )
        except sqlite3.IntegrityError:
            pass
    
    def get_reshard_records(self):
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT item_id, source_cluster, destination_cluster FROM reshard_records"
        )
        return {(row['item_id'], row['source_cluster'], row['destination_cluster']) for row in cursor}
    
    def clear_reshard_records(self):
        with self.transaction() as conn:
            conn.execute("DELETE FROM reshard_records")
    
    def flush_all(self):
        with self.transaction() as conn:
            conn.execute("DELETE FROM balances")
            conn.execute("DELETE FROM locks")
            conn.execute("DELETE FROM wal_entries")
            conn.execute("DELETE FROM paxos_state")
            conn.execute("DELETE FROM node_metadata")
            conn.execute("DELETE FROM reshard_records")
        print(f"Node {self.node_id}: Database flushed")
    
    def checkpoint(self):
        conn = self._get_connection()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    
    def vacuum(self):
        conn = self._get_connection()
        conn.execute("VACUUM")
    
    def get_stats(self):
        conn = self._get_connection()
        
        stats = {}
        cursor = conn.execute("SELECT COUNT(*) as count FROM balances")
        stats['total_items'] = cursor.fetchone()['count']
        
        cursor = conn.execute("SELECT COUNT(*) as count FROM balances WHERE modified = 1")
        stats['modified_items'] = cursor.fetchone()['count']
        
        cursor = conn.execute("SELECT COUNT(*) as count FROM locks")
        stats['active_locks'] = cursor.fetchone()['count']
        
        cursor = conn.execute("SELECT COUNT(*) as count FROM wal_entries")
        stats['wal_entries'] = cursor.fetchone()['count']
        
        cursor = conn.execute("SELECT COUNT(*) as count FROM paxos_state")
        stats['paxos_sequences'] = cursor.fetchone()['count']
        
        return stats
    
    def close(self):
        if hasattr(self._local, 'conn'):
            self._local.conn.close()
            delattr(self._local, 'conn')


if __name__ == "__main__":
    import tempfile
    import shutil
    
    test_dir = tempfile.mkdtemp()
    
    try:
        db = PersistentDatabase(node_id=1, db_dir=test_dir)
        db.initialize_balances([1, 2, 3, 4, 5], initial_balance=100)
        
        print("\n=== Balance Operations ===")
        print(f"Balance of item 1: {db.get_balance(1)}")
        db.set_balance(1, 150)
        print(f"Updated balance of item 1: {db.get_balance(1)}")
        db.update_balance(1, -20)
        print(f"After delta -20: {db.get_balance(1)}")
        
        print("\n------- Lock Operations ------")
        print(f"Acquire lock on item 2: {db.acquire_lock(2, 'txn_123')}")
        print(f"Try acquire same lock: {db.acquire_lock(2, 'txn_456')}")
        print(f"Lock holder: {db.get_lock(2)}")
        db.release_lock(2)
        print(f"After release: {db.get_lock(2)}")
        
        print("\n------ WAL Operations --------")
        db.append_wal('txn_001', 'PREPARED', {'sender': 1, 'receiver': 2, 'amount': 10})
        db.append_wal('txn_001', 'COMMITTED')
        entry = db.get_wal_entry('txn_001')
        print(f"Latest WAL entry: {entry}")
        
        print("\n----- Testing Persistence -------")
        db.close()
        
        db2 = PersistentDatabase(node_id=1, db_dir=test_dir)
        print(f"After reopen, balance of item 1: {db2.get_balance(1)}")
        print(f"Modified items: {db2.get_modified_items()}")
        
        stats = db2.get_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
        
        db2.close()
        
    finally:
        shutil.rmtree(test_dir)
        print("\n------ Test completed -------")
