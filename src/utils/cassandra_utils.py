from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, SimpleStatement

class CassandraInstance:
    def __init__(self, contact_points=['193.166.180.240'], port=12001, local_dc='DATACENTER1', protocol_version=4):
        cluster = Cluster(
            contact_points=contact_points,
            port=port,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=local_dc),
            protocol_version=protocol_version
        )
        self.instance = cluster.connect()
    
    def prepare_query(self, query):
        """Helper function to prepare CQL queries."""
        return self.instance.prepare(query)
    
    def insert(self, keyspace_name, table_name, data):
        """Generic function to insert data into a Cassandra table."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {keyspace_name}.{table_name} ({columns}) VALUES ({placeholders})"
        prepared = self.prepare_query(query)
        self.instance.execute(prepared, tuple(data.values()))

    def update(self, keyspace_name, table_name, data, conditions):
        """Generic function to update data in a Cassandra table."""
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
        query = f"UPDATE {keyspace_name}.{table_name} SET {set_clause} WHERE {where_clause}"
        prepared = self.prepare_query(query)
        self.instance.execute(prepared, tuple(data.values()) + tuple(conditions.values()))

    def delete(self, keyspace_name, table_name, conditions):
        """Generic function to delete data from a Cassandra table."""
        where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
        query = f"DELETE FROM {keyspace_name}.{table_name} WHERE {where_clause}"
        prepared = self.prepare_query(query)
        self.instance.execute(prepared, tuple(conditions.values()))

    def batch_insert(self, keyspace_name, table_name, data_list):
        """Generic function to batch insert data into a Cassandra table."""
        columns = ', '.join(data_list[0].keys())
        placeholders = ', '.join(['?'] * len(data_list[0]))
        query = f"INSERT INTO {keyspace_name}.{table_name} ({columns}) VALUES ({placeholders})"
        prepared = self.prepare_query(query)
        batch = BatchStatement()
        for data in data_list:
            batch.add(prepared, tuple(data.values()))
        self.instance.execute(batch)

    def batch_update(self, keyspace_name, table_name, data_list, condition_keys):
        """Generic function to batch update data in a Cassandra table."""
        if not data_list:
            return
        set_clause = ', '.join([f"{k} = ?" for k in data_list[0].keys() if k not in condition_keys])
        where_clause = ' AND '.join([f"{k} = ?" for k in condition_keys])
        query = f"UPDATE {keyspace_name}.{table_name} SET {set_clause} WHERE {where_clause}"
        prepared = self.prepare_query(query)
        batch = BatchStatement()
        for data in data_list:
            values = [data[k] for k in data.keys() if k not in condition_keys] + [data[k] for k in condition_keys]
            batch.add(prepared, tuple(values))
        self.instance.execute(batch)

# HIDE MANAGEMENT KEYSPACES AND TABLES
HIDE_SYSTEM_TABLES = True
SYSTEM_TABLES = ['system_auth', 'system_schema', 'system_distributed', 'system', 'system_traces']

class CassandraInstance:
    def __init__(self, contact_points=['193.166.180.240'], port=12001, local_dc='DATACENTER1', protocol_version=4):
        cluster = Cluster(
            contact_points=contact_points,
            port=port,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=local_dc),
            protocol_version=protocol_version
        )
        self.instance = cluster.connect()
    
    def prepare_query(self, query):
        """Helper function to prepare CQL queries."""
        return self.instance.prepare(query)
    
    def insert(self, keyspace_name, table_name, data):
        """Generic function to insert data into a Cassandra table."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        query = f"INSERT INTO {keyspace_name}.{table_name} ({columns}) VALUES ({placeholders})"
        prepared = self.prepare_query(query)
        self.instance.execute(prepared, tuple(data.values()))

    def update(self, keyspace_name, table_name, data, conditions):
        """Generic function to update data in a Cassandra table."""
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
        query = f"UPDATE {keyspace_name}.{table_name} SET {set_clause} WHERE {where_clause}"
        prepared = self.prepare_query(query)
        self.instance.execute(prepared, tuple(data.values()) + tuple(conditions.values()))

    def delete(self, keyspace_name, table_name, conditions):
        """Generic function to delete data from a Cassandra table."""
        where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
        query = f"DELETE FROM {keyspace_name}.{table_name} WHERE {where_clause}"
        prepared = self.prepare_query(query)
        self.instance.execute(prepared, tuple(conditions.values()))

    def batch_insert(self, keyspace_name, table_name, data_list):
        """Generic function to batch insert data into a Cassandra table."""
        columns = ', '.join(data_list[0].keys())
        placeholders = ', '.join(['?'] * len(data_list[0]))
        query = f"INSERT INTO {keyspace_name}.{table_name} ({columns}) VALUES ({placeholders})"
        prepared = self.prepare_query(query)
        batch = BatchStatement()
        for data in data_list:
            batch.add(prepared, tuple(data.values()))
        self.instance.execute(batch)

    def batch_update(self, keyspace_name, table_name, data_list, condition_keys):
        """Generic function to batch update data in a Cassandra table."""
        if not data_list:
            return

        data_keys = data_list[0].keys()
        set_clause = ', '.join([f"{k} = ?" for k in data_keys if k not in condition_keys])
        where_clause = ' AND '.join([f"{k} = ?" for k in condition_keys])
        query = f"UPDATE {keyspace_name}.{table_name} SET {set_clause} WHERE {where_clause}"
        prepared = self.prepare_query(query)

        batch = BatchStatement()
        for data in data_list:
            values = [data[k] for k in data_keys if k not in condition_keys] + [data[k] for k in condition_keys]
            batch.add(prepared, tuple(values))
        self.instance.execute(batch)