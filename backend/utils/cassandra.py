from cassandra.cluster import Cluster

# HIDE MANAGEMENT KEYSPACES AND TABLES
HIDE_SYSTEM_TABLES = True
SYSTEM_TABLES = ['system_auth', 'system_schema', 'system_distributed', 'system', 'system_traces']

class create_cassandra_instance:
    def __init__(self):
        cluster = Cluster([('localhost', 12001), ('localhost', 12002)])
        self.instance = cluster.connect()
        
    # FULL DATABASE OVERVIEW (KEYSPACES)
    def db_overview(self):
        container = {}

        # PERFORM THE TABLE QUERY
        table_query = self.instance.execute(
            f"SELECT keyspace_name, table_name FROM system_schema.tables"
        )

        for row in table_query:
            if HIDE_SYSTEM_TABLES and row.keyspace_name in SYSTEM_TABLES:
                continue

            if row.keyspace_name not in container:
                container[row.keyspace_name] = []

            container[row.keyspace_name].append(row.table_name)

        return container
    
    # KEYSPACE OVERVIEW (TABLES)
    def keyspace_overview(self, keyspace_name):
        container = {}

        # STOP & THROW ERROR IF KEYSPACE DOESNT EXIST
        if keyspace_name not in self.db_overview().keys():
            raise Exception(f"KEYSPACE '{keyspace_name}' DOES NOT EXIST")

        # PERFORM THE TABLE QUERY
        table_query = self.instance.execute(
            f"SELECT * FROM system_schema.columns WHERE keyspace_name= '{keyspace_name}'"
        )

        # LOOP IN TABLE STRUCTURES
        for row in table_query:

            if row.table_name not in container:
                container[row.table_name] = {
                    'n_rows': None,
                    'columns': {},
                    'indexing': {},
                }

            # PUSH IN COL_NAME => TYPE KEYS UNDER TABLE
            container[row.table_name]['columns'][row.column_name] = row.type

            # APPEND PRIMARY KEYS WHEN APPROPRIATE
            if row.kind != 'regular':
                container[row.table_name]['indexing'][row.kind] = row.column_name

        # ADD TABLE ROW COUNT
        for table_name in container.keys():
            count_query = self.instance.execute(f"SELECT COUNT(*) FROM {keyspace_name}.{table_name}")
            container[table_name]['n_rows'] = count_query.one()[0]

        return container

    # TABLE OVERVIEW
    def table_overview(self, keyspace_name, table_name):
        container = []

        db_content = self.db_overview()

        if keyspace_name not in db_content.keys():
            raise Exception(f"KEYSPACE '{keyspace_name}' DOES NOT EXIST")
        
        if table_name not in db_content[keyspace_name]:
            raise Exception(f"TABLE '{keyspace_name}.{table_name}' DOES NOT EXIST")

        return container
    
    def create_keyspace(self, keyspace_name):
        self.instance.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {
                'class': 'SimpleStrategy', 
                'replication_factor': '1'
            };
        """ % keyspace_name)

    # CREATE TABLE QUERY
    def create_table(self, domain, columns, indexing):
        domain_parts = domain.split('.')

        if len(domain_parts) != 2:
            raise Exception('BAD TABLE DOMAIN FORMAT (keyspace_name.table_name)')

        db_content = self.db_overview()
        keyspace_name = domain_parts[0]

        # IF KEYSPACE DOES NOT EXIST, CREATE IT
        if keyspace_name not in db_content.keys():
            self.create_keyspace(keyspace_name)

        # MAKE SURE PRIMARY KEYS ARE OK
        for key in indexing:
            col_list = list(columns.keys())
            if key not in col_list:
                raise Exception(f"PRIMARY KEY '{key}' IS NOT A VALID COLUMN")
        
        # BASE QUERY
        query = f'CREATE TABLE IF NOT EXISTS {domain} ('
        
        # LOOP IN COLUMNS
        for column_name, column_type in columns.items():
            query += f'{column_name} {column_type}, '
            
        # ADD PRIMARY KEYS
        key_string = ', '.join(indexing)
        query += f'PRIMARY KEY({key_string}));'

        self.instance.execute(query)

