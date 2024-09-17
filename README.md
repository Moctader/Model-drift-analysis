## PROJECT DOMAINS

- Monitoring Stack:
    - Prometheus: [`localhost:9090`]('localhost:9090)
        - Example endpoint: [`localhost:13000`]('localhost:13000)
        - `TODO: IMPLEMENT MODEL DRIFT ENDPOINT`
    - Grafana: [`localhost:9091`]('localhost:9091)
        - [Cadvisor Docker Metrics](http://localhost:9091/d/4dMaCsRZz/cadvisor-docker-metrics)
        - [Node Exporter HW Metrics](http://localhost:9091/d/rYdddlPWk/node-exporter-hw-metrics)
        - [Kafka Metrics](http://localhost:9091/d/5nhADrDWk/kafka-cluster-metrics)
        - [Cassandra DB Metrics](http://localhost:9091/d/000000086/cassandra-cluster-metrics)
        - Flink Cluster Metrics
        - `TODO: CREATE THE MODEL DRIFT DASHBOARD`

---
    
- Kafka Stack:
    - Broker 1: `localhost:11001`
    - Broker 2: `localhost:11002`
- Cassandra Stack:
    - Broker 1: `localhost:12001`
    - Broker 2: `localhost:12002`
---
- Backend API:
    - Kafka:
        - List all topics: `localhost:3003/kafka`
        - Topic details: `localhost:3003/kafka/<topic_name>`
        - Create default topics: `localhost:3003/kafka/init`
    - Cassandra:
        - List all keyspaces and tables: `localhost:3003/cassandra`
        - List tables details in keyspace: `localhost:3003/cassandra/<keyspace_name>`
        - List table contents: `localhost:3003/cassandra/<keyspace_name>/<table_name>`
        - Create default tables: `localhost:3003/cassandra/init`


# Clear everything of Docker
```bash
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker rmi $(docker images -q) -f
docker volume rm $(docker volume ls -q)
```

# See the data in Cassandra
```bash
docker exec -it docker_env-cassandra_1-1 cqlsh
docker exec -it docker_env-cassandra_2-1 cqlsh
USE data_engineering_key_space;
DESCRIBE TABLES;
SELECT count(*) FROM stock_prices;
ALTER TABLE stock_prices WITH gc_grace_seconds = 86400; 
```

```bash
pip install --upgrade pip setuptools wheel
```
