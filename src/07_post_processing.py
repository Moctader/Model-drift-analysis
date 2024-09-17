import time
import pandas as pd
import numpy as np
from prometheus_client import Summary
from constants.constants import DATA_PREPROCESSING_TOPIC, POST_PROCESSING_TOPIC
import utils.kafka_clients as kafka_clients
from utils.types import DICT_NAMESPACE, KAFKA_DICT, KAFKA_PUSH_FUNC
from cassandra.query import SimpleStatement
from utils.cassandra_utils import CassandraInstance

config = DICT_NAMESPACE({
    'input_topic': POST_PROCESSING_TOPIC,
    'output_topic': DATA_PREPROCESSING_TOPIC,
    'page_size': 1000,
})

cassandra = CassandraInstance()

REQUEST_TIME = Summary('request_processing_seconds',
                       'Time spent processing request')


def fetch_data():
    data = pd.DataFrame()
    query = f"""SELECT * FROM data_engineering_key_space.stock_price_features LIMIT {
        config.page_size};"""
    statement = SimpleStatement(query, fetch_size=config.page_size)

    try:
        result_set = cassandra.instance.execute(statement)
        while result_set:
            current_page = pd.DataFrame(result_set.current_rows)
            data = pd.concat([data, current_page], ignore_index=True)
            if result_set.has_more_pages:
                result_set = result_set.fetch_next_page()
            else:
                break
        if data.empty:
            raise ValueError("No data fetched from Cassandra")
    except Exception as e:
        raise RuntimeError(f"Error fetching data from Cassandra: {e}")

    return data


@REQUEST_TIME.time()
def perform_backtest(data):
    data['datetime'] = pd.to_datetime(data['datetime'])
    data.set_index('datetime', inplace=True)

    data['PnL'] = data['close'] - data['open']
    sharpe_ratio = data['PnL'].mean() / data['PnL'].std()

    return {
        'sharpe_ratio': sharpe_ratio,
        'total_return': data['PnL'].sum(),
        'max_drawdown': np.max(np.maximum.accumulate(data['close']) - data['close'])
    }


def handle_event(input_data: KAFKA_DICT, kafka_push: KAFKA_PUSH_FUNC):
    try:
        if isinstance(input_data, dict):
            return

        if input_data == POST_PROCESSING_TOPIC:
            print("Post-processing started.")
            start_time = time.time()

            data = fetch_data()
            performance_metrics = perform_backtest(data)

            print(f"Performance metrics: {performance_metrics}")

            if performance_metrics['sharpe_ratio'] < 1.0 or performance_metrics['max_drawdown'] > 10:
                kafka_push(config.output_topic, config.output_topic)
            else:
                print("Model is performing well, no retraining needed.")

            elapsed_time = time.time() - start_time
            print(f"Post-processing completed in {elapsed_time:.3f}s.")
            print("Post-processing completed.")

    except Exception as e:
        print(f"Error handling post-processing event: {e}")


kafka_clients.start_consumer_producer(config.input_topic, handle_event)
