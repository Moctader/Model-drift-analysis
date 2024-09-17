import time
import pandas as pd
import numpy as np
import warnings
from evidently import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from evidently.metrics import RegressionQualityMetric
from prometheus_client import Summary
from constants.constants import DATA_PREPROCESSING_TOPIC, MODEL_TRACKER_TOPIC
import utils.kafka_clients as kafka_clients
from utils.types import DICT_NAMESPACE, KAFKA_DICT, KAFKA_PUSH_FUNC
from cassandra.query import SimpleStatement
from utils.cassandra_utils import CassandraInstance

warnings.filterwarnings("ignore", category=FutureWarning, module="sklearn")

config = DICT_NAMESPACE({
    'input_topic': MODEL_TRACKER_TOPIC,
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
def monitor_model_performance(data):
    data['datetime'] = pd.to_datetime(data['datetime'])
    data.set_index('datetime', inplace=True)

    required_columns = ['close', 'returns', 'lag_1', 'lag_3', 'ema_10', 'macd']

    for col in required_columns:
        if col not in data.columns:
            data[col] = 0

    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data.fillna(0, inplace=True)

    data = data[(data[required_columns] != 0).any(axis=1)]

    if data.empty or data.isna().any().any():
        data['predictions'] = np.random.rand(len(data))
    else:
        data['predictions'] = np.random.rand(len(data))

    column_mapping = ColumnMapping()
    column_mapping.target = 'close'
    column_mapping.prediction = 'predictions'

    report = Report(metrics=[DataDriftPreset(), RegressionQualityMetric()])
    with np.errstate(invalid='ignore'):
        report.run(reference_data=data, current_data=data,
                   column_mapping=column_mapping)

    return report


def handle_event(input_data: KAFKA_DICT, kafka_push: KAFKA_PUSH_FUNC):
    try:
        if isinstance(input_data, dict):
            return

        if input_data == MODEL_TRACKER_TOPIC:
            print("Model tracker started.")
            start_time = time.time()

            data = fetch_data()

            report = monitor_model_performance(data)

            if report.as_dict()['metrics'][0]['result']['dataset_drift']:
                kafka_push(config.output_topic, config.output_topic)
            else:
                print("No significant drift detected.")

            elapsed_time = time.time() - start_time
            print(f"Model tracker completed in {elapsed_time:.3f}s.")
            print(f"Model tracker completed.")

    except Exception as e:
        print(f"Error handling model tracking event: {e}")


kafka_clients.start_consumer_producer(config.input_topic, handle_event)
