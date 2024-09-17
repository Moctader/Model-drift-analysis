import os
import pickle
import time
import pandas as pd
from constants.constants import MODEL_USAGE_TOPIC, POST_PROCESSING_TOPIC, MODEL_TRACKER_TOPIC
import utils.kafka_clients as kafka_clients
from utils.types import DICT_NAMESPACE, KAFKA_DICT, KAFKA_PUSH_FUNC
from cassandra.query import SimpleStatement
from utils.cassandra_utils import CassandraInstance

config = DICT_NAMESPACE({
    'input_topic': MODEL_USAGE_TOPIC,
    'output_topic': POST_PROCESSING_TOPIC,
    'output_topic1': MODEL_TRACKER_TOPIC,
    'page_size': 1000,
})

cassandra = CassandraInstance()


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


def load_model(model_tag):
    try:
        models_dir = 'models'
        model_path = os.path.join(models_dir, f'{model_tag}.pkl')

        if os.path.exists(model_path):
            with open(model_path, 'rb') as f:
                model = pickle.load(f)

            return model
        else:
            raise FileNotFoundError(f"Model file '{model_path}' not found.")
    except FileNotFoundError as e:
        raise RuntimeError(f"Error loading model: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error loading model: {e}")


def handle_event(input_data: KAFKA_DICT, kafka_push: KAFKA_PUSH_FUNC):
    try:
        print("Model usage process started.")
        start_time = time.time()

        if 'model_tag' not in input_data:
            raise ValueError("No model tag found in input data.")
        model_tag = input_data['model_tag']

        model = load_model(model_tag)

        data = fetch_data()

        required_columns = ['returns', 'lag_1', 'lag_3', 'EMA_10', 'MACD']

        for col in required_columns:
            if col not in data.columns:
                data[col] = 0

        missing_columns = [
            col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"""Missing required columns for prediction: {
                             missing_columns}""")

        X = data[required_columns].fillna(0).values
        predictions = model.predict(X)

        output_data = {'predictions': predictions.tolist()}
        print(f"Model predictions: {output_data['predictions'][:5]}...")

        kafka_push(config.output_topic, config.output_topic)
        kafka_push(config.output_topic1, config.output_topic1)

        elapsed_time = time.time() - start_time
        print(f"Model usage process completed in {elapsed_time:.3f}s.")
        print(f"Model usage process completed.")

    except ValueError as ve:
        print(f"ValueError: {ve}")
    except FileNotFoundError as fnf:
        print(f"FileNotFoundError: {fnf}")
    except RuntimeError as re:
        print(f"RuntimeError: {re}")
    except Exception as e:
        print(f"Unexpected error: {e}")


kafka_clients.start_consumer_producer(
    config.input_topic, handle_event
)
