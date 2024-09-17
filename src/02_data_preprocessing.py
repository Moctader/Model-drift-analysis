import pandas as pd
import time
import warnings
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler
from constants.constants import CASSANDRA_KEYSPACE, PROCESSED_DATA_TABLE, DATA_PREPROCESSING_TOPIC, FEATURE_ENGINEERING_TOPIC
import utils.kafka_clients as kafka_clients
from utils.cassandra_utils import CassandraInstance
from utils.types import DICT_NAMESPACE, KAFKA_DICT, KAFKA_PUSH_FUNC
from cassandra.query import SimpleStatement

warnings.simplefilter(action='ignore', category=FutureWarning)

load_dotenv()

config = DICT_NAMESPACE({
    'input_topic': DATA_PREPROCESSING_TOPIC,
    'output_topic': FEATURE_ENGINEERING_TOPIC,
    'cool_down': 5,
})

cassandra = CassandraInstance()


def fetch_data():
    try:
        data = pd.DataFrame()
        page_size = 1000
        query = """
            SELECT symbol, timestamp, close, datetime, gmtoffset, high, is_real_data, low, open, volume 
            FROM data_engineering_key_space.stock_prices 
            WHERE is_real_data = false ALLOW FILTERING;
        """
        statement = SimpleStatement(query, fetch_size=page_size)
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

        return data
    except Exception as e:
        print(f"Error fetching data from Cassandra: {e}")
        return pd.DataFrame()


def process_data(data: pd.DataFrame) -> pd.DataFrame:
    try:
        data['datetime'] = pd.to_datetime(data['datetime'], errors='coerce')
        # Drop rows with invalid datetime
        data = data.dropna(subset=['datetime'])
        data.set_index('datetime', inplace=True)

        # Handle missing values using forward fill or time interpolation
        data.fillna(method='ffill', inplace=True)

        # Remove duplicates based on datetime
        data.drop_duplicates(inplace=True)

        # Normalize and rescale numerical columns using Min-Max Scaling
        columns_to_scale = ['open', 'high', 'low', 'close', 'volume']
        scaler = MinMaxScaler()
        data.loc[:, columns_to_scale] = scaler.fit_transform(
            data[columns_to_scale])

        # normalization: x - ema(x)
        for col in columns_to_scale:
            ema_col = data[col].ewm(span=12, adjust=False).mean()
            data[col] = data[col] - ema_col

        # Ensure timestamp is filled from datetime if missing
        data['timestamp'] = data.apply(
            lambda row: int(row.name.timestamp()) if pd.isna(row['timestamp']) else row['timestamp'], axis=1
        )
        data['timestamp'] = data['timestamp'].fillna(0).astype(int)
        data['volume'] = data['volume'].fillna(0).astype(int)

        return data.reset_index()
    except Exception as e:
        print(f"Error processing data: {e}")
        return pd.DataFrame()


def save_processed_data(data: pd.DataFrame, batch_size=50):
    try:
        records = []
        for _, row in data.iterrows():
            record = {
                'symbol': row['symbol'],
                'timestamp': int(row['timestamp']),
                'gmtoffset': int(row.get('gmtoffset', 0)),
                'datetime': row['datetime'].to_pydatetime(),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume']),
                'is_real_data': False,
            }
            records.append(record)
            if len(records) >= batch_size:
                cassandra.batch_insert(
                    CASSANDRA_KEYSPACE, PROCESSED_DATA_TABLE, records)
                records.clear()

        if records:
            cassandra.batch_insert(
                CASSANDRA_KEYSPACE, PROCESSED_DATA_TABLE, records)
    except Exception as e:
        print(f"Error saving processed data to Cassandra: {e}")


def handle_event(input_data: KAFKA_DICT, kafka_push: KAFKA_PUSH_FUNC):
    if isinstance(input_data, dict):
        return

    if input_data == DATA_PREPROCESSING_TOPIC:
        print("Data preprocessing has started.")
        start_time = time.time()

        try:
            data = fetch_data()
            if data.empty:
                print("No data to process.")
                return
            processed_data = process_data(data)
            if processed_data.empty:
                print("No processed data to save.")
                return
            save_processed_data(processed_data)
            kafka_push(config.output_topic, config.output_topic)
        except ValueError as e:
            print(f"Error during preprocessing: {e}")
        except Exception as e:
            print(f"Unexpected error in handle_event: {e}")

        elapsed_time = time.time() - start_time
        print(f"Data preprocessing has finished in {elapsed_time:.3f}s.")
        print("Data preprocessing completed successfully.")


# Start Kafka consumer/producer
try:
    kafka_clients.start_consumer_producer(config.input_topic, handle_event)
except Exception as e:
    print(f"Error starting Kafka consumer/producer: {e}")
