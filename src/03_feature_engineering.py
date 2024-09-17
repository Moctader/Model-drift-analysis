import pandas as pd
import numpy as np
import time
import warnings
from dotenv import load_dotenv
import utils.kafka_clients as kafka_clients
from constants.constants import CASSANDRA_KEYSPACE, STOCK_PRICE_FEATURES_TABLE, FEATURE_ENGINEERING_TOPIC, MODEL_TRAINING_TOPIC
from utils.cassandra_utils import CassandraInstance
from utils.types import DICT_NAMESPACE, KAFKA_DICT, KAFKA_PUSH_FUNC
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands
from ta.trend import MACD
from cassandra.query import SimpleStatement

np.seterr(invalid='ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)

load_dotenv()

config = DICT_NAMESPACE({
    'input_topic': FEATURE_ENGINEERING_TOPIC,
    'output_topic': MODEL_TRAINING_TOPIC,
})

cassandra = CassandraInstance()


def clean_data(data):
    data.replace([np.inf, -np.inf], np.nan, inplace=True)
    data.fillna(method='ffill', inplace=True)
    data.fillna(method='bfill', inplace=True)
    return data


def fetch_data_chunk(statement, page_size=1000):
    try:
        result_set = cassandra.instance.execute(statement)
        return result_set
    except Exception as e:
        print(f"Error fetching data chunk from Cassandra: {e}")
        return None


def concat_data_chunks(result_set):
    data = pd.DataFrame()
    try:
        while result_set:
            current_page = pd.DataFrame(result_set.current_rows)
            data = pd.concat([data, current_page], ignore_index=True)
            if result_set.has_more_pages:
                result_set = result_set.fetch_next_page()
            else:
                break
        return data
    except Exception as e:
        print(f"Error concatenating data chunks: {e}")
        return pd.DataFrame()


def fetch_data():
    try:
        query = """
            SELECT * FROM data_engineering_key_space.stock_prices_processed
            WHERE is_real_data = false ALLOW FILTERING;
        """
        statement = SimpleStatement(query, fetch_size=1000)
        result_set = fetch_data_chunk(statement)
        if result_set is None:
            raise ValueError("Failed to retrieve data from Cassandra")
        data = concat_data_chunks(result_set)
        if data.empty:
            raise ValueError("No data fetched from Cassandra")
        data = clean_data(data)
        return data
    except ValueError as e:
        print(f"ValueError: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data from Cassandra: {e}")
        return pd.DataFrame()


def compute_ema(data, span):
    try:
        return data.ewm(span=span).mean()
    except Exception as e:
        print(f"Error computing EMA for span {span}: {e}")
        return pd.Series()


def generate_technical_indicators(data: pd.DataFrame) -> pd.DataFrame:
    try:
        data['EMA_10'] = compute_ema(data['close'], span=10)
        data['EMA_30'] = compute_ema(data['close'], span=30)
        indicator_bb = BollingerBands(
            close=data['close'], window=20, window_dev=2)
        data['bb_mavg'] = indicator_bb.bollinger_mavg()
        data['bb_hband'] = indicator_bb.bollinger_hband()
        data['bb_lband'] = indicator_bb.bollinger_lband()
        macd = MACD(close=data['close'])
        data['MACD'] = macd.macd()
        data['MACD_signal'] = macd.macd_signal()
        data['MACD_diff'] = macd.macd_diff()
        rsi = RSIIndicator(close=data['close'], window=14)
        data['RSI'] = rsi.rsi()
        return data
    except Exception as e:
        print(f"Error generating technical indicators: {e}")
        return data


def fractional_differencing(data: pd.DataFrame, column: str, d: float = 0.5) -> pd.DataFrame:
    try:
        data[f'fracdiff_{column}'] = data[column].diff().fillna(0)
        return data
    except Exception as e:
        print(f"""Error applying fractional differencing on column {
              column}: {e}""")
        return data


def generate_cyclical_feature(series, period):
    try:
        sin_feature = np.sin(2 * np.pi * series / period)
        cos_feature = np.cos(2 * np.pi * series / period)
        return sin_feature, cos_feature
    except Exception as e:
        print(f"Error generating cyclical features: {e}")
        return pd.Series(), pd.Series()


def generate_cyclical_features(data: pd.DataFrame) -> pd.DataFrame:
    try:
        data['day_of_week'] = data['datetime'].dt.dayofweek
        data['month'] = data['datetime'].dt.month
        data['day_of_week_sin'], data['day_of_week_cos'] = generate_cyclical_feature(
            data['day_of_week'], 7)
        data['month_sin'], data['month_cos'] = generate_cyclical_feature(
            data['month'], 12)
        return data
    except Exception as e:
        print(f"Error generating cyclical features: {e}")
        return data


def generate_lag_feature(series, lags):
    try:
        lag_data = {}
        for lag in lags:
            lag_data[f'lag_{lag}'] = series.shift(lag)
        return pd.DataFrame(lag_data)
    except Exception as e:
        print(f"Error generating lag features: {e}")
        return pd.DataFrame()


def generate_lag_features(data: pd.DataFrame) -> pd.DataFrame:
    try:
        lags = [1, 3, 7]
        lag_data = generate_lag_feature(data['close'], lags)
        data = pd.concat([data, lag_data], axis=1)
        return data
    except Exception as e:
        print(f"Error generating lag features: {e}")
        return data


def calculate_trading_metrics(data: pd.DataFrame) -> pd.DataFrame:
    try:
        data['close'] = data['close'].replace([np.inf, -np.inf], np.nan)
        data['close'] = data['close'].fillna(
            method='ffill').fillna(method='bfill')
        data['returns'] = data['close'].pct_change().fillna(0)
        data['cumulative_return'] = (1 + data['returns']).cumprod()
        trading_days = 252
        data['annualized_return'] = data['cumulative_return'].pow(
            1 / trading_days) - 1
        data['annualized_volatility'] = data['returns'].rolling(
            window=trading_days).std() * np.sqrt(trading_days)
        data['sharpe_ratio'] = data['annualized_return'] / \
            data['annualized_volatility']
        data['cum_return'] = (1 + data['returns']).cumprod()
        data['cum_max'] = data['cum_return'].cummax()
        data['drawdown'] = data['cum_max'] - data['cum_return']
        data['max_drawdown'] = data['drawdown'].max()
        data['equally_weighted_return'] = data[['close']].mean(axis=1)
        return data
    except Exception as e:
        print(f"Error calculating trading metrics: {e}")
        return data


def save_to_database(data: pd.DataFrame, batch_size=50):
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
                'is_real_data': bool(row['is_real_data']),
                'EMA_10': float(row['EMA_10']),
                'EMA_30': float(row['EMA_30']),
                'bb_mavg': float(row['bb_mavg']),
                'bb_hband': float(row['bb_hband']),
                'bb_lband': float(row['bb_lband']),
                'MACD': float(row['MACD']),
                'MACD_signal': float(row['MACD_signal']),
                'MACD_diff': float(row['MACD_diff']),
                'RSI': float(row['RSI']),
                'day_of_week_sin': float(row['day_of_week_sin']),
                'day_of_week_cos': float(row['day_of_week_cos']),
                'month_sin': float(row['month_sin']),
                'month_cos': float(row['month_cos']),
                'returns': float(row['returns']),
                'cumulative_return': float(row['cumulative_return']),
                'annualized_return': float(row['annualized_return']),
                'annualized_volatility': float(row['annualized_volatility']),
                'sharpe_ratio': float(row['sharpe_ratio']),
                'max_drawdown': float(row['max_drawdown']),
                'equally_weighted_return': float(row['equally_weighted_return']),
            }
            records.append(record)

            if len(records) >= batch_size:
                cassandra.batch_insert(
                    CASSANDRA_KEYSPACE, STOCK_PRICE_FEATURES_TABLE, records)
                records.clear()

        if records:
            cassandra.batch_insert(
                CASSANDRA_KEYSPACE, STOCK_PRICE_FEATURES_TABLE, records)
    except Exception as e:
        print(f"Error saving data to Cassandra: {e}")


def process_data(input_data: pd.DataFrame):
    try:
        input_data.replace([np.inf, -np.inf], np.nan, inplace=True)
        input_data.fillna(0, inplace=True)

        data = generate_technical_indicators(input_data)
        data = generate_cyclical_features(data)
        data = generate_lag_features(data)
        data = fractional_differencing(data, column='close')
        data = calculate_trading_metrics(data)

        data.replace([np.inf, -np.inf], 0, inplace=True)
        data.fillna(0, inplace=True)

        save_to_database(data)
    except Exception as e:
        print(f"Error processing data: {e}")


def handle_event(input_data: KAFKA_DICT, kafka_push: KAFKA_PUSH_FUNC):
    try:
        if isinstance(input_data, dict):
            return

        if input_data == FEATURE_ENGINEERING_TOPIC:
            print("Feature engineering has started.")
            start_time = time.time()
            data = fetch_data()
            if not data.empty:
                process_data(data)
            else:
                print("No data fetched for feature engineering.")
            kafka_push(config.output_topic, config.output_topic)
            elapsed_time = time.time() - start_time
            print(f"Feature engineering has finished in {elapsed_time:.3f}s.")
            print("Feature engineering completed successfully.")
    except Exception as e:
        print(f"Error handling event: {e}")


kafka_clients.start_consumer_producer(config.input_topic, handle_event)
