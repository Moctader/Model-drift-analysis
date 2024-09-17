import sys
import os
import pandas as pd
import numpy as np
import logging
import vectorbt as vbt
from sklearn.impute import SimpleImputer
import plotly.graph_objects as go
from utils.cassandra_utils import CassandraInstance
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement
import yaml
import os
import pandas as pd
import json
import time
import asyncio
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from utils.cassandra_utils import CassandraInstance
from utils.kafka_utils import create_producer
from cassandra import ReadFailure
from constants.constants import DATA_FEEDER_TOPIC, DATA_PREPROCESSING_TOPIC, CASSANDRA_KEYSPACE, RAW_DATA_TABLE, VERSION_TYPE_HISTORY_DATA, VERSIONS_TABLE
from utils.types import DICT_NAMESPACE
import os
import logging
import json
import time
import pandas as pd
import requests
from utils.types import DICT_NAMESPACE
from utils.kafka_utils import create_producer
from utils.cassandra_utils import CassandraInstance
from dotenv import load_dotenv
from typing import Dict, List, Text
from evidently import ColumnMapping
from evidently.metric_preset import DataDriftPreset
from evidently.metrics import DatasetSummaryMetric
from evidently.report import Report
from evidently.metrics import ColumnDriftMetric
from evidently.metrics import RegressionQualityMetric
import random
import datetime
from typing import Dict, Tuple
from model_training import train_model, is_exists, update_model_results_to_database, deploy_model


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to the Cassandra cluster
cluster = Cluster(
    contact_points=['193.166.180.240'],
    port=12001,
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='DATACENTER1'),
    protocol_version=4
)
session = cluster.connect()
session.set_keyspace('data')
cassandra = CassandraInstance()



def data_ingestion(symbol: str) -> pd.DataFrame:
    try:
        api_url = (
            f"{'https://eodhd.com/api/intraday'}/{'EURUSD.FOREX'}?api_token={'658e841fa1a6f9.30546411'}&fmt=json"
        )

        response = requests.get(api_url)
        response.raise_for_status()

        data = pd.DataFrame(response.json())
        data['symbol'] = symbol

        # Ensure the datetime column is in the correct format
        if 'datetime' in data.columns:
            data['datetime'] = pd.to_datetime(data['datetime'])

        # Convert columns to match Cassandra schema
        data['timestamp'] = pd.to_numeric(
            data['timestamp'], errors='coerce', downcast='integer')
        data['open'] = pd.to_numeric(data['open'], errors='coerce')
        data['high'] = pd.to_numeric(data['high'], errors='coerce')
        data['low'] = pd.to_numeric(data['low'], errors='coerce')
        data['close'] = pd.to_numeric(data['close'], errors='coerce')
        data['volume'] = pd.to_numeric(
            data['volume'], errors='coerce', downcast='integer')
        data['version'] = 1

        # Replace NaN or Null values with 0
        data = data.fillna(0)

        logger.info(f"Data ingested for {symbol}.")

        return data
    except requests.exceptions.RequestException as e:
        raise Exception(f"Network error for {symbol}: {e}")
    except ValueError as e:
        raise Exception(f"Parsing error for {symbol}: {e}")
    except Exception as e:
        raise Exception(f"Error ingesting data for {symbol}: {e}")
    


def save_to_database(data: pd.DataFrame, batch_size=50):
    try:
        records = []

        for _, row in data.iterrows():
            record = {
                'symbol': row['symbol'],
                'timestamp': int(row['timestamp']),
                'datetime': row['datetime'].to_pydatetime(),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume']) if pd.notnull(row['volume']) else None,
                'version': int(row['version']),
            }

            records.append(record)

            if len(records) >= batch_size:
                cassandra.batch_insert(
                    'golam', 'stock_price', records)
                records.clear()

        if records:
            cassandra.batch_insert(
                'golam', 'stock_price', records)

        print("Data saved to Cassandra in batches.")
    except Exception as e:
        raise Exception(f"Failed to save data to Cassandra: {e}")



########################################################################################################
# Read data from the Cassandra table
def fetch_data_from_database():
    query = "SELECT * FROM golam.stock_price"
    prepared = cassandra.prepare_query(query)
    result = cassandra.instance.execute(prepared)
    data = pd.DataFrame(result)

    logger.info("Data fetched from database successfully.")

    return data


def process_data(data: pd.DataFrame) -> pd.DataFrame:
    data['datetime'] = pd.to_datetime(data['datetime'])
    data.set_index('datetime', inplace=True)
    data['returns'] = data['close'].pct_change()

    # Replace infinite values with NaN to avoid float conversion errors
    data['returns'] = data['returns'].replace(
        [float('inf'), float('-inf')], np.nan)

    # Ensure all NaN values are handled before processing
    imputer = SimpleImputer(strategy='mean')
    data['returns'] = imputer.fit_transform(data[['returns']])

    logger.info("Data processed successfully.")

    return data.reset_index()


def save_processed_data_to_database(data: pd.DataFrame, batch_size=50):
    records = []
    for _, row in data.iterrows():
        record = {
            'symbol': row['symbol'],
            'timestamp': int(row['timestamp']),
            'datetime': row['datetime'].to_pydatetime(),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': int(row['volume']),
            'returns': float(row['returns']),
            'version': int(row.get('version', 1)),
        }
        records.append(record)

        if len(records) >= batch_size:
            cassandra.batch_insert(
                'golam', 'stock_price_processed', records)
            records.clear()

    if records:
        cassandra.batch_insert('golam',
                               'stock_price_processed', records)
    logger.info("Data saved to database successfully.")

########################################################################################################

# Fetch data from the database
def fetch_data_from_database():
    query = "SELECT * FROM golam.stock_price_processed"
    result = session.execute(query)
    data = pd.DataFrame(result)
    logger.info("Data fetched from database successfully.")
    return data

############################################################################################################
# Prepare the reference data
import joblib
from pathlib import Path

def prepare_reference_dataset(df: pd.DataFrame):
    """Prepare reference dataset for monitoring."""
    df = df.reset_index()
    target_col = "close"  # Use the same target as in training
    prediction_col = "predictions"
    features = ['returns']
    
    # Split the data into reference data (excluding the last day) and current data (only the last day)
    last_day = df['datetime'].max().date()
    reference_data = df[df['datetime'].dt.date < last_day].copy()
    current_data = df[df['datetime'].dt.date == last_day].copy()
   
    # Prepare scoring data for reference and current data
    reference_scoring_data = reference_data[features]
    reference_target_data = reference_data[target_col]
    
    model = joblib.load("models/financial_trading_model.pkl")
    reference_data[prediction_col] = model.predict(reference_scoring_data)
    

    REFERENCE_DATA_DIR = "data/reference"
    Path(REFERENCE_DATA_DIR).mkdir(exist_ok=True)
    path = f"{REFERENCE_DATA_DIR}/reference_data.parquet"

    reference_data.to_parquet(path)
    logger.info(f"Reference data saved to {path}")
    #show_reference_data_head(path)
    current_data=prepare_current_data(current_data)
    return reference_data, current_data


#prepare the current data

def prepare_current_data(current_data):
    """Load the saved model and make predictions on the current data."""
    prediction_col = "predictions"
    features = ['returns']
    model = joblib.load("models/financial_trading_model.pkl")
    # Prepare scoring data for current data
    current_scoring_data = current_data[features]
    # Generate predictions for current data
    current_data[prediction_col] = model.predict(current_scoring_data)

    CURRENT_DATA_DIR = "data/current"
    Path(CURRENT_DATA_DIR).mkdir(exist_ok=True)
    current_path = f"{CURRENT_DATA_DIR}/current_data.parquet"

    current_data.to_parquet(current_path)
    logger.info(f"Current data saved to {current_path}")
    return current_data

############################################################################################################
def numpy_to_standard_types(input_data: Dict) -> Dict:

    output_data: Dict = {}

    for k, v in input_data.items():
        if isinstance(v, np.generic):
            v = v.item()
        output_data[k] = v

    return output_data

# Save the Report
def save_report(report, directory, filename):
    """Save the report to the specified directory and filename."""
    Path(directory).mkdir(parents=True, exist_ok=True)
    file_path = f"{directory}/{filename}"
    report.save_html(file_path)
    logging.info(f"Report saved to {file_path}")


############################################################################################################

# Generate data quality and data drift reports and

def generate_reports(
    current_data: pd.DataFrame,
    reference_data: pd.DataFrame,
    num_features: List[Text],
    cat_features: List[Text],
    prediction_col: Text,
    timestamp: float,
) -> None:
    """
    Generate data quality and data drift reports and
    commit metrics to the database.

    Args:
        current_data (pd.DataFrame):
            The current DataFrame with features and predictions.
        reference_data (pd.DataFrame):
            The reference DataFrame with features and predictions.
        num_features (List[Text]):
            List of numerical feature column names.
        cat_features (List[Text]):
            List of categorical feature column names.
        prediction_col (Text):
            Name of the prediction column.
        timestamp (float):
            Metric pipeline execution timestamp.
    """

    logging.info("Prepare column mapping")
    column_mapping = ColumnMapping()
    column_mapping.numerical_features = num_features
    column_mapping.categorical_features = cat_features
    column_mapping.prediction = prediction_col

    logging.info("Data quality report")
    data_quality_report = Report(metrics=[DatasetSummaryMetric()])
    data_quality_report.run(
        reference_data=reference_data,
        current_data=current_data,
        column_mapping=column_mapping,
    )
    save_report(data_quality_report, "../Drift_Reports", "data_quality_report.html")

    logging.info("Data drift report")
    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(
        reference_data=reference_data,
        current_data=current_data,
        column_mapping=column_mapping,
    )
    save_report(data_drift_report, "../Drift_Reports", "data_drift_report.html")

    logging.info("Commit metrics into database")
    data_quality_report_content: Dict = data_quality_report.as_dict()
    data_drift_report_content: Dict = data_drift_report.as_dict()
    commit_data_metrics_to_db(
        data_quality_report=data_quality_report_content,
        data_drift_report=data_drift_report_content,
        timestamp=timestamp,
    )

def monitor_data(current_data, ts) -> None:
    """Build and save data validation reports.

    Args:
        ts (str): ISO 8601 formatted timestamp.
    """
    num_features = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    cat_features = []
    prediction_col = "predictions"

    # Prepare reference data
    DATA_REF_DIR = "data/reference"
    ref_path = f"{DATA_REF_DIR}/reference_data.parquet"
    ref_data = pd.read_parquet(ref_path)
    columns: List[Text] = num_features + cat_features + [prediction_col]
    reference_data = ref_data.loc[:, columns]


    if current_data.shape[0] == 0:
        # Skip monitoring if current data is empty
        logging.warning("Current data is empty. Skipping monitoring.")
        return

    # Prepare column_mapping object
    # for Evidently reports and generate reports
    generate_reports(
        current_data=current_data,
        reference_data=reference_data,
        num_features=num_features,
        cat_features=cat_features,
        prediction_col=prediction_col,
        timestamp=pendulum.parse(ts).timestamp(),
        target_col="close",
    )
############################################################################################################

# Build and save monitoring reports.

def generate_reports(
    current_data: pd.DataFrame,
    reference_data: pd.DataFrame,
    num_features: List[Text],
    cat_features: List[Text],
    prediction_col: Text,
    target_col: Text,
    timestamp: float,
) -> None:
 

    print("Prepare column_mapping object for Evidently reports")
    column_mapping = ColumnMapping()
    column_mapping.target = target_col
    column_mapping.prediction = prediction_col
    column_mapping.numerical_features = num_features
    column_mapping.categorical_features = cat_features

    logging.info("Create a model performance report")
    model_performance_report = Report(metrics=[RegressionQualityMetric()])
    model_performance_report.run(
        reference_data=reference_data,
        current_data=current_data,
        column_mapping=column_mapping,
    )
    save_report(model_performance_report, "../Drift_Reports", "model_performance_report.html")

    logging.info("Target drift report")
    target_drift_report = Report(metrics=[ColumnDriftMetric(target_col)])
    target_drift_report.run(
        reference_data=reference_data,
        current_data=current_data,
        column_mapping=column_mapping,
    )
    save_report(target_drift_report, "../Drift_Reports", "target_drift_report.html")


    logging.info("Save metrics to database")
    model_performance_report_content: Dict = model_performance_report.as_dict()
    target_drift_report_content: Dict = target_drift_report.as_dict()
    commit_data_metrics_to_db1(
        model_performance_report=model_performance_report_content,
        target_drift_report=target_drift_report_content,
        timestamp=timestamp,
    )


def monitor_model(current_data, ts) -> None:
    """Build and save monitoring reports.

    Args:
        ts (pendulum.DateTime, optional): Timestamp.
        interval (int, optional): Interval. Defaults to 60.
    """

    DATA_REF_DIR = "data/reference"
    target_col = "close"
    num_features = ['datetime','open', 'high', 'low', 'close', 'volume']
    cat_features = []
    prediction_col = "predictions"

    if current_data.shape[0] == 0:
        # Skip monitoring if current data is empty
        # Usually it may happen for few first batches
        print("Current data is empty!")
        print("Skip model monitoring")

    else:

        # Prepare reference data
        ref_path = f"{DATA_REF_DIR}/reference_data.parquet"
        ref_data = pd.read_parquet(ref_path)
        columns: List[Text] = num_features + cat_features + [prediction_col]
        reference_data = ref_data.loc[:, columns]
        # Generate reports
        generate_reports(
            current_data=current_data,
            reference_data=reference_data,
            num_features=num_features,
            cat_features=cat_features,
            prediction_col=prediction_col,
            target_col=target_col,
            timestamp=pendulum.parse(ts).timestamp(),
        )

############################################################################################################
#Directory containing model files
models_directory = 'models/'

# Load configuration
with open('drift-yaml/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Function to select a model by name
def select_model(config, model_name):
    for model in config['models']:
        if model['name'] == model_name:
            return model
    return None


# Function to read model files from the models directory
def read_models_from_directory(directory):
    model_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    return model_files

model_files =read_models_from_directory(models_directory)


# Check each model file against the configuration
selected_model = None
for model_file in model_files:
    model_name, _ = os.path.splitext(model_file)  # Assuming model name is the file name without extension
    selected_model = select_model(config, model_name)
    if selected_model:
        logger.info(f"Selected model configuration: {selected_model}")
        print(f"Selected model configuration: {selected_model}")
        break
############################################################################################################

if selected_model:
    def parse_model_performance_report(model_performance_report: Dict) -> Dict:
        """Parse model performance report and return metrics results.

        """

        assert len(model_performance_report["metrics"]) == 1
        quality_metric: Dict = model_performance_report["metrics"][0]
        assert quality_metric["metric"] == "RegressionQualityMetric"
        raw_quality_metric_result: Dict = quality_metric["result"]
        current_metrics: Dict = raw_quality_metric_result['current']
        raw_quality_metric_result.update(current_metrics)
        quality_metric_result: Dict = {
            k: v
            for k, v in raw_quality_metric_result.items()
            if isinstance(v, (int, float, str, np.generic))
        }
        quality_metric_result = numpy_to_standard_types(quality_metric_result)
        # Check if any metric exceeds its threshold

        
        performance_thresholds = selected_model['performance_thresholds']

        if quality_metric_result['rmse'] > performance_thresholds['rmse']:
            logger.info("Model performance drift detected: RMSE exceeds threshold")

        if quality_metric_result['mean_abs_error'] > performance_thresholds['mean_abs_error']:
            logger.info("Model performance drift detected: Mean Absolute Error exceeds threshold")

        if quality_metric_result['r2_score'] < performance_thresholds['r2_score']:
            logger.info("Model performance drift detected: R2 Score is below threshold")

        return quality_metric_result


if selected_model:
    def parse_target_drift_report(target_drift_report: Dict) -> Dict:
        """Parse target drift report and return metrics results.
        """

        assert len(target_drift_report["metrics"]) == 1
        drift_metric: Dict = target_drift_report["metrics"][0]
        assert drift_metric["metric"] == "ColumnDriftMetric"
        raw_drift_metric_result: Dict = drift_metric["result"]
        drift_metric_result: Dict = {
            k: v
            for k, v in raw_drift_metric_result.items()
            if isinstance(v, (int, float, str, np.generic))
        }
        drift_metric_result = numpy_to_standard_types(drift_metric_result)
        remove_fields: List[Text] = ["column_name", "column_type"]

        for field in remove_fields:
            del drift_metric_result[field]
        print(drift_metric_result)



        drift_thresholds = selected_model['drift_thresholds']
        if drift_metric_result['drift_score'] > drift_thresholds['drift_score']:
            logger.info("Model drift detected: Drift score exceeds threshold")

        if drift_metric_result['drift_detected'] != drift_thresholds['drift_detected']:
            logger.info("Model drift detected: Drift detected status does not match threshold")

        return drift_metric_result

############################################################################################################



def commit_data_metrics_to_db(
    data_quality_report: Dict, data_drift_report: Dict, timestamp: float) -> None:
    """Commit data metrics to database.

    Args:
        data_quality_report (Dict): Data quality report
        data_drift_report (Dict): Data drift report
        timestamp (float): Metrics calculation timestamp
    """
    ### DATA QUALITY
    # dataset_summary_metric_result: Dict = parse_data_quality_report(data_quality_report)
    # dataset_summary_metric_result['id'] = random.randint(0, 2**31 - 1)    # Generate a unique id
    # insert_data_quality(dataset_summary_metric_result)


    # ### DATA DRIFT PREDICTION   
    # drift_report_results: Tuple[Dict, Dict] = parse_data_drift_report(data_drift_report)
    # dataset_drift_result: Dict = drift_report_results[0]
    # data_drift_prediction_result: Dict = drift_report_results[1]
    # data_drift_prediction_result['id'] = random.randint(0, 2**31 - 1)    # Generate a unique id
    # insert_data_drift_prediction(data_drift_prediction_result)
    pass



def commit_data_metrics_to_db1(model_performance_report: Dict, target_drift_report: Dict, timestamp: float) -> None:

    ### MODEL PERFORMANCE REPORT
    model_performance_report = parse_model_performance_report(model_performance_report)
    model_performance_report['id'] = datetime.datetime.now()
    #insert_model_performance(model_performance_report)


    ### TARGET DRIFT REPORT
    target_drift_report = parse_target_drift_report(target_drift_report)
    target_drift_report['id'] = datetime.datetime.now()
    # insert_target_drift(target_drift_report)


############################################################################################################
import pendulum

def drift_monitoring():
    df=fetch_data_from_database()
    reference_data, current_data=prepare_reference_dataset(df)

    model=train_model(df)
    deploy_model(model)

    # Check if the model exists

    ts = pendulum.now().to_iso8601_string()
        
    monitor_data(current_data, ts)
    monitor_model(current_data, ts)
    




def main():
    try:
        all_data = pd.DataFrame()
        data= data_ingestion('EURUSD.FOREX')
        all_data = pd.concat([all_data, data], ignore_index=True)

        if not all_data.empty:
            df=save_to_database(all_data)

        data = fetch_data_from_database()
        data=process_data(data)
        save_processed_data_to_database(data)
        drift_monitoring()
     

    except KeyboardInterrupt:
        logger.info('Process stopped.')
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == '__main__':
    main()