#classify whether the stock price will go up or down based on historical data
import logging
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
import bentoml
import os
import pickle
import utils.kafka_clients as kafka_clients
from utils.types import DICT_NAMESPACE, KAFKA_DICT, KAFKA_PUSH_FUNC
from utils.cassandra_utils import CassandraInstance
from typing import Optional
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score
import logging
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score

config = DICT_NAMESPACE({
    'input_pipeline_topic': 'model_training',
    'output_pipeline_topic': 'model_usage',
})

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
cassandra = CassandraInstance()


def fetch_data_from_database():
    query = "SELECT * FROM golam.stock_price_processed"
    prepared = cassandra.prepare_query(query)
    result = cassandra.instance.execute(prepared)
    data = pd.DataFrame(result)
    logger.info("Data fetched from database successfully.")
    return data


def is_exists(model_name: str):
    query = "SELECT * FROM golam.model_result"
    prepared = cassandra.prepare_query(query)
    result = cassandra.instance.execute(prepared)

    return bool(result)

def update_model_results_to_database(
    model_name: str,
    score: Optional[float] = None,
    mse: Optional[float] = None,
    avg_mse: Optional[float] = None,
    model_tag: Optional[str] = None
):
    data = {}
    if score is not None:
        data['score'] = float(score)
    if mse is not None:
        data['mse'] = float(mse)
    if avg_mse is not None:
        data['avg_mse'] = float(avg_mse)
    if model_tag is not None:
        data['model_tag'] = str(model_tag)  # type: ignore

    if data:
        conditions = {'id': 1}
        cassandra.update('golam',
                         'model_result', data, conditions)
        logger.info(
            (
                f"Model results for '{model_name}' "
                f"updated in database successfully."
            )
        )


def train_model(data: pd.DataFrame):
    # Feature extraction
    X = data['returns'].values.reshape(-1, 1)
    
    # Define the target variable for classification
    data['target'] = (data['close'].shift(-1) > data['close']).astype(int)
    y = data['target'].values[:-1]  # Remove the last value which will be NaN due to shift
    X = X[:-1]  # Align X with y

    # Impute missing values in X
    imputer = SimpleImputer(strategy='median')
    X = imputer.fit_transform(X)

    # Initialize and train the Random Forest classification model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    # Evaluate the model
    y_pred = model.predict(X)
    accuracy = accuracy_score(y, y_pred)
    logger.info(f"Model trained with accuracy: {accuracy}")

    # Record initial model metadata
    initial_record = {
        'id': 1,
        'datetime': pd.Timestamp.now().to_pydatetime(),
        'model_name': "financial_trading_model",
        'model_tag': "",
        'score': float(accuracy),
        'mse': 0.0,
        'avg_mse': 0.0,
        'total_return': 0.0,
        'version': 1,
    }

    if is_exists("classification_financial_trading_model"):
        update_model_results_to_database(
            "classification_financial_trading_model", score=float(accuracy))
    else:
        cassandra.insert('golam',
                         'model_result', initial_record)

    return model


def evaluate_model(model, features: pd.DataFrame):
    X = features['returns'].values.reshape(-1, 1)
    y = features['close'].values

    mask = ~pd.isna(y)
    X = X[mask]
    y = y[mask]

    predictions = model.predict(X)

    if pd.isna(predictions).any():
        logger.error("Predictions contain NaN values.")
        raise ValueError("Predictions contain NaN values.")

    mse = mean_squared_error(y, predictions)
    logger.info(f"Model evaluation completed with MSE: {mse}")

    update_model_results_to_database("classification_classification_financial_trading_model", mse=mse)


def validate_model(model, validation_data: pd.DataFrame):
    tscv = TimeSeriesSplit(n_splits=5)
    X = validation_data['returns'].values.reshape(-1, 1)
    y = validation_data['close'].values

    imputer = SimpleImputer(strategy='median')
    y = imputer.fit_transform(y.reshape(-1, 1)).ravel()

    mse_scores = []

    for train_index, test_index in tscv.split(X):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        mse_scores.append(mse)

    avg_mse = np.mean(mse_scores)
    logger.info(f"Model validation completed with average MSE: {avg_mse}")

    update_model_results_to_database(
        "classification_classification_financial_trading_model", avg_mse=avg_mse)


def deploy_model(model):
    models_dir = 'models'
    os.makedirs(models_dir, exist_ok=True)

    bento_svc = bentoml.sklearn.save_model("classification_classification_financial_trading_model", model)
    model_tag = str(bento_svc.tag)

    model_tag = model_tag.replace(":", "_")
    model_path = os.path.join(models_dir, 'classification_classification_financial_trading_model.pkl')

    with open(model_path, 'wb') as f:
        pickle.dump(model, f)

    logger.info(f"Model saved manually to {model_path}")
    logger.info(f"Model deployment completed: {model_tag}")

    return model_tag


def handle_pipeline_input_event(input_data: KAFKA_DICT, kafka_push: KAFKA_PUSH_FUNC):
    try:
        logger.info("Received pipeline event for model training.")
        data = fetch_data_from_database()

        model = train_model(data)

        evaluate_model(model, data)

        validate_model(model, data)

        model_tag = deploy_model(model)

        update_model_results_to_database(
            "classification_classification_financial_trading_model", model_tag="classification_classification_financial_trading_model")

        kafka_push(config.output_pipeline_topic, {"model_tag": model_tag})
        logger.info("Data pushed to Kafka for model deployment successfully.")
    except Exception as e:
        logger.error(f"Error processing pipeline input event: {e}")
    finally:
        #kafka_push.__self__.close()
        logger.info("Kafka producer closed.")


if __name__ == '__main__':
    kafka_clients.start_consumer_producer(
        config.input_pipeline_topic, handle_pipeline_input_event
    )