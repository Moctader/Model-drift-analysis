import os
import pickle
import pandas as pd
import numpy as np
import time
import bentoml
import optuna
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, precision_score, recall_score, f1_score
from vectorbt import Portfolio
from cassandra.query import SimpleStatement
from constants.constants import CASSANDRA_KEYSPACE, MODEL_RESULTS_TABLE, MODEL_TRAINING_TOPIC, MODEL_USAGE_TOPIC
from utils.cassandra_utils import CassandraInstance
import utils.kafka_clients as kafka_clients
from utils.types import DICT_NAMESPACE, KAFKA_DICT, KAFKA_PUSH_FUNC

config = DICT_NAMESPACE({
    'input_topic': MODEL_TRAINING_TOPIC,
    'output_topic': MODEL_USAGE_TOPIC,
    'page_size': 1000,
    'cool_down': 5,
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


def split_data_by_time(data, validation_split=0.2):
    split_index = int(len(data) * (1 - validation_split))
    train_data = data.iloc[:split_index]
    test_data = data.iloc[split_index:]

    return train_data, test_data


def prepare_features_and_labels(data):
    required_columns = ['returns', 'lag_1', 'lag_3', 'EMA_10', 'MACD']

    data = data.copy()

    for col in required_columns:
        if col not in data.columns:
            data[col] = 0

    X = data[required_columns].fillna(0).replace([np.inf, -np.inf], 0).values
    y = data['close'].fillna(0).replace([np.inf, -np.inf], 0).values

    return X, y


def train_linear_regression_model(X_train, y_train):
    model = LinearRegression()
    model.fit(X_train, y_train)

    return model


def optimize_hyperparameters(train_data, val_data):
    def objective(trial):
        X_train, y_train = prepare_features_and_labels(train_data)
        X_val, y_val = prepare_features_and_labels(val_data)
        alpha = trial.suggest_float("alpha", 0.0001, 1.0)
        model = LinearRegression()
        model.fit(X_train, y_train)
        predictions = model.predict(X_val)
        mse = mean_squared_error(y_val, predictions)
        return mse
    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=20)

    return study.best_params


def evaluate_model(model, X_test, y_test):
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    rmse = np.sqrt(mse)
    y_pred = (predictions > 0).astype(int)
    y_true = (y_test > 0).astype(int)
    precision = precision_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)

    return mse, rmse, precision, recall, f1


def backtest_strategy(data):
    data.loc[:, 'datetime'] = pd.to_datetime(data['datetime'], errors='coerce')
    data = data.set_index('datetime')

    data = data.dropna(subset=['close'])

    data.loc[:, 'close'] = data['close'].replace(
        [np.inf, -np.inf], np.nan).fillna(0)
    data.loc[:, 'close'] = data['close'].apply(lambda x: max(x, 1e-5))

    data = data.asfreq(pd.infer_freq(data.index))

    pf = Portfolio.from_signals(
        close=data['close'],
        entries=data['returns'] > 0,
        exits=data['returns'] < 0,
        init_cash=100000,
        fees=0.001
    )
    performance_metrics = {
        "total_return": pf.total_return(),
        "sharpe_ratio": pf.sharpe_ratio(),
        "max_drawdown": pf.max_drawdown(),
        "annualized_volatility": pf.annualized_volatility(),
        "annualized_return": pf.annualized_return(),
        "cumulative_return": pf.total_return(),
        "num_trades": pf.trades.count(),
        "winning_trades": pf.trades.winning.count(),
        "losing_trades": pf.trades.losing.count()
    }

    return performance_metrics


def signal_strength(performance_metrics):
    sharpe_ratio = performance_metrics['sharpe_ratio']
    max_drawdown = performance_metrics['max_drawdown']
    cumulative_return = performance_metrics['cumulative_return']
    signal = sharpe_ratio * 0.5 + \
        (1 - max_drawdown / 10) * 0.3 + cumulative_return * 0.2

    return signal * 100


def deploy_model(model, is_green=True):
    model_tag = None
    try:
        models_dir = 'models'
        os.makedirs(models_dir, exist_ok=True)

        bento_svc = bentoml.sklearn.save_model(
            f"financial_trading_model", model)
        model_tag = str(bento_svc.tag).replace(":", "_")
        model_path = os.path.join(
            models_dir, f'{model_tag}.pkl')

        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
    except Exception as e:
        raise RuntimeError(f"Error during deployment: {e}")
    return model_tag


def save_model_metrics_to_db(model_tag, start_time, end_time, metrics):
    try:
        record = {
            'id': int(time.time()),
            'start_datetime': start_time,
            'end_datetime': end_time,
            'model_name': "financial_trading_model",
            'model_tag': model_tag,
            'version': model_tag.split("_")[-1],
            'rmse': metrics.get('rmse', 0),
            'mse': metrics.get('mse', 0),
            'avg_mse': metrics.get('avg_mse', 0),
            'precision': metrics.get('precision', 0),
            'recall': metrics.get('recall', 0),
            'f1_score': metrics.get('f1_score', 0),
            'sharpe_ratio': metrics.get('sharpe_ratio', 0),
            'max_drawdown': metrics.get('max_drawdown', 0),
            'total_return': metrics.get('total_return', 0),
            'annualized_volatility': metrics.get('annualized_volatility', 0),
            'annualized_return': metrics.get('annualized_return', 0),
            'cumulative_return': metrics.get('cumulative_return', 0),
            'num_trades': metrics.get('num_trades', 0),
            'winning_trades': metrics.get('winning_trades', 0),
            'losing_trades': metrics.get('losing_trades', 0)
        }

        cassandra.insert(CASSANDRA_KEYSPACE, MODEL_RESULTS_TABLE, record)
    except Exception as e:
        raise RuntimeError(f"Error saving model metrics to database: {e}")


def handle_event(input_data: KAFKA_DICT, kafka_push: KAFKA_PUSH_FUNC):
    try:
        if isinstance(input_data, dict):
            return

        if input_data == MODEL_TRAINING_TOPIC:
            print("Model training has started.")
            start_time = time.time()

            data = fetch_data()
            train_data, val_data = split_data_by_time(data)
            best_params = optimize_hyperparameters(train_data, val_data)
            print(f"Optimal hyperparameters: {best_params}")

            X_train, y_train = prepare_features_and_labels(train_data)
            X_val, y_val = prepare_features_and_labels(val_data)
            model = train_linear_regression_model(X_train, y_train)
            mse, rmse, precision, recall, f1 = evaluate_model(
                model, X_val, y_val)
            backtest_results = backtest_strategy(val_data)

            if backtest_results["sharpe_ratio"] < 1.0 or backtest_results["max_drawdown"] > 10:
                raise RuntimeError("Backtesting failed: retrain the model.")
            signal_strength_score = signal_strength(backtest_results)

            if signal_strength_score < 70:
                print("Signal strength is weak, manual intervention required.")
                return

            model_tag = deploy_model(model, is_green=True)
            metrics = {
                'mse': mse,
                'rmse': rmse,
                'precision': precision,
                'recall': recall,
                'f1_score': f1,
                'total_return': backtest_results['total_return'],
                'sharpe_ratio': backtest_results['sharpe_ratio'],
                'max_drawdown': backtest_results['max_drawdown'],
                'annualized_volatility': backtest_results['annualized_volatility'],
                'annualized_return': backtest_results['annualized_return'],
                'cumulative_return': backtest_results['cumulative_return'],
                'num_trades': backtest_results['num_trades'],
                'winning_trades': backtest_results['winning_trades'],
                'losing_trades': backtest_results['losing_trades']
            }

            save_model_metrics_to_db(
                model_tag, start_time, time.time(), metrics)
            kafka_push(config.output_topic, {"model_tag": model_tag})

            elapsed_time = time.time() - start_time
            print(f"Model training has finished in {elapsed_time:.3f}s.")
            print("Model training completed successfully.")
    except Exception as e:
        print(f"Error handling event: {e}")


kafka_clients.start_consumer_producer(config.input_topic, handle_event)
