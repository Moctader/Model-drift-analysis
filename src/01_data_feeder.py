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

load_dotenv()

config = DICT_NAMESPACE({
    'input_topic': DATA_FEEDER_TOPIC,
    'output_topic': DATA_PREPROCESSING_TOPIC,
    'cool_down': 5,
    'api_url': os.getenv('API_URL'),
    'api_token': os.getenv('API_TOKEN'),
    'api_tickers': os.getenv('API_TICKERS'),
    'api_date_from': os.getenv('API_DATE_FROM'),
    'api_date_to': os.getenv('API_DATE_TO'),
})

cassandra = CassandraInstance()
tickers = config.api_tickers.split(',')

if not all([config.api_url, config.api_token, config.api_tickers]):
    raise ValueError("API_URL, API_TOKEN, and API_TICKERS must be set.")

semaphore = asyncio.Semaphore(5)


async def fetch_data_from_api(symbol, start_date, end_date, is_real_data=True):
    async with semaphore:
        try:
            start_timestamp = int(start_date.timestamp())
            end_timestamp = int(end_date.timestamp())

            api_url = (
                f"{config.api_url}/{symbol}"
                f"?api_token={config.api_token}&fmt=json"
                f"&from={start_timestamp}&to={end_timestamp}"
            )

            response = await asyncio.to_thread(requests.get, api_url)
            response.raise_for_status()

            data = response.json()

            if not data:
                return pd.DataFrame()

            data = pd.DataFrame(data)

            if 'datetime' in data.columns:
                data['datetime'] = pd.to_datetime(
                    data['datetime'], errors='coerce')
                data = data.dropna(subset=['datetime'])
            else:
                return pd.DataFrame()

            data['symbol'] = symbol
            data['is_real_data'] = is_real_data

            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching real-time data for {symbol}: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Unexpected error during API call for {symbol}: {e}")
            return pd.DataFrame()


def fetch_version_info():
    try:
        query = f"""SELECT * FROM {CASSANDRA_KEYSPACE}.{VERSIONS_TABLE}
                    WHERE type = '{VERSION_TYPE_HISTORY_DATA}'"""
        prepared = cassandra.prepare_query(query)
        result = cassandra.instance.execute(prepared)
        data = pd.DataFrame(result)

        if data.empty:
            start_date = datetime.strptime(
                config.api_date_from, "%Y-%m-%d").date()
            end_date = datetime.strptime(config.api_date_to, "%Y-%m-%d").date()
            record = {
                "type": VERSION_TYPE_HISTORY_DATA,
                "version_no": "v1",
                "start_date": start_date,
                "end_date": end_date
            }
            cassandra.insert(CASSANDRA_KEYSPACE, VERSIONS_TABLE, record)
            return {"start_date": start_date, "end_date": end_date, "has_history": False}
        else:
            start_date = data.iloc[0]['start_date'].date()
            end_date = data.iloc[0]['end_date'].date()
            return {"start_date": start_date, "end_date": end_date, "has_history": True}
    except Exception as e:
        print(f"Error fetching version info from Cassandra: {e}")
        return {"start_date": None, "end_date": None, "has_history": False}


def fetch_existing_history_data(symbol, start_date, end_date):
    try:
        query = f"""
            SELECT * FROM {CASSANDRA_KEYSPACE}.{RAW_DATA_TABLE}
            WHERE symbol = '{symbol}'
            AND datetime >= '{start_date}'
            AND datetime <= '{end_date}'
            ALLOW FILTERING
        """
        prepared = cassandra.prepare_query(query)
        result = cassandra.instance.execute(prepared)
        data = pd.DataFrame(result)

        return data if not data.empty else pd.DataFrame()

    except ReadFailure as e:
        print(f"Error fetching existing historical data for {symbol}: {e}")
    except Exception as e:
        print(f"Unexpected error fetching data for {symbol}: {e}")
    return pd.DataFrame()


def update_version_end_date(new_end_date):
    try:
        version_info = fetch_version_info()
        current_end_date = version_info['end_date']

        if isinstance(new_end_date, datetime):
            new_end_date = new_end_date.date()

        if isinstance(current_end_date, datetime):
            current_end_date = current_end_date.date()

        if new_end_date and new_end_date > current_end_date:
            conditions = {"type": VERSION_TYPE_HISTORY_DATA}
            cassandra.update(CASSANDRA_KEYSPACE, VERSIONS_TABLE, {
                             "end_date": new_end_date}, conditions)
    except Exception as e:
        print(f"Error updating version end date: {e}")


def save_to_database(data: pd.DataFrame, batch_size=50):
    try:
        records = []
        for _, row in data.iterrows():
            record = {
                'symbol': row['symbol'],
                'timestamp': int(row['timestamp']),
                'datetime': row['datetime'].to_pydatetime(),
                'gmtoffset': int(row.get('gmtoffset', 0)),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': int(row['volume']) if pd.notnull(row['volume']) else None,
                'is_real_data': row.get('is_real_data', False)
            }
            records.append(record)

            if len(records) >= batch_size:
                cassandra.batch_insert(
                    CASSANDRA_KEYSPACE, RAW_DATA_TABLE, records)
                records.clear()

        if records:
            cassandra.batch_insert(CASSANDRA_KEYSPACE, RAW_DATA_TABLE, records)
    except Exception as e:
        print(f"Error saving data to Cassandra: {e}")


async def push_to_kafka(data, topic):
    kafka_producer = create_producer()

    try:
        for _, row in data.iterrows():
            row_dict = row.to_dict()
            row_dict['datetime'] = row_dict['datetime'].isoformat() if isinstance(
                row_dict['datetime'], pd.Timestamp) else row_dict['datetime']
            message = json.dumps(row_dict)
            kafka_producer.push_msg(topic, message)
            await asyncio.sleep(config.cool_down)
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")
    finally:
        kafka_producer.close()


async def ingest_historical_data():
    try:
        version = fetch_version_info()

        if not version['has_history']:
            tasks = []
            for ticker in tickers:
                today_start = version['start_date']
                today_end = version['end_date']

                start_datetime = datetime.combine(
                    today_start, datetime.min.time())
                end_datetime = (datetime.combine(
                    today_end, datetime.max.time()) - timedelta(seconds=1)).replace(microsecond=0)

                task = fetch_data_from_api(
                    ticker, start_datetime, end_datetime, False)
                tasks.append(task)

            all_data = await asyncio.gather(*tasks)

            for data in all_data:
                if not data.empty:
                    save_to_database(data)

        kafka_producer = create_producer()
        kafka_producer.push_msg(config.output_topic, config.output_topic)
        kafka_producer.close()
    except Exception as e:
        print(f"Error during historical data ingestion: {e}")


def compare_and_filter_new_data(existing_data, new_data):
    try:
        if not existing_data.empty:
            filtered_data = pd.merge(
                new_data,
                existing_data[['datetime']],
                on='datetime',
                how='left',
                indicator=True
            )
            new_data = filtered_data[filtered_data['_merge'] == 'left_only'].drop(columns=[
                                                                                  '_merge'])
        return new_data
    except Exception as e:
        print(f"Error comparing and filtering new data: {e}")
        return new_data


async def ingest_real_time_data():
    try:
        version = fetch_version_info()

        tasks = []
        for ticker in tickers:
            today_start = version['end_date']
            today_end = datetime.today().date()

            start_datetime = datetime.combine(today_start, datetime.min.time())
            end_datetime = (datetime.combine(
                today_end, datetime.max.time()) - timedelta(seconds=1)).replace(microsecond=0)

            task = ingest_real_time_for_ticker(
                ticker, start_datetime, end_datetime)
            tasks.append(task)

        await asyncio.gather(*tasks)
    except Exception as e:
        print(f"Error during real-time data ingestion: {e}")


async def ingest_real_time_for_ticker(ticker, start_datetime, end_datetime):
    try:
        existing_data = fetch_existing_history_data(
            ticker, start_datetime, end_datetime)
        real_time_data = await fetch_data_from_api(ticker, start_datetime, end_datetime)

        if not real_time_data.empty:
            if existing_data.empty:
                new_data = real_time_data
            else:
                new_data = compare_and_filter_new_data(
                    existing_data, real_time_data)

            if not new_data.empty:
                save_to_database(new_data)
                await push_to_kafka(new_data, config.output_topic)

        update_version_end_date(end_datetime)
    except Exception as e:
        print(f"Error ingesting real-time data for {ticker}: {e}")


async def task():
    while True:
        try:
            await asyncio.sleep(config.cool_down)
            await ingest_real_time_data()
            await asyncio.sleep(3000)
        except Exception as e:
            print(f"Error in scheduled task: {e}")


async def main():
    try:
        await task()
    except Exception as e:
        print(f"Error in main function: {e}")

# Run historical data ingestion
print("Data feeder has started.")
start_time = time.time()
try:
    asyncio.run(ingest_historical_data())
    print(f"Data feeder has finished in {time.time() - start_time:.3f}s.")
    print("Data feeder completed successfully.")
except Exception as e:
    print(f"Error during historical data ingestion: {e}")

# Start real-time data ingestion
try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Program interrupted and stopped.")
finally:
    cassandra.instance.shutdown()
