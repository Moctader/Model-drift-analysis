from fastapi import APIRouter, Response, status
from pydantic import BaseModel
import utils.cassandra as cassandra_utils

########################################################################################################
########################################################################################################

router = APIRouter()
cassandra = cassandra_utils.create_cassandra_instance()

########################################################################################################
########################################################################################################


@router.get('/cassandra/')
async def overview(response: Response):
    try:
        response.status_code = status.HTTP_200_OK
        return cassandra.db_overview()

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return 'ERROR: ' + str(error)

########################################################################################################
########################################################################################################


class Table(BaseModel):
    domain: str
    columns: dict
    indexing: list


@router.post('/cassandra/create')
async def create_table(table: Table, response: Response):
    try:
        response.status_code = status.HTTP_201_CREATED
        cassandra.create_table(table.domain, table.columns, table.indexing)

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return 'ERROR: ' + str(error)

########################################################################################################
########################################################################################################

# CREATE DEFAULT TABLES AFTER A FRESH DOCKER LAUNCH


@router.get('/cassandra/init')
async def init_experiment_topics(response: Response):

    try:
        response.status_code = status.HTTP_201_CREATED

        # TABLE FOR STOCK RAW DATA
        cassandra.create_table('data_engineering_key_space.versions', {
            'type': 'text',
            'version_no': 'text',
            'start_date': 'date',
            'end_date': 'date',
        }, ['type'])

        # TABLE FOR STOCK RAW DATA
        cassandra.create_table('data_engineering_key_space.stock_prices', {
            'symbol': 'text',
            'timestamp': 'bigint',
            'gmtoffset': 'int',
            'datetime': 'timestamp',
            'open': 'double',
            'high': 'double',
            'low': 'double',
            'close': 'double',
            'volume': 'int',
            'is_real_data': 'boolean',
        }, ['symbol', 'timestamp'])

        # TABLE FOR PROCESSED STOCK PROCESSED DATA
        cassandra.create_table('data_engineering_key_space.stock_prices_processed', {
            'symbol': 'text',
            'timestamp': 'bigint',
            'gmtoffset': 'int',
            'datetime': 'timestamp',
            'open': 'double',
            'high': 'double',
            'low': 'double',
            'close': 'double',
            'volume': 'int',
            'is_real_data': 'boolean',
        }, ['symbol', 'timestamp'])

        # TABLE FOR FEATURE ENGINEERING
        cassandra.create_table('data_engineering_key_space.stock_price_features', {
            'symbol': 'text',
            'timestamp': 'bigint',
            'gmtoffset': 'int',
            'datetime': 'timestamp',
            'open': 'double',
            'high': 'double',
            'low': 'double',
            'close': 'double',
            'volume': 'int',
            'is_real_data': 'boolean',
            'EMA_10': 'double',
            'EMA_30': 'double',
            'bb_mavg': 'double',
            'bb_hband': 'double',
            'bb_lband': 'double',
            'MACD': 'double',
            'MACD_signal': 'double',
            'MACD_diff': 'double',
            'RSI': 'double',
            'day_of_week_sin': 'double',
            'day_of_week_cos': 'double',
            'month_sin': 'double',
            'month_cos': 'double',
            'returns': 'double',
            'cumulative_return': 'double',
            'annualized_return': 'double',
            'annualized_volatility': 'double',
            'sharpe_ratio': 'double',
            'max_drawdown': 'double',
            'equally_weighted_return': 'double'
        }, ['symbol', 'timestamp'])

        # TABLE FOR PROCESSED STOCK PROCESSED DATA
        cassandra.create_table('data_engineering_key_space.model_results', {
            'id': 'int',
            'start_datetime': 'timestamp',
            'end_datetime': 'timestamp',
            'model_name': 'text',
            'model_tag': 'text',
            'version': 'text',
            'rmse': 'double',
            'mse': 'double',
            'avg_mse': 'double',
            'precision': 'double',
            'recall': 'double',
            'f1_score': 'double',
            'sharpe_ratio': 'double',
            'max_drawdown': 'double',
            'total_return': 'double',
            'annualized_volatility': 'double',
            'annualized_return': 'double',
            'cumulative_return': 'double',
            'num_trades': 'double',
            'winning_trades': 'double',
            'losing_trades': 'double'
        }, ['id'])

        return "Database table schema created successfully"

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return 'ERROR: ' + str(error)


########################################################################################################
########################################################################################################


@router.get('/cassandra/{keyspace_name}')
async def keyspace_overview(keyspace_name: str, response: Response):
    try:
        response.status_code = status.HTTP_200_OK
        return cassandra.keyspace_overview(keyspace_name)

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return 'ERROR: ' + str(error)

########################################################################################################
########################################################################################################


@router.get('/cassandra/{keyspace_name}/{table_name}')
async def table_overview(keyspace_name: str, table_name: str, response: Response):
    try:
        response.status_code = status.HTTP_200_OK
        return 'TODO'
        # return cassandra.table_overview(keyspace_name, table_name)

    except Exception as error:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return 'ERROR: ' + str(error)
