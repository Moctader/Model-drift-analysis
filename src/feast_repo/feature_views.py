from datetime import timedelta
from feast import Entity, Feature, FeatureView, FileSource
from feast.types import Float32

# Define an entity (e.g., stock symbol)
symbol_entity = Entity(name="symbol", join_keys=["symbol"])

# Offline source (for batch processing)
stock_price_source = FileSource(
    path="./feast_repo/data/stock_price_features.parquet",  # Update path to match feast_repo
    timestamp_field="datetime",
    created_timestamp_column="created"
)

# Define a feature view
stock_price_fv = FeatureView(
    name="stock_price_features",
    entities=[symbol_entity],
    ttl=timedelta(days=1),
    schema=[
        Feature(name="EMA_10", dtype=Float32),
        Feature(name="EMA_30", dtype=Float32),
        Feature(name="RSI", dtype=Float32),
        Feature(name="bb_mavg", dtype=Float32),
        Feature(name="bb_hband", dtype=Float32),
        Feature(name="bb_lband", dtype=Float32),
        Feature(name="MACD", dtype=Float32),
        Feature(name="MACD_signal", dtype=Float32),
        Feature(name="MACD_diff", dtype=Float32),
        Feature(name="sharpe_ratio", dtype=Float32),
        Feature(name="cumulative_return", dtype=Float32),
        Feature(name="annualized_return", dtype=Float32),
        Feature(name="annualized_volatility", dtype=Float32),
        Feature(name="max_drawdown", dtype=Float32),
        Feature(name="equally_weighted_return", dtype=Float32),
    ],
    online=True,  # Store features online in Cassandra
    source=stock_price_source  # Use the offline source for batch processing
)
