from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

spark = SparkSession.builder \
    .appName("CryptoStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("btc_price", DoubleType()),
    StructField("eth_price", DoubleType()),
    StructField("timestamp", LongType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.btc_price"),
    col("data.eth_price"),
    (col("data.btc_price") - col("data.eth_price")).alias("price_diff"),
    to_timestamp(col("data.timestamp")).alias("event_time")
)

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/crypto_db") \
        .option("dbtable", "crypto_prices") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()
