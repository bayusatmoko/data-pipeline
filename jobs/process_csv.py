from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("DailyTransactionsProcessor").getOrCreate()

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# NOTE: Use HDFS URI (namenode service + RPC port)
raw_path = "hdfs://namenode:9000/data/raw/csv/daily_*.csv"
processed_base = "hdfs://namenode:9000/data/processed/transactions/"

df = spark.read.csv(raw_path, header=True, schema=schema)

df_clean = df.dropna().dropDuplicates()

# user spend
user_spend = df_clean.groupBy("user_id") \
    .agg(F.sum("amount").alias("total_spent"), F.count("transaction_id").alias("transactions_user"))

# product revenue
product_revenue = df_clean.groupBy("product_id") \
    .agg(F.sum("amount").alias("total_revenue"), F.count("transaction_id").alias("transactions_product"))

df_enriched = df_clean \
    .join(user_spend, "user_id", "left") \
    .join(product_revenue, "product_id", "left")

# save outputs
df_clean.write.mode("overwrite").parquet(processed_base + "clean/")
user_spend.write.mode("overwrite").parquet(processed_base + "user_spend/")
product_revenue.write.mode("overwrite").parquet(processed_base + "product_revenue/")
df_enriched.write.mode("overwrite").parquet(processed_base + "enriched/")

spark.stop()