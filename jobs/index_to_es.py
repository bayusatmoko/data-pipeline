from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IndexTransactionsToElasticsearch") \
    .getOrCreate()

processed_enriched = "hdfs://namenode:9000/data/processed/transactions/enriched/"

df = spark.read.parquet(processed_enriched)

# Write to Elasticsearch (requires spark-submit --packages org.elasticsearch:elasticsearch-spark-30_2.12:<es_version>)
df.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "transactions_enriched/_doc") \
    .option("es.mapping.id", "transaction_id") \
    .mode("overwrite") \
    .save()

spark.stop()
